#!/bin/ksh
# =============================================================================
# Script  : load_daily_sales.ksh
# Purpose : Daily sales ETL orchestrator. Accepts load date and region as
#           positional parameters, resolves env config, calls SQL*Plus to
#           extract staging data, then invokes PL/SQL load procedures.
#
# Usage   : load_daily_sales.ksh <LOAD_DATE> <REGION_CODE> [BATCH_MODE]
# Example : load_daily_sales.ksh 2024-01-15 NORTH Y
#
# Called by: UC4 job RETAIL_DAILY_SALES_LOAD (retail_daily_workflow.xml)
# =============================================================================

set -u   # Treat unset variables as errors

# ----------------------------------------------------------------------------
# 1. Positional parameter resolution (REQ-PRE-01)
# ----------------------------------------------------------------------------
LOAD_DATE=${1:?"ERROR: LOAD_DATE (arg 1) is required. Format: YYYY-MM-DD"}
REGION_CODE=${2:?"ERROR: REGION_CODE (arg 2) is required. e.g. NORTH/SOUTH/EAST/WEST"}
BATCH_MODE=${3:-"N"}          # Optional: Y=batch/silent, N=verbose

# Validate date format
echo $LOAD_DATE | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
if [ $? -ne 0 ]; then
    echo "ERROR: LOAD_DATE must be YYYY-MM-DD format. Got: $LOAD_DATE"
    exit 1
fi

# ----------------------------------------------------------------------------
# 2. Load environment configuration
# ----------------------------------------------------------------------------
ENV_CONFIG_DIR=${ENV_CONFIG_DIR:-"/opt/etl/config"}
ENV_CONFIG="${ENV_CONFIG_DIR}/env_retail.properties"

if [ ! -f "$ENV_CONFIG" ]; then
    echo "ERROR: Config file not found: $ENV_CONFIG"
    exit 2
fi

# Source config - populates DB_HOST, DB_PORT, DB_SID, DB_SCHEMA, LOG_DIR etc.
. "$ENV_CONFIG"

# Override DB schema if region has dedicated schema
case "$REGION_CODE" in
    NORTH) DB_SCHEMA="${DB_SCHEMA_NORTH:-$DB_SCHEMA}" ;;
    SOUTH) DB_SCHEMA="${DB_SCHEMA_SOUTH:-$DB_SCHEMA}" ;;
    EAST)  DB_SCHEMA="${DB_SCHEMA_EAST:-$DB_SCHEMA}"  ;;
    WEST)  DB_SCHEMA="${DB_SCHEMA_WEST:-$DB_SCHEMA}"  ;;
    *)
        echo "WARN: Unknown region $REGION_CODE - using default schema"
        ;;
esac

# ----------------------------------------------------------------------------
# 3. Derived variables
# ----------------------------------------------------------------------------
LOAD_DATE_FMT=$(echo $LOAD_DATE | tr '-' '')    # YYYYMMDD format for filenames
LOG_DATE=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="${LOG_DIR}/retail_sales_${REGION_CODE}_${LOAD_DATE_FMT}_${LOG_DATE}.log"
SQLPLUS_LOG="${LOG_DIR}/sqlplus_extract_${REGION_CODE}_${LOAD_DATE_FMT}.log"
PID_FILE="${LOG_DIR}/retail_sales_${REGION_CODE}.pid"

# Construct Oracle connection string
ORA_CONNECT="${DB_USER}/${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_SID}"

# Fiscal period derivation (Q1=Jan-Mar, Q2=Apr-Jun, etc.)
LOAD_MONTH=$(date -d "$LOAD_DATE" '+%m' 2>/dev/null || echo $LOAD_DATE | cut -c6-7)
if   [ "$LOAD_MONTH" -le 3 ];  then FISCAL_PERIOD="Q1"
elif [ "$LOAD_MONTH" -le 6 ];  then FISCAL_PERIOD="Q2"
elif [ "$LOAD_MONTH" -le 9 ];  then FISCAL_PERIOD="Q3"
else                                 FISCAL_PERIOD="Q4"
fi

export LOAD_DATE REGION_CODE FISCAL_PERIOD LOAD_DATE_FMT

# ----------------------------------------------------------------------------
# 4. Lock check - prevent duplicate runs
# ----------------------------------------------------------------------------
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 $OLD_PID 2>/dev/null; then
        echo "ERROR: Another instance is running (PID=$OLD_PID). Exiting."
        exit 3
    else
        echo "WARN: Stale PID file found ($OLD_PID). Removing and continuing."
        rm -f "$PID_FILE"
    fi
fi
echo $$ > "$PID_FILE"

# ----------------------------------------------------------------------------
# 5. Logging function
# ----------------------------------------------------------------------------
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$REGION_CODE] $1" | tee -a "$LOG_FILE"
}

# ----------------------------------------------------------------------------
# 6. Cleanup function
# ----------------------------------------------------------------------------
cleanup() {
    EXIT_CODE=$?
    rm -f "$PID_FILE"
    if [ $EXIT_CODE -ne 0 ]; then
        log "SCRIPT FAILED with exit code: $EXIT_CODE"
    fi
}
trap cleanup EXIT

log "============================================================"
log "load_daily_sales.ksh STARTED"
log "LOAD_DATE=$LOAD_DATE  REGION_CODE=$REGION_CODE  BATCH_MODE=$BATCH_MODE"
log "FISCAL_PERIOD=$FISCAL_PERIOD  DB_SCHEMA=$DB_SCHEMA"
log "============================================================"

# ----------------------------------------------------------------------------
# 7. Step 1: SQL*Plus extraction - passes LOAD_DATE and REGION_CODE as vars
#    The SQL*Plus script uses &&LOAD_DATE and &&REGION_CODE substitution vars
# ----------------------------------------------------------------------------
log "Step 1: Extracting staging data via SQL*Plus..."

sqlplus -s "$ORA_CONNECT" <<SQLPLUS_EOF > "$SQLPLUS_LOG" 2>&1
    -- Pass shell variables into SQL*Plus substitution variables
    DEFINE LOAD_DATE    = '$LOAD_DATE'
    DEFINE REGION_CODE  = '$REGION_CODE'
    DEFINE BATCH_ID     = '$LOAD_DATE_FMT'
    DEFINE FISCAL_QTR   = '$FISCAL_PERIOD'

    @${SQLPLUS_DIR}/sales_extract.sql
    EXIT SQL.SQLCODE;
SQLPLUS_EOF

SQLPLUS_RC=$?
if [ $SQLPLUS_RC -ne 0 ]; then
    log "ERROR: SQL*Plus extraction failed (rc=$SQLPLUS_RC)"
    log "See: $SQLPLUS_LOG"
    exit 4
fi
log "Step 1: SQL*Plus extraction complete."

# ----------------------------------------------------------------------------
# 8. Step 2: Validate row counts in staging before PL/SQL load
# ----------------------------------------------------------------------------
log "Step 2: Validating staging row counts..."

STG_COUNT=$(sqlplus -s "$ORA_CONNECT" <<VALIDATE_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_SALES_TRANSACTIONS
    WHERE  TRANSACTION_DATE = TO_DATE('$LOAD_DATE','YYYY-MM-DD')
    AND    REGION_CODE      = '$REGION_CODE'
    AND    ETL_STATUS       = 'PENDING';
    EXIT;
VALIDATE_EOF
)

STG_COUNT=$(echo $STG_COUNT | tr -d ' ')
log "Staging row count for ${REGION_CODE}/${LOAD_DATE}: $STG_COUNT"

if [ -z "$STG_COUNT" ] || [ "$STG_COUNT" -eq 0 ]; then
    log "WARN: No staging rows found for date=$LOAD_DATE region=$REGION_CODE"
    if [ "$BATCH_MODE" = "Y" ]; then
        log "Batch mode: treating 0 rows as success (no-op)"
        exit 0
    fi
fi

# Check against minimum expected threshold
MIN_ROWS=${MIN_EXPECTED_ROWS:-100}
if [ "$STG_COUNT" -lt "$MIN_ROWS" ]; then
    log "WARN: Row count ($STG_COUNT) below minimum threshold ($MIN_ROWS)"
fi

# ----------------------------------------------------------------------------
# 9. Step 3: Invoke PL/SQL master load procedure
# ----------------------------------------------------------------------------
log "Step 3: Running PL/SQL MASTER_LOAD..."

sqlplus -s "$ORA_CONNECT" <<PLSQL_EOF >> "$LOG_FILE" 2>&1
    SET SERVEROUTPUT ON SIZE UNLIMITED
    SET FEEDBACK OFF
    DECLARE
        v_err_msg VARCHAR2(500);
    BEGIN
        PKG_SALES_HISTORIZATION.MASTER_LOAD(
            p_load_date   => TO_DATE('$LOAD_DATE','YYYY-MM-DD'),
            p_region_code => '$REGION_CODE'
        );
        DBMS_OUTPUT.PUT_LINE('MASTER_LOAD completed successfully.');
    EXCEPTION
        WHEN OTHERS THEN
            v_err_msg := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_err_msg);
            RAISE;
    END;
    /
    EXIT SQL.SQLCODE;
PLSQL_EOF

PLSQL_RC=$?
if [ $PLSQL_RC -ne 0 ]; then
    log "ERROR: PL/SQL MASTER_LOAD failed (rc=$PLSQL_RC)"
    exit 5
fi
log "Step 3: PL/SQL load complete."

# ----------------------------------------------------------------------------
# 10. Step 4: Post-load validation and summary
# ----------------------------------------------------------------------------
log "Step 4: Post-load row count validation..."

FACT_COUNT=$(sqlplus -s "$ORA_CONNECT" <<FACT_VALIDATE_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM FACT_DAILY_SALES
    WHERE  TRANSACTION_DATE = TO_DATE('$LOAD_DATE','YYYY-MM-DD')
    AND    REGION_CODE      = '$REGION_CODE'
    AND    LOAD_DATE        >= TRUNC(SYSDATE);
    EXIT;
FACT_VALIDATE_EOF
)

FACT_COUNT=$(echo $FACT_COUNT | tr -d ' ')
log "FACT_DAILY_SALES rows loaded: $FACT_COUNT"

log "============================================================"
log "load_daily_sales.ksh COMPLETED SUCCESSFULLY"
log "LOAD_DATE=$LOAD_DATE REGION=$REGION_CODE FACT_ROWS=$FACT_COUNT"
log "============================================================"

exit 0
