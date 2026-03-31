#!/bin/ksh
# =============================================================================
# Script  : load_product_master.ksh
# Purpose : Loads product master data from source flat files into Oracle
#           staging, then triggers SCD Type 2 dimension load. Implements
#           retry logic with exponential backoff for transient DB failures.
#
# Usage   : load_product_master.ksh <LOAD_DATE> [MAX_RETRIES] [RETRY_WAIT_SEC]
# Example : load_product_master.ksh 2024-01-15 3 60
#
# Called by: UC4 job RETAIL_PRODUCT_MASTER_LOAD
# Predecessor: RETAIL_STG_EXTRACT
# =============================================================================

LOAD_DATE=${1:?"LOAD_DATE required (YYYY-MM-DD)"}
MAX_RETRIES=${2:-3}
RETRY_WAIT=${3:-60}

# ----------------------------------------------------------------------------
# Source environment
# ----------------------------------------------------------------------------
ENV_CONFIG="${ENV_CONFIG_DIR:-/opt/etl/config}/env_retail.properties"
. "$ENV_CONFIG"

LOG_DIR="${LOG_DIR:-/opt/etl/logs}"
SQLPLUS_DIR="${SQLPLUS_DIR:-/opt/etl/sqlplus}"
LOAD_DATE_FMT=$(echo $LOAD_DATE | tr '-' '')
LOG_FILE="${LOG_DIR}/product_master_${LOAD_DATE_FMT}_$(date '+%H%M%S').log"
ORA_CONNECT="${DB_USER}/${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_SID}"
SOURCE_FILE="${SOURCE_FILE_DIR}/product_master_${LOAD_DATE_FMT}.csv"
CONTROL_FILE="${SQLPLUS_DIR}/product_master.ctl"

export LOAD_DATE LOAD_DATE_FMT SOURCE_FILE

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

log "=== load_product_master.ksh START: LOAD_DATE=$LOAD_DATE ==="

# ----------------------------------------------------------------------------
# 1. Check source file exists
# ----------------------------------------------------------------------------
if [ ! -f "$SOURCE_FILE" ]; then
    log "ERROR: Source file not found: $SOURCE_FILE"
    exit 1
fi

FILE_ROWS=$(wc -l < "$SOURCE_FILE")
log "Source file rows (including header): $FILE_ROWS"

# ----------------------------------------------------------------------------
# 2. Retry loop: SQL*Loader into staging
# ----------------------------------------------------------------------------
ATTEMPT=0
LOAD_SUCCESS=0

while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))
    log "SQL*Loader attempt $ATTEMPT of $MAX_RETRIES..."

    sqlldr userid="$ORA_CONNECT" \
           control="$CONTROL_FILE" \
           data="$SOURCE_FILE" \
           log="${LOG_DIR}/sqlldr_${LOAD_DATE_FMT}_${ATTEMPT}.log" \
           bad="${LOG_DIR}/sqlldr_${LOAD_DATE_FMT}_${ATTEMPT}.bad" \
           direct=TRUE \
           errors=0 >> "$LOG_FILE" 2>&1

    SQLLDR_RC=$?

    if [ $SQLLDR_RC -eq 0 ]; then
        log "SQL*Loader succeeded on attempt $ATTEMPT."
        LOAD_SUCCESS=1
        break
    elif [ $SQLLDR_RC -eq 2 ]; then
        # Exit code 2 = warnings (partial load) - acceptable
        log "WARN: SQL*Loader partial load (rc=2). Checking bad file..."
        BAD_FILE="${LOG_DIR}/sqlldr_${LOAD_DATE_FMT}_${ATTEMPT}.bad"
        if [ -f "$BAD_FILE" ] && [ -s "$BAD_FILE" ]; then
            BAD_COUNT=$(wc -l < "$BAD_FILE")
            log "WARN: $BAD_COUNT rows rejected. See: $BAD_FILE"
            if [ "$BAD_COUNT" -gt "${MAX_REJECTED_ROWS:-50}" ]; then
                log "ERROR: Too many rejected rows ($BAD_COUNT > $MAX_REJECTED_ROWS). Failing."
                exit 2
            fi
        fi
        LOAD_SUCCESS=1
        break
    else
        log "ERROR: SQL*Loader failed (rc=$SQLLDR_RC) on attempt $ATTEMPT"
        if [ $ATTEMPT -lt $MAX_RETRIES ]; then
            WAIT_TIME=$((RETRY_WAIT * ATTEMPT))  # Exponential backoff
            log "Waiting ${WAIT_TIME}s before retry $((ATTEMPT + 1))..."
            sleep $WAIT_TIME
        fi
    fi
done

if [ $LOAD_SUCCESS -eq 0 ]; then
    log "FATAL: SQL*Loader failed after $MAX_RETRIES attempts."
    exit 3
fi

# ----------------------------------------------------------------------------
# 3. Validate staging counts (before PL/SQL)
# ----------------------------------------------------------------------------
log "Validating staging counts for LOAD_DATE=$LOAD_DATE..."

STG_COUNT=$(sqlplus -s "$ORA_CONNECT" <<EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT COUNT(*) FROM STG_PRODUCT_MASTER
    WHERE  LOAD_DATE = TO_DATE('$LOAD_DATE','YYYY-MM-DD');
    EXIT;
EOF
)
STG_COUNT=$(echo $STG_COUNT | tr -d ' ')
log "STG_PRODUCT_MASTER rows: $STG_COUNT"

EXPECTED_ROWS=$((FILE_ROWS - 1))   # Subtract header
if [ "$STG_COUNT" -lt "$EXPECTED_ROWS" ]; then
    log "WARN: Staging count ($STG_COUNT) < expected ($EXPECTED_ROWS)"
fi

# ----------------------------------------------------------------------------
# 4. Retry loop: PL/SQL dimension load
# ----------------------------------------------------------------------------
ATTEMPT=0
DIM_SUCCESS=0

while [ $ATTEMPT -lt $MAX_RETRIES ]; do
    ATTEMPT=$((ATTEMPT + 1))
    log "PL/SQL dimension load attempt $ATTEMPT of $MAX_RETRIES..."

    sqlplus -s "$ORA_CONNECT" <<PLSQL_EOF >> "$LOG_FILE" 2>&1
        SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
        DECLARE
            v_rows NUMBER := 0;
        BEGIN
            PKG_SALES_HISTORIZATION.LOAD_DIM_PRODUCT(
                p_load_date   => TO_DATE('$LOAD_DATE','YYYY-MM-DD'),
                p_batch_id    => TO_NUMBER('$LOAD_DATE_FMT'),
                p_rows_loaded => v_rows
            );
            -- Also apply category rules and bulk enrichment
            PKG_PRODUCT_DIM_LOAD.BULK_ENRICH_PRODUCTS(
                p_load_date     => TO_DATE('$LOAD_DATE','YYYY-MM-DD'),
                p_source_schema => '${SUPPLIER_SCHEMA:-SUPPLIER_DATA}'
            );
            DBMS_OUTPUT.PUT_LINE('DIM_PRODUCT rows loaded: ' || v_rows);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
                RAISE;
        END;
        /
        EXIT SQL.SQLCODE;
PLSQL_EOF

    PLSQL_RC=$?
    if [ $PLSQL_RC -eq 0 ]; then
        log "PL/SQL dimension load succeeded on attempt $ATTEMPT."
        DIM_SUCCESS=1
        break
    else
        log "ERROR: PL/SQL failed (rc=$PLSQL_RC) on attempt $ATTEMPT"
        if [ $ATTEMPT -lt $MAX_RETRIES ]; then
            sleep $((RETRY_WAIT * ATTEMPT))
        fi
    fi
done

if [ $DIM_SUCCESS -eq 0 ]; then
    log "FATAL: PL/SQL dimension load failed after $MAX_RETRIES attempts."
    exit 4
fi

# ----------------------------------------------------------------------------
# 5. Apply category-specific transformation rules
# ----------------------------------------------------------------------------
log "Applying category-specific rules..."

# Get distinct categories loaded today
CATEGORIES=$(sqlplus -s "$ORA_CONNECT" <<CAT_EOF
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0 TRIMOUT ON
    SELECT DISTINCT CATEGORY_CODE FROM STG_PRODUCT_MASTER
    WHERE  LOAD_DATE = TO_DATE('$LOAD_DATE','YYYY-MM-DD')
    AND    CATEGORY_CODE IS NOT NULL;
    EXIT;
CAT_EOF
)

for CAT in $CATEGORIES; do
    CAT=$(echo $CAT | tr -d ' ')
    [ -z "$CAT" ] && continue
    log "  Applying rules for category: $CAT"
    sqlplus -s "$ORA_CONNECT" <<CAT_RULE_EOF >> "$LOG_FILE" 2>&1
        SET SERVEROUTPUT ON SIZE UNLIMITED FEEDBACK OFF
        BEGIN
            PKG_PRODUCT_DIM_LOAD.APPLY_CATEGORY_RULES(
                p_category_code => '$CAT',
                p_load_date     => TO_DATE('$LOAD_DATE','YYYY-MM-DD')
            );
        END;
        /
        EXIT;
CAT_RULE_EOF
    if [ $? -ne 0 ]; then
        log "WARN: Category rule application failed for $CAT - continuing"
    fi
done

log "=== load_product_master.ksh COMPLETED SUCCESSFULLY ==="
exit 0
