-- =============================================================================
-- SQL*Plus Script : sales_extract.sql
-- Purpose        : Extracts sales transactions from source Oracle schema into
--                  STG_SALES_TRANSACTIONS for a given load date and region.
--                  Uses &&-style substitution variables passed from shell.
--
-- Called by      : load_daily_sales.ksh (via SQL*Plus heredoc)
-- Variables set by caller:
--   &&LOAD_DATE    - e.g. 2024-01-15
--   &&REGION_CODE  - e.g. NORTH
--   &&BATCH_ID     - e.g. 20240115
--   &&FISCAL_QTR   - e.g. Q1
-- =============================================================================

SET ECHO OFF
SET FEEDBACK OFF
SET HEADING OFF
SET VERIFY OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET TRIMSPOOL ON

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;
WHENEVER OSERROR  EXIT 9 ROLLBACK;

SPOOL /opt/etl/logs/sales_extract_&&REGION_CODE._&&BATCH_ID..log APPEND

PROMPT ============================================================
PROMPT sales_extract.sql running
PROMPT LOAD_DATE   = &&LOAD_DATE
PROMPT REGION_CODE = &&REGION_CODE
PROMPT BATCH_ID    = &&BATCH_ID
PROMPT FISCAL_QTR  = &&FISCAL_QTR
PROMPT ============================================================

-- ----------------------------------------------------------------------------
-- Step 1: Clear any existing PENDING rows for this date/region (idempotency)
-- ----------------------------------------------------------------------------
PROMPT Step 1: Purging previous PENDING staging records...

DELETE FROM STG_SALES_TRANSACTIONS
WHERE  TRANSACTION_DATE = TO_DATE('&&LOAD_DATE', 'YYYY-MM-DD')
AND    REGION_CODE      = '&&REGION_CODE'
AND    ETL_STATUS       = 'PENDING';

PROMPT Deleted &&_ROWCOUNT rows from staging.

-- ----------------------------------------------------------------------------
-- Step 2: Extract from source transactions table (cross-schema link)
--         SOURCE_OPS is the operational Oracle schema
-- ----------------------------------------------------------------------------
PROMPT Step 2: Extracting from SOURCE_OPS.SALES_TXN...

INSERT /*+ APPEND */ INTO STG_SALES_TRANSACTIONS (
    TRANSACTION_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    STORE_ID,
    TRANSACTION_DATE,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMT,
    DISCOUNT_PCT,
    REGION_CODE,
    CURRENCY_CODE,
    PAYMENT_METHOD,
    POS_TERMINAL_ID,
    LOAD_DATE,
    LOAD_BATCH_ID,
    SOURCE_FILE_NAME,
    ETL_STATUS
)
SELECT
    t.TXN_ID,
    t.CUST_ID,
    t.PROD_ID,
    t.STORE_ID,
    TRUNC(t.TXN_DATETIME)                           AS TRANSACTION_DATE,
    t.SOLD_QTY,
    t.UNIT_SELL_PRICE,
    NVL(t.DISC_AMOUNT, 0)                           AS DISCOUNT_AMT,
    CASE WHEN t.UNIT_SELL_PRICE > 0
         THEN ROUND((NVL(t.DISC_AMOUNT,0) / t.UNIT_SELL_PRICE) * 100, 2)
         ELSE 0
    END                                              AS DISCOUNT_PCT,
    t.STORE_REGION_CD,
    NVL(t.CURRENCY, 'GBP')                          AS CURRENCY_CODE,
    t.PAYMENT_TYPE_CD,
    t.TERMINAL_REF,
    TO_DATE('&&LOAD_DATE', 'YYYY-MM-DD'),
    TO_NUMBER('&&BATCH_ID'),
    '&&REGION_CODE._txn_&&LOAD_DATE',
    'PENDING'
FROM   SOURCE_OPS.SALES_TXN t
WHERE  TRUNC(t.TXN_DATETIME) = TO_DATE('&&LOAD_DATE', 'YYYY-MM-DD')
AND    t.STORE_REGION_CD     = '&&REGION_CODE'
AND    t.TXN_STATUS_CD       NOT IN ('VOID','CANCELLED','TEST')
AND    t.SOLD_QTY            > 0
AND    t.UNIT_SELL_PRICE     >= 0;

COMMIT;
PROMPT Inserted &&_ROWCOUNT rows into STG_SALES_TRANSACTIONS.

-- ----------------------------------------------------------------------------
-- Step 3: Extract and upsert customer records to shared staging
-- ----------------------------------------------------------------------------
PROMPT Step 3: Refreshing STG_CUSTOMER_SALES for region &&REGION_CODE...

MERGE INTO STG_CUSTOMER_SALES tgt
USING (
    SELECT DISTINCT
        c.CUST_ID                                   AS CUSTOMER_ID,
        c.CUST_REF_CD                               AS CUSTOMER_CODE,
        c.FIRST_NM                                  AS FIRST_NAME,
        c.LAST_NM                                   AS LAST_NAME,
        LOWER(c.EMAIL_ADDR)                         AS EMAIL,
        c.MOBILE_PHONE                              AS PHONE,
        c.HOME_REGION_CD                            AS REGION_CODE,
        NVL(lp.TIER_CD, 'STANDARD')                AS LOYALTY_TIER,
        lp.LIFETIME_SPEND                           AS LIFETIME_VALUE,
        c.REG_DATE                                  AS REGISTRATION_DATE,
        MAX(t.TXN_DATETIME) OVER (PARTITION BY c.CUST_ID) AS LAST_PURCHASE_DATE,
        TO_DATE('&&LOAD_DATE', 'YYYY-MM-DD')        AS LOAD_DATE
    FROM   SOURCE_OPS.CUSTOMER c
    JOIN   SOURCE_OPS.SALES_TXN t
             ON t.CUST_ID = c.CUST_ID
            AND TRUNC(t.TXN_DATETIME) = TO_DATE('&&LOAD_DATE', 'YYYY-MM-DD')
            AND t.STORE_REGION_CD     = '&&REGION_CODE'
    LEFT JOIN SOURCE_OPS.LOYALTY_PROFILE lp ON lp.CUST_ID = c.CUST_ID
) src
ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID)
WHEN MATCHED THEN UPDATE SET
    tgt.LOYALTY_TIER       = src.LOYALTY_TIER,
    tgt.LIFETIME_VALUE     = src.LIFETIME_VALUE,
    tgt.LAST_PURCHASE_DATE = src.LAST_PURCHASE_DATE,
    tgt.LOAD_DATE          = src.LOAD_DATE
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, CUSTOMER_CODE, FIRST_NAME, LAST_NAME,
    EMAIL, PHONE, REGION_CODE, LOYALTY_TIER, LIFETIME_VALUE,
    REGISTRATION_DATE, LAST_PURCHASE_DATE, LOAD_DATE
) VALUES (
    src.CUSTOMER_ID, src.CUSTOMER_CODE, src.FIRST_NAME, src.LAST_NAME,
    src.EMAIL, src.PHONE, src.REGION_CODE, src.LOYALTY_TIER, src.LIFETIME_VALUE,
    src.REGISTRATION_DATE, src.LAST_PURCHASE_DATE, src.LOAD_DATE
);

COMMIT;
PROMPT STG_CUSTOMER_SALES merge complete: &&_ROWCOUNT rows.

-- ----------------------------------------------------------------------------
-- Step 4: Data quality summary report
-- ----------------------------------------------------------------------------
PROMPT Step 4: Data quality checks for &&REGION_CODE / &&LOAD_DATE...

SELECT 'TOTAL_ROWS'       AS metric, COUNT(*)                   AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE' UNION ALL
SELECT 'NULL_PRODUCT_ID'  AS metric, COUNT(*)                   AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE' AND PRODUCT_ID IS NULL UNION ALL
SELECT 'NULL_CUSTOMER_ID' AS metric, COUNT(*)                   AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE' AND CUSTOMER_ID IS NULL UNION ALL
SELECT 'ZERO_AMOUNT_ROWS' AS metric, COUNT(*)                   AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE' AND UNIT_PRICE = 0 UNION ALL
SELECT 'DISTINCT_STORES'  AS metric, COUNT(DISTINCT STORE_ID)   AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE' UNION ALL
SELECT 'DISTINCT_PRODUCTS'AS metric, COUNT(DISTINCT PRODUCT_ID) AS value FROM STG_SALES_TRANSACTIONS WHERE TRANSACTION_DATE = TO_DATE('&&LOAD_DATE','YYYY-MM-DD') AND REGION_CODE = '&&REGION_CODE';

SPOOL OFF
PROMPT sales_extract.sql DONE.

EXIT 0;
