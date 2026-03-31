-- =============================================================================
-- Package: PKG_SALES_HISTORIZATION
-- Description: Implements SCD Type 2 historization for DIM_PRODUCT and
--              DIM_CUSTOMER, loads FACT_DAILY_SALES, and generates
--              FACT_REGIONAL_SUMMARY. Contains dynamic SQL for partition-aware
--              processing.
-- Schema: DW_OWNER
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_SALES_HISTORIZATION AS

    PROCEDURE LOAD_DIM_PRODUCT(
        p_load_date   IN DATE,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    );

    PROCEDURE LOAD_DIM_CUSTOMER(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    );

    PROCEDURE LOAD_FACT_SALES(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    );

    PROCEDURE GENERATE_REGIONAL_SUMMARY(
        p_summary_date IN DATE,
        p_region_code  IN VARCHAR2
    );

    PROCEDURE MASTER_LOAD(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2
    );

END PKG_SALES_HISTORIZATION;
/

CREATE OR REPLACE PACKAGE BODY PKG_SALES_HISTORIZATION AS

    -- -------------------------------------------------------------------------
    -- Private: Get or Create Batch ID
    -- -------------------------------------------------------------------------
    FUNCTION GET_BATCH_ID RETURN NUMBER IS
    BEGIN
        RETURN SEQ_BATCH_ID.NEXTVAL;
    END GET_BATCH_ID;

    -- -------------------------------------------------------------------------
    -- PROC: LOAD_DIM_PRODUCT
    -- SCD Type 2: Expire old record (update valid_to + is_current),
    --             Insert new version for changed attributes.
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_DIM_PRODUCT(
        p_load_date   IN DATE,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    ) IS
        v_rows       NUMBER := 0;
        v_changed    NUMBER := 0;
        v_new        NUMBER := 0;

        CURSOR cur_changed_products IS
            SELECT s.PRODUCT_ID,
                   s.PRODUCT_CODE,
                   s.PRODUCT_NAME,
                   s.CATEGORY_CODE,
                   s.SUBCATEGORY_CODE,
                   s.SUPPLIER_ID,
                   s.COST_PRICE,
                   s.LIST_PRICE,
                   s.TAX_CLASS_CODE
            FROM   STG_PRODUCT_MASTER s
            WHERE  s.LOAD_DATE = p_load_date
            AND    s.IS_ACTIVE  = 'Y';

    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> LOAD_DIM_PRODUCT started: ' || TO_CHAR(p_load_date,'YYYY-MM-DD'));

        FOR rec IN cur_changed_products LOOP

            -- Check if product exists in dimension
            SELECT COUNT(*)
            INTO   v_rows
            FROM   DIM_PRODUCT
            WHERE  PRODUCT_ID  = rec.PRODUCT_ID
            AND    IS_CURRENT  = 'Y';

            IF v_rows = 0 THEN
                -- Brand new product - insert first version
                INSERT INTO DIM_PRODUCT (
                    DIM_PRODUCT_KEY, PRODUCT_ID, PRODUCT_CODE, PRODUCT_NAME,
                    CATEGORY_CODE, SUBCATEGORY_CODE, SUPPLIER_ID,
                    COST_PRICE, LIST_PRICE, TAX_CLASS_CODE,
                    VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NUM,
                    CREATED_BY, CREATED_DATE
                ) VALUES (
                    SEQ_DIM_PRODUCT_KEY.NEXTVAL, rec.PRODUCT_ID, rec.PRODUCT_CODE,
                    rec.PRODUCT_NAME, rec.CATEGORY_CODE, rec.SUBCATEGORY_CODE,
                    rec.SUPPLIER_ID, rec.COST_PRICE, rec.LIST_PRICE, rec.TAX_CLASS_CODE,
                    p_load_date, TO_DATE('9999-12-31','YYYY-MM-DD'), 'Y', 1,
                    'ETL_PROCESS', SYSDATE
                );
                v_new := v_new + 1;

            ELSE
                -- Check if any tracked attributes have changed (SCD Type 2 logic)
                DECLARE
                    v_attr_changed NUMBER := 0;
                BEGIN
                    SELECT COUNT(*) INTO v_attr_changed
                    FROM   DIM_PRODUCT
                    WHERE  PRODUCT_ID  = rec.PRODUCT_ID
                    AND    IS_CURRENT  = 'Y'
                    AND (
                        NVL(PRODUCT_NAME,   'X') <> NVL(rec.PRODUCT_NAME,   'X') OR
                        NVL(CATEGORY_CODE,  'X') <> NVL(rec.CATEGORY_CODE,  'X') OR
                        NVL(LIST_PRICE,       0) <> NVL(rec.LIST_PRICE,       0) OR
                        NVL(COST_PRICE,       0) <> NVL(rec.COST_PRICE,       0) OR
                        NVL(SUPPLIER_ID,      0) <> NVL(rec.SUPPLIER_ID,      0)
                    );

                    IF v_attr_changed > 0 THEN
                        -- Step 1: Expire the current record
                        UPDATE DIM_PRODUCT
                        SET    VALID_TO     = p_load_date - 1,
                               IS_CURRENT  = 'N',
                               UPDATED_BY  = 'ETL_PROCESS',
                               UPDATED_DATE = SYSDATE
                        WHERE  PRODUCT_ID  = rec.PRODUCT_ID
                        AND    IS_CURRENT  = 'Y';

                        -- Step 2: Insert new current version
                        DECLARE
                            v_prev_version NUMBER;
                        BEGIN
                            SELECT MAX(VERSION_NUM) INTO v_prev_version
                            FROM   DIM_PRODUCT
                            WHERE  PRODUCT_ID = rec.PRODUCT_ID;

                            INSERT INTO DIM_PRODUCT (
                                DIM_PRODUCT_KEY, PRODUCT_ID, PRODUCT_CODE, PRODUCT_NAME,
                                CATEGORY_CODE, SUBCATEGORY_CODE, SUPPLIER_ID,
                                COST_PRICE, LIST_PRICE, TAX_CLASS_CODE,
                                VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NUM,
                                CREATED_BY, CREATED_DATE
                            ) VALUES (
                                SEQ_DIM_PRODUCT_KEY.NEXTVAL, rec.PRODUCT_ID, rec.PRODUCT_CODE,
                                rec.PRODUCT_NAME, rec.CATEGORY_CODE, rec.SUBCATEGORY_CODE,
                                rec.SUPPLIER_ID, rec.COST_PRICE, rec.LIST_PRICE, rec.TAX_CLASS_CODE,
                                p_load_date, TO_DATE('9999-12-31','YYYY-MM-DD'), 'Y',
                                NVL(v_prev_version, 0) + 1,
                                'ETL_PROCESS', SYSDATE
                            );
                            v_changed := v_changed + 1;
                        END;
                    END IF;
                END;
            END IF;

        END LOOP;

        COMMIT;
        p_rows_loaded := v_new + v_changed;
        DBMS_OUTPUT.PUT_LINE('<<< LOAD_DIM_PRODUCT complete. New=' || v_new || ' Changed=' || v_changed);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR in LOAD_DIM_PRODUCT: ' || SQLERRM);
            RAISE;
    END LOAD_DIM_PRODUCT;

    -- -------------------------------------------------------------------------
    -- PROC: LOAD_DIM_CUSTOMER
    -- SCD Type 2 for customer with region-partitioned processing.
    -- Uses EXECUTE IMMEDIATE for dynamic region-based partition querying.
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_DIM_CUSTOMER(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    ) IS
        v_sql           VARCHAR2(4000);
        v_stg_table     VARCHAR2(100);
        v_count         NUMBER := 0;
        v_cursor_id     INTEGER;
        v_rows_fetched  INTEGER;

        TYPE t_cust_rec IS RECORD (
            CUSTOMER_ID     NUMBER,
            CUSTOMER_CODE   VARCHAR2(20),
            FULL_NAME       VARCHAR2(200),
            EMAIL           VARCHAR2(200),
            PHONE           VARCHAR2(30),
            REGION_CODE     VARCHAR2(10),
            LOYALTY_TIER    VARCHAR2(20)
        );
        TYPE t_cust_tab IS TABLE OF t_cust_rec INDEX BY BINARY_INTEGER;
        v_cust_data t_cust_tab;

    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> LOAD_DIM_CUSTOMER: region=' || p_region_code);

        -- Dynamically determine staging table based on region
        -- Some regions have dedicated staging tables e.g. STG_CUSTOMER_SALES_NORTH
        v_stg_table := 'STG_CUSTOMER_SALES';

        v_sql := 'SELECT c.CUSTOMER_ID, '
              || '       c.CUSTOMER_CODE, '
              || '       c.FIRST_NAME || '' '' || c.LAST_NAME AS FULL_NAME, '
              || '       c.EMAIL, '
              || '       c.PHONE, '
              || '       c.REGION_CODE, '
              || '       c.LOYALTY_TIER '
              || '  FROM ' || v_stg_table || ' c '
              || ' WHERE c.LOAD_DATE   = :1 '
              || '   AND c.REGION_CODE = :2 '
              || '   AND c.CUSTOMER_ID IS NOT NULL';

        -- Execute dynamic query and process each customer (SCD Type 2)
        v_cursor_id := DBMS_SQL.OPEN_CURSOR;
        DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);
        DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':1', p_load_date);
        DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':2', p_region_code);

        -- Alternatively - direct EXECUTE IMMEDIATE into a collection
        EXECUTE IMMEDIATE
            'SELECT COUNT(*) FROM ' || v_stg_table ||
            ' WHERE LOAD_DATE = :dt AND REGION_CODE = :reg'
            INTO v_count
            USING p_load_date, p_region_code;

        DBMS_SQL.CLOSE_CURSOR(v_cursor_id);

        DBMS_OUTPUT.PUT_LINE('    Staging records for region ' || p_region_code || ': ' || v_count);

        -- Merge-based SCD Type 2 using EXECUTE IMMEDIATE for flexibility
        v_sql :=
            'MERGE INTO DIM_CUSTOMER tgt '
         || 'USING ( '
         || '    SELECT s.CUSTOMER_ID, '
         || '           s.CUSTOMER_CODE, '
         || '           s.FIRST_NAME || '' '' || s.LAST_NAME AS FULL_NAME, '
         || '           s.EMAIL, s.PHONE, s.REGION_CODE, s.LOYALTY_TIER '
         || '    FROM ' || v_stg_table || ' s '
         || '    WHERE s.LOAD_DATE   = :load_dt '
         || '      AND s.REGION_CODE = :reg_code '
         || ') src '
         || 'ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = ''Y'') '
         || 'WHEN MATCHED THEN UPDATE SET '
         || '    tgt.VALID_TO    = :load_dt - 1, '
         || '    tgt.IS_CURRENT  = ''N'', '
         || '    tgt.UPDATED_DATE = SYSDATE '
         || 'WHERE ( '
         || '    NVL(tgt.FULL_NAME,  ''X'') <> NVL(src.FULL_NAME,  ''X'') OR '
         || '    NVL(tgt.EMAIL,      ''X'') <> NVL(src.EMAIL,      ''X'') OR '
         || '    NVL(tgt.LOYALTY_TIER,''X'')<> NVL(src.LOYALTY_TIER,''X'') '
         || ')';

        EXECUTE IMMEDIATE v_sql USING p_load_date, p_region_code, p_load_date;

        -- Insert new versions for expired rows
        INSERT INTO DIM_CUSTOMER (
            DIM_CUSTOMER_KEY, CUSTOMER_ID, CUSTOMER_CODE, FULL_NAME,
            EMAIL, PHONE, REGION_CODE, LOYALTY_TIER,
            VALID_FROM, VALID_TO, IS_CURRENT, VERSION_NUM, CREATED_DATE
        )
        SELECT SEQ_DIM_CUSTOMER_KEY.NEXTVAL,
               s.CUSTOMER_ID, s.CUSTOMER_CODE,
               s.FIRST_NAME || ' ' || s.LAST_NAME,
               s.EMAIL, s.PHONE, s.REGION_CODE, s.LOYALTY_TIER,
               p_load_date,
               TO_DATE('9999-12-31','YYYY-MM-DD'),
               'Y',
               NVL((SELECT MAX(d2.VERSION_NUM) FROM DIM_CUSTOMER d2
                    WHERE d2.CUSTOMER_ID = s.CUSTOMER_ID), 0) + 1,
               SYSDATE
        FROM   STG_CUSTOMER_SALES s
        WHERE  s.LOAD_DATE   = p_load_date
        AND    s.REGION_CODE = p_region_code
        AND    NOT EXISTS (
               SELECT 1 FROM DIM_CUSTOMER d
               WHERE  d.CUSTOMER_ID = s.CUSTOMER_ID
               AND    d.IS_CURRENT  = 'Y'
        );

        p_rows_loaded := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< LOAD_DIM_CUSTOMER complete. Rows=' || p_rows_loaded);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR in LOAD_DIM_CUSTOMER: ' || SQLERRM);
            RAISE;
    END LOAD_DIM_CUSTOMER;

    -- -------------------------------------------------------------------------
    -- PROC: LOAD_FACT_SALES
    -- Loads FACT_DAILY_SALES by joining staging to current dimension keys.
    -- -------------------------------------------------------------------------
    PROCEDURE LOAD_FACT_SALES(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2,
        p_batch_id    IN NUMBER,
        p_rows_loaded OUT NUMBER
    ) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> LOAD_FACT_SALES: date=' || TO_CHAR(p_load_date,'YYYY-MM-DD') || ' region=' || p_region_code);

        INSERT /*+ APPEND */ INTO FACT_DAILY_SALES (
            FACT_SALES_KEY, DIM_PRODUCT_KEY, DIM_CUSTOMER_KEY, DIM_STORE_KEY,
            TRANSACTION_DATE,
            FISCAL_YEAR, FISCAL_MONTH, FISCAL_WEEK,
            QUANTITY, UNIT_PRICE, DISCOUNT_AMT,
            GROSS_AMOUNT, NET_AMOUNT, TAX_AMOUNT,
            REGION_CODE, CURRENCY_CODE,
            LOAD_DATE, LOAD_BATCH_ID
        )
        SELECT
            SEQ_FACT_SALES_KEY.NEXTVAL,
            dp.DIM_PRODUCT_KEY,
            dc.DIM_CUSTOMER_KEY,
            ds.DIM_STORE_KEY,
            s.TRANSACTION_DATE,
            TO_NUMBER(TO_CHAR(s.TRANSACTION_DATE, 'YYYY')),
            TO_NUMBER(TO_CHAR(s.TRANSACTION_DATE, 'MM')),
            TO_NUMBER(TO_CHAR(s.TRANSACTION_DATE, 'IW')),
            s.QUANTITY,
            s.UNIT_PRICE,
            s.DISCOUNT_AMT,
            s.QUANTITY * s.UNIT_PRICE                       AS GROSS_AMOUNT,
            (s.QUANTITY * s.UNIT_PRICE) - s.DISCOUNT_AMT   AS NET_AMOUNT,
            ROUND(((s.QUANTITY * s.UNIT_PRICE) - s.DISCOUNT_AMT) * 0.20, 4) AS TAX_AMOUNT,
            s.REGION_CODE,
            s.CURRENCY_CODE,
            SYSDATE,
            p_batch_id
        FROM   STG_SALES_TRANSACTIONS s
        JOIN   DIM_PRODUCT  dp ON dp.PRODUCT_ID  = s.PRODUCT_ID  AND dp.IS_CURRENT = 'Y'
        JOIN   DIM_CUSTOMER dc ON dc.CUSTOMER_ID = s.CUSTOMER_ID AND dc.IS_CURRENT = 'Y'
        LEFT JOIN DIM_STORE ds ON ds.STORE_ID    = s.STORE_ID    AND ds.IS_CURRENT = 'Y'
        WHERE  s.TRANSACTION_DATE = p_load_date
        AND    s.REGION_CODE      = p_region_code
        AND    s.ETL_STATUS       = 'PENDING';

        p_rows_loaded := SQL%ROWCOUNT;

        -- Mark staging records as processed
        UPDATE STG_SALES_TRANSACTIONS
        SET    ETL_STATUS    = 'LOADED',
               LOAD_BATCH_ID = p_batch_id
        WHERE  TRANSACTION_DATE = p_load_date
        AND    REGION_CODE      = p_region_code
        AND    ETL_STATUS       = 'PENDING';

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< LOAD_FACT_SALES complete. Rows=' || p_rows_loaded);
    END LOAD_FACT_SALES;

    -- -------------------------------------------------------------------------
    -- PROC: GENERATE_REGIONAL_SUMMARY
    -- Aggregates FACT_DAILY_SALES into FACT_REGIONAL_SUMMARY.
    -- Uses EXECUTE IMMEDIATE to build region-filtered dynamic delete + insert.
    -- -------------------------------------------------------------------------
    PROCEDURE GENERATE_REGIONAL_SUMMARY(
        p_summary_date IN DATE,
        p_region_code  IN VARCHAR2
    ) IS
        v_sql    VARCHAR2(2000);
        v_region VARCHAR2(10) := UPPER(TRIM(p_region_code));
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> GENERATE_REGIONAL_SUMMARY: ' || v_region || ' ' || TO_CHAR(p_summary_date,'YYYY-MM-DD'));

        -- Remove existing summary for this date/region (idempotent)
        v_sql := 'DELETE FROM FACT_REGIONAL_SUMMARY '
              || 'WHERE SUMMARY_DATE = :1 '
              || 'AND   REGION_CODE  = :2';
        EXECUTE IMMEDIATE v_sql USING p_summary_date, v_region;

        -- Re-aggregate from fact table
        INSERT INTO FACT_REGIONAL_SUMMARY (
            SUMMARY_KEY, REGION_CODE, SUMMARY_DATE,
            TOTAL_TRANSACTIONS, TOTAL_QUANTITY, TOTAL_REVENUE,
            AVG_BASKET_SIZE, DISTINCT_CUSTOMERS, DISTINCT_PRODUCTS,
            LOAD_DATE, LOAD_BATCH_ID
        )
        SELECT SEQ_FACT_REGIONAL.NEXTVAL,
               REGION_CODE,
               TRANSACTION_DATE,
               COUNT(1)                 AS TOTAL_TRANSACTIONS,
               SUM(QUANTITY)            AS TOTAL_QUANTITY,
               SUM(NET_AMOUNT)          AS TOTAL_REVENUE,
               AVG(NET_AMOUNT)          AS AVG_BASKET_SIZE,
               COUNT(DISTINCT DIM_CUSTOMER_KEY) AS DISTINCT_CUSTOMERS,
               COUNT(DISTINCT DIM_PRODUCT_KEY)  AS DISTINCT_PRODUCTS,
               SYSDATE,
               SEQ_BATCH_ID.CURRVAL
        FROM   FACT_DAILY_SALES
        WHERE  TRANSACTION_DATE = p_summary_date
        AND    REGION_CODE      = v_region
        GROUP BY REGION_CODE, TRANSACTION_DATE;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< GENERATE_REGIONAL_SUMMARY complete.');
    END GENERATE_REGIONAL_SUMMARY;

    -- -------------------------------------------------------------------------
    -- PROC: MASTER_LOAD
    -- Orchestrates full daily load for a given date and region.
    -- -------------------------------------------------------------------------
    PROCEDURE MASTER_LOAD(
        p_load_date   IN DATE,
        p_region_code IN VARCHAR2
    ) IS
        v_batch_id   NUMBER;
        v_rows_prod  NUMBER := 0;
        v_rows_cust  NUMBER := 0;
        v_rows_fact  NUMBER := 0;
    BEGIN
        v_batch_id := GET_BATCH_ID();
        DBMS_OUTPUT.PUT_LINE('=== MASTER_LOAD START: batch=' || v_batch_id
            || ' date=' || TO_CHAR(p_load_date,'YYYY-MM-DD')
            || ' region=' || p_region_code);

        LOAD_DIM_PRODUCT(p_load_date, v_batch_id, v_rows_prod);
        LOAD_DIM_CUSTOMER(p_load_date, p_region_code, v_batch_id, v_rows_cust);
        LOAD_FACT_SALES(p_load_date, p_region_code, v_batch_id, v_rows_fact);
        GENERATE_REGIONAL_SUMMARY(p_load_date, p_region_code);

        DBMS_OUTPUT.PUT_LINE('=== MASTER_LOAD END: products=' || v_rows_prod
            || ' customers=' || v_rows_cust || ' facts=' || v_rows_fact);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('FATAL ERROR in MASTER_LOAD: ' || SQLERRM);
            RAISE;
    END MASTER_LOAD;

END PKG_SALES_HISTORIZATION;
/
