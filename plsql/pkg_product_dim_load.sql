-- =============================================================================
-- Package: PKG_PRODUCT_DIM_LOAD
-- Description: Dynamic category-based product dimension loader.
--              Uses EXECUTE IMMEDIATE to handle per-category rule overrides
--              stored in a config table. High complexity: nested dynamic SQL,
--              DBMS_SQL cursor, runtime table name resolution.
-- Schema: DW_OWNER
-- =============================================================================

CREATE OR REPLACE PACKAGE PKG_PRODUCT_DIM_LOAD AS

    PROCEDURE APPLY_CATEGORY_RULES(
        p_category_code IN VARCHAR2,
        p_load_date     IN DATE
    );

    PROCEDURE BULK_ENRICH_PRODUCTS(
        p_load_date     IN DATE,
        p_source_schema IN VARCHAR2 DEFAULT 'DW_OWNER'
    );

    PROCEDURE PURGE_EXPIRED_VERSIONS(
        p_retention_days IN NUMBER DEFAULT 730
    );

END PKG_PRODUCT_DIM_LOAD;
/

CREATE OR REPLACE PACKAGE BODY PKG_PRODUCT_DIM_LOAD AS

    -- -------------------------------------------------------------------------
    -- PROC: APPLY_CATEGORY_RULES
    -- Applies category-specific transformation rules loaded dynamically.
    -- Rule table name is built at runtime from the category code.
    -- -------------------------------------------------------------------------
    PROCEDURE APPLY_CATEGORY_RULES(
        p_category_code IN VARCHAR2,
        p_load_date     IN DATE
    ) IS
        v_rule_table    VARCHAR2(100);
        v_rule_sql      VARCHAR2(4000);
        v_update_sql    VARCHAR2(4000);
        v_rule_count    NUMBER;
        v_rule_name     VARCHAR2(100);
        v_rule_expr     VARCHAR2(500);
        v_col_target    VARCHAR2(50);

        TYPE t_rule_rec IS RECORD (
            rule_name   VARCHAR2(100),
            col_target  VARCHAR2(50),
            rule_expr   VARCHAR2(500)
        );
        TYPE t_rule_tab IS TABLE OF t_rule_rec;
        v_rules t_rule_tab;

        v_cur   SYS_REFCURSOR;
    BEGIN
        -- Build rule table name dynamically based on category
        v_rule_table := 'CAT_RULES_' || UPPER(REPLACE(p_category_code, '-', '_'));
        DBMS_OUTPUT.PUT_LINE('Applying rules from: ' || v_rule_table);

        -- Check if category-specific rule table exists
        SELECT COUNT(*) INTO v_rule_count
        FROM   ALL_TABLES
        WHERE  TABLE_NAME = v_rule_table
        AND    OWNER      = 'DW_OWNER';

        IF v_rule_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('No custom rule table for category ' || p_category_code || ' - using defaults');
            RETURN;
        END IF;

        -- Fetch rules dynamically from the category-specific table
        v_rule_sql := 'SELECT rule_name, col_target, rule_expr '
                   || 'FROM ' || v_rule_table
                   || ' WHERE is_active = ''Y'' '
                   || ' ORDER BY execution_order';

        OPEN v_cur FOR v_rule_sql;
        FETCH v_cur BULK COLLECT INTO v_rules;
        CLOSE v_cur;

        -- Apply each rule via EXECUTE IMMEDIATE
        FOR i IN 1 .. v_rules.COUNT LOOP
            v_update_sql :=
                'UPDATE DIM_PRODUCT SET '
             || v_rules(i).col_target || ' = (' || v_rules(i).rule_expr || ') '
             || 'WHERE CATEGORY_CODE = :cat '
             || '  AND IS_CURRENT    = ''Y'' '
             || '  AND VALID_FROM    = :dt';

            BEGIN
                EXECUTE IMMEDIATE v_update_sql USING p_category_code, p_load_date;
                DBMS_OUTPUT.PUT_LINE('  Applied rule: ' || v_rules(i).rule_name
                    || ' (' || SQL%ROWCOUNT || ' rows)');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  WARN: Rule ' || v_rules(i).rule_name
                        || ' failed: ' || SQLERRM);
            END;
        END LOOP;

        COMMIT;
    END APPLY_CATEGORY_RULES;

    -- -------------------------------------------------------------------------
    -- PROC: BULK_ENRICH_PRODUCTS
    -- Enriches DIM_PRODUCT from supplier schemas using dynamic cross-schema SQL.
    -- Source schema name is a runtime parameter - allows multi-source loading.
    -- -------------------------------------------------------------------------
    PROCEDURE BULK_ENRICH_PRODUCTS(
        p_load_date     IN DATE,
        p_source_schema IN VARCHAR2 DEFAULT 'DW_OWNER'
    ) IS
        v_sql            VARCHAR2(4000);
        v_supplier_tbl   VARCHAR2(100);
        v_cnt            NUMBER;

        -- Cursor to iterate over distinct suppliers needing enrichment
        CURSOR cur_suppliers IS
            SELECT DISTINCT SUPPLIER_ID
            FROM   DIM_PRODUCT
            WHERE  IS_CURRENT = 'Y'
            AND    VALID_FROM  = p_load_date
            AND    SUPPLIER_ID IS NOT NULL;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('>>> BULK_ENRICH_PRODUCTS from schema: ' || p_source_schema);

        FOR sup IN cur_suppliers LOOP
            -- Each supplier may have own staging table: SUPPLIER_<ID>_PRODUCTS
            v_supplier_tbl := p_source_schema || '.SUPPLIER_' || sup.SUPPLIER_ID || '_PRODUCTS';

            -- Check table exists before querying
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM ALL_TABLES '
             || 'WHERE OWNER = :sch AND TABLE_NAME = :tbl'
            INTO v_cnt
            USING p_source_schema,
                  'SUPPLIER_' || sup.SUPPLIER_ID || '_PRODUCTS';

            IF v_cnt > 0 THEN
                -- Merge enrichment data from supplier-specific table
                v_sql :=
                    'MERGE INTO DIM_PRODUCT tgt '
                 || 'USING ( '
                 || '    SELECT product_id, '
                 || '           list_price   AS enriched_price, '
                 || '           cost_price   AS enriched_cost, '
                 || '           weight_kg    AS enriched_weight '
                 || '    FROM ' || v_supplier_tbl
                 || '    WHERE effective_date <= :eff_dt '
                 || ') src '
                 || 'ON (tgt.PRODUCT_ID = src.product_id AND tgt.IS_CURRENT = ''Y'') '
                 || 'WHEN MATCHED THEN UPDATE SET '
                 || '    tgt.LIST_PRICE  = src.enriched_price, '
                 || '    tgt.COST_PRICE  = src.enriched_cost, '
                 || '    tgt.UPDATED_DATE = SYSDATE';

                EXECUTE IMMEDIATE v_sql USING p_load_date;
                DBMS_OUTPUT.PUT_LINE('  Enriched supplier ' || sup.SUPPLIER_ID || ': ' || SQL%ROWCOUNT || ' rows');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  Supplier table not found: ' || v_supplier_tbl || ' - skipping');
            END IF;
        END LOOP;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< BULK_ENRICH_PRODUCTS complete.');
    END BULK_ENRICH_PRODUCTS;

    -- -------------------------------------------------------------------------
    -- PROC: PURGE_EXPIRED_VERSIONS
    -- Removes very old SCD Type 2 history beyond retention period.
    -- Builds partition-aware DELETE using EXECUTE IMMEDIATE.
    -- -------------------------------------------------------------------------
    PROCEDURE PURGE_EXPIRED_VERSIONS(
        p_retention_days IN NUMBER DEFAULT 730
    ) IS
        v_cutoff_date DATE;
        v_sql         VARCHAR2(1000);
        v_rows_del    NUMBER;
        v_part_count  NUMBER;
    BEGIN
        v_cutoff_date := SYSDATE - p_retention_days;
        DBMS_OUTPUT.PUT_LINE('>>> PURGE_EXPIRED_VERSIONS: cutoff=' || TO_CHAR(v_cutoff_date,'YYYY-MM-DD'));

        -- Check if table is partitioned (affects delete strategy)
        SELECT COUNT(*) INTO v_part_count
        FROM   ALL_TAB_PARTITIONS
        WHERE  TABLE_NAME = 'DIM_PRODUCT'
        AND    TABLE_OWNER = 'DW_OWNER';

        IF v_part_count > 0 THEN
            -- Partition-aware delete: drop individual partitions whose VALID_TO < cutoff
            DBMS_OUTPUT.PUT_LINE('  Table is partitioned - using partition drop strategy');
            v_sql :=
                'ALTER TABLE DIM_PRODUCT DROP PARTITION FOR (DATE ''' ||
                TO_CHAR(v_cutoff_date, 'YYYY-MM-DD') || ''')';
            -- Note: only execute if partition exists; wrapped in exception handler
            BEGIN
                EXECUTE IMMEDIATE v_sql;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  Partition drop skipped: ' || SQLERRM);
            END;
        ELSE
            -- Standard row-level delete
            v_sql :=
                'DELETE FROM DIM_PRODUCT '
             || 'WHERE IS_CURRENT = ''N'' '
             || '  AND VALID_TO   < :cutoff';
            EXECUTE IMMEDIATE v_sql USING v_cutoff_date;
            v_rows_del := SQL%ROWCOUNT;
            DBMS_OUTPUT.PUT_LINE('  Deleted ' || v_rows_del || ' expired rows');
        END IF;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('<<< PURGE_EXPIRED_VERSIONS complete.');
    END PURGE_EXPIRED_VERSIONS;

END PKG_PRODUCT_DIM_LOAD;
/
