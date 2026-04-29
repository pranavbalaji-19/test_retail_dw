-- =============================================================================
-- Ab Initio Graph (.mp) -- Master Program
-- Project  : retail_sales_transform
-- Graph    : retail_daily_sales.mp
-- Purpose  : Top-level orchestration graph for daily retail sales ETL:
--            extract transactions, validate, enrich with product dimension,
--            aggregate to regional daily summaries, and load to DW.
--            Invokes sales_rollup.xfr as the core transform logic.
--            Scheduled daily by UC4 (retail_daily_workflow.xml).
--            Region-parallel: runs once per REGION_CODE (NORTH/SOUTH/EAST/WEST).
--
-- Cross-repo consumers:
--   - crm_analytics_legacy reads DW_OWNER.STG_CUSTOMER_SALES
--   - crm_analytics_legacy reads DW_OWNER.FACT_REGIONAL_SUMMARY
--
-- Publishes event: RETAIL_DAILY_COMPLETE
--   Consumed by: crm_analytics_legacy (crm_weekly_workflow.xml)
--
-- Pre-requisite events:
--   - FINANCE_DAILY_GL_CLOSE (from finance_etl_legacy) before product master
-- =============================================================================

graph retail_daily_sales;

parameters
  -- Runtime parameters (from load_daily_sales.ksh / UC4)
  string LOAD_DATE             = "${LOAD_DATE}";
  string REGION_CODE           = "${REGION_CODE}";
  string BATCH_MODE            = "${BATCH_MODE:-FULL}";
  string BATCH_ID              = "${BATCH_ID}";
  string FISCAL_QTR            = "${FISCAL_QTR}";

  -- Connection parameters
  string ORA_CONNECT_STRING    = "${ORA_CONNECT_STRING}";
  string SOURCE_SCHEMA         = "SOURCE_OPS";

  -- Schema and sub-graph references
  string SCHEMA_FILE           = "retail_sales.dml";
  string ROLLUP_TRANSFORM      = "sales_rollup.xfr";

  -- Output paths
  string ERROR_OUTPUT_DIR      = "/opt/etl/errors";
  string LOG_DIR               = "/opt/etl/logs/retail";

  -- Parallelism and thresholds
  integer AB_PARALLELISM       = 4;
  integer MIN_ROW_COUNT        = 100;
  decimal MIN_ROW_RATIO        = 0.80;
end;


-- ==========================================================================
-- Phase 1: Pre-checks and Source Reads (parallel)
-- ==========================================================================

-- Check lock file / prevent duplicate runs (mirrors load_daily_sales.ksh logic)
check_lock: run_program
  command    = "test ! -f /tmp/retail_daily_${REGION_CODE}.lock && "
               "echo $$ > /tmp/retail_daily_${REGION_CODE}.lock";
  success_codes = "0";
  error_message = "Duplicate run detected for region " || REGION_CODE;
end;

-- Read raw sales transactions from source operational system
read_stg_transactions: input_table
  database   = ORA_CONNECT_STRING;
  table      = SOURCE_SCHEMA || ".SALES_TXN";
  query      = "SELECT t.*, " || BATCH_ID || " AS load_batch_id "
               "FROM " || SOURCE_SCHEMA || ".SALES_TXN t "
               "WHERE t.TXN_DATE = TO_DATE('" || LOAD_DATE || "','YYYY-MM-DD') "
               "AND t.REGION_CODE = '" || REGION_CODE || "' "
               "AND t.TXN_STATUS = 'COMPLETED' "
               "AND t.QUANTITY > 0";
  record_format = SCHEMA_FILE : "stg_sales_record";
  fetch_size = 10000;
end;

-- Read current product dimension for enrichment
read_dim_product: input_table
  database   = ORA_CONNECT_STRING;
  table      = "DIM_PRODUCT";
  query      = "SELECT product_id, product_code, product_name, "
               "category_code, subcategory_code, list_price, "
               "is_current, valid_from, valid_to "
               "FROM DIM_PRODUCT WHERE IS_CURRENT = 'Y'";
  record_format = SCHEMA_FILE : "product_dim_lookup";
end;

-- Read previous day's row count for threshold comparison
read_prev_day_count: run_program
  command    = "sqlplus -s " || ORA_CONNECT_STRING
               || " -e \"SELECT COUNT(*) FROM FACT_DAILY_SALES "
               || "WHERE TRANSACTION_DATE = TO_DATE('" || LOAD_DATE || "','YYYY-MM-DD')-1 "
               || "AND REGION_CODE = '" || REGION_CODE || "';\"";
  capture_output = true;
end;


-- ==========================================================================
-- Phase 2: Validation and Filtering
-- ==========================================================================

-- Filter out invalid/zero-amount records
filter_valid: filter_by_expression
  input      = read_stg_transactions;
  keep       = "(quantity > 0.0) AND (unit_price > 0.0) AND (product_id IS NOT NULL)";
  reject     = "NOT ((quantity > 0.0) AND (unit_price > 0.0) AND (product_id IS NOT NULL))";
end;

-- Write rejected records to error file
write_rejects: output_file
  input      = filter_valid.reject;
  filename   = ERROR_OUTPUT_DIR || "/retail_rejected_" || REGION_CODE || "_" || LOAD_DATE || ".dat";
  delimiter  = "|";
  record_format = SCHEMA_FILE : "rejected_record";
end;


-- ==========================================================================
-- Phase 3: Core Transform (references sales_rollup.xfr)
-- ==========================================================================

-- Enrich with derived fields: gross/net amounts, fiscal period, loyalty tier
enrich_fields: reformat
  input      = filter_valid.keep;
  transform  = ROLLUP_TRANSFORM;
  record_format = SCHEMA_FILE : "stg_sales_record";
end;

-- Join with product dimension (LEFT OUTER)
join_product: join
  input[0]   = enrich_fields;
  input[1]   = read_dim_product;
  keys       = "product_id";
  join_type  = "left_outer";
  record_format = SCHEMA_FILE : "enriched_sales_record";
end;

-- Write unmatched transactions (no product dim record)
write_unmatched: output_file
  input      = join_product.unmatched;
  filename   = ERROR_OUTPUT_DIR || "/retail_unmatched_product_" || REGION_CODE || "_" || LOAD_DATE || ".dat";
  delimiter  = "|";
end;

-- Aggregate to regional daily summary
rollup_regional: rollup
  input      = join_product;
  keys       = "region_code, transaction_date, category_code, fiscal_year, fiscal_month";
  record_format = SCHEMA_FILE : "regional_summary_record";
  transform  = <<SALES_ROLLUP
    initialise:
      out.region_code           = in.region_code;
      out.summary_date          = in.transaction_date;
      out.category_code         = in.category_code;
      out.fiscal_year           = in.fiscal_year;
      out.fiscal_month          = in.fiscal_month;
      out.total_transactions    = 0;
      out.total_quantity        = 0.0;
      out.total_gross_revenue   = 0.0;
      out.total_net_revenue     = 0.0;
      out.total_discount_amount = 0.0;
      out.avg_basket_value      = 0.0;
      out.distinct_customers    = 0;
      out.distinct_products     = 0;
      out.distinct_stores       = 0;
    aggregate:
      out.total_transactions    += 1;
      out.total_quantity        += in.quantity;
      out.total_gross_revenue   += in.gross_amount;
      out.total_net_revenue     += in.net_amount;
      out.total_discount_amount += in.discount_amt;
    finalise:
      out.avg_basket_value = out.total_net_revenue / max(out.total_transactions, 1);
  SALES_ROLLUP
end;


-- ==========================================================================
-- Phase 4: Outputs and Post-validation
-- ==========================================================================

-- Write enriched transactions to FACT_DAILY_SALES
write_fact_sales: output_table
  input      = join_product;
  database   = ORA_CONNECT_STRING;
  table      = "FACT_DAILY_SALES";
  mode       = "insert_append";
end;

-- Write regional summary to FACT_REGIONAL_SUMMARY (shared table — consumed by CRM)
write_regional_summary: output_table
  input      = rollup_regional;
  database   = ORA_CONNECT_STRING;
  table      = "FACT_REGIONAL_SUMMARY";
  mode       = "insert_append";
end;

-- Row count validation against previous day
validate_row_count: run_program
  command    = "sqlplus -s " || ORA_CONNECT_STRING
               || " @validate_sales_counts.sql "
               || LOAD_DATE || " " || REGION_CODE || " "
               || string(MIN_ROW_COUNT) || " " || string(MIN_ROW_RATIO);
  success_codes = "0";
  error_message = "Row count validation failed for " || REGION_CODE || " on " || LOAD_DATE;
end;

-- Remove lock file on completion
release_lock: run_program
  command    = "rm -f /tmp/retail_daily_${REGION_CODE}.lock";
  success_codes = "0,1";
end;

-- Audit log
write_audit: output_file
  input      = rollup_regional;
  filename   = LOG_DIR || "/retail_daily_audit_" || REGION_CODE || "_" || LOAD_DATE || ".log";
  mode       = "count_only";
end;


-- ==========================================================================
-- Flow Definition
-- ==========================================================================

-- Phase 1: lock check then parallel reads
check_lock -> read_stg_transactions, read_dim_product, read_prev_day_count;

-- Phase 2: validation
read_stg_transactions   -> filter_valid;
filter_valid.reject     -> write_rejects;

-- Phase 3: transform chain
filter_valid.keep       -> enrich_fields;
enrich_fields           -> join_product;
read_dim_product        -> join_product;
join_product.unmatched  -> write_unmatched;
join_product            -> rollup_regional;

-- Phase 4: outputs (parallel)
join_product            -> write_fact_sales;
rollup_regional         -> write_regional_summary;
rollup_regional         -> write_audit;

-- Phase 5: post-validation (after writes complete)
write_fact_sales        -> validate_row_count;
validate_row_count      -> release_lock;

end;
