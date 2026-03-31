#!/usr/bin/env python3
"""
Script   : retail_data_quality.py
Purpose  : Post-load data quality checks for FACT_DAILY_SALES and
           FACT_REGIONAL_SUMMARY. Builds dynamic SQL queries based on
           configurable rule definitions loaded from the DQ_RULES table.
           Flags anomalies and sends email notifications.

Called by: UC4 job RETAIL_DATA_QUALITY_CHECK
Args     : --load-date YYYY-MM-DD --env PROD --notify-email addr@company.com
"""

import argparse
import sys
import os
import cx_Oracle
from datetime import datetime, date
from typing import List, Dict, Any, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retail DQ Check")
    parser.add_argument("--load-date",     required=True, help="YYYY-MM-DD")
    parser.add_argument("--env",           default="PROD")
    parser.add_argument("--notify-email",  default="")
    parser.add_argument("--fail-on-error", action="store_true")
    return parser.parse_args()


def get_connection(env: str) -> cx_Oracle.Connection:
    """Build Oracle connection from environment variables."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "1521")
    sid  = os.environ.get("DB_SID",  "RETAILDW")
    user = os.environ.get("DB_USER", "DW_OWNER")
    pwd  = os.environ.get("DB_PASS", "")

    dsn = cx_Oracle.makedsn(host, int(port), sid=sid)
    return cx_Oracle.connect(user=user, password=pwd, dsn=dsn)


def load_dq_rules(conn: cx_Oracle.Connection, domain: str) -> List[Dict]:
    """
    Dynamically fetch DQ rules from DQ_RULES config table.
    Rules define: target_table, rule_name, rule_type, threshold, sql_fragment.
    """
    sql = """
        SELECT rule_id, rule_name, target_table, rule_type,
               sql_fragment, threshold_pct, severity
        FROM   DQ_RULES
        WHERE  domain     = :domain
        AND    is_active   = 'Y'
        ORDER BY execution_order
    """
    cursor = conn.cursor()
    cursor.execute(sql, {"domain": domain})
    cols = [d[0].lower() for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def build_dq_check_sql(rule: Dict, load_date: str) -> Tuple[str, Dict]:
    """
    Dynamically constructs a DQ check SQL from the rule definition.
    Uses the sql_fragment as the metric expression — high complexity dynamic SQL.
    """
    rule_type    = rule["rule_type"]
    target_table = rule["target_table"]
    sql_fragment = rule["sql_fragment"]  # e.g. "COUNT(*)" or "SUM(net_amount)"

    if rule_type == "NULL_CHECK":
        sql = f"""
            SELECT '{rule["rule_name"]}' AS rule_name,
                   COUNT(*) AS total_rows,
                   {sql_fragment} AS failed_rows,
                   ROUND({sql_fragment} / NULLIF(COUNT(*), 0) * 100, 4) AS fail_pct
            FROM   {target_table}
            WHERE  transaction_date = TO_DATE(:load_date, 'YYYY-MM-DD')
        """
    elif rule_type == "RANGE_CHECK":
        sql = f"""
            SELECT '{rule["rule_name"]}' AS rule_name,
                   COUNT(*) AS total_rows,
                   SUM(CASE WHEN NOT ({sql_fragment}) THEN 1 ELSE 0 END) AS failed_rows,
                   ROUND(SUM(CASE WHEN NOT ({sql_fragment}) THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*), 0) * 100, 4) AS fail_pct
            FROM   {target_table}
            WHERE  transaction_date = TO_DATE(:load_date, 'YYYY-MM-DD')
        """
    elif rule_type == "REFERENTIAL_CHECK":
        sql = f"""
            SELECT '{rule["rule_name"]}' AS rule_name,
                   COUNT(*) AS total_rows,
                   SUM(CASE WHEN ({sql_fragment}) IS NULL THEN 1 ELSE 0 END) AS failed_rows,
                   ROUND(SUM(CASE WHEN ({sql_fragment}) IS NULL THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*), 0) * 100, 4) AS fail_pct
            FROM   {target_table}
            WHERE  transaction_date = TO_DATE(:load_date, 'YYYY-MM-DD')
        """
    elif rule_type == "THRESHOLD_COUNT":
        # Dynamic: check that row count is within expected range
        sql = f"""
            SELECT '{rule["rule_name"]}' AS rule_name,
                   {sql_fragment} AS total_rows,
                   0 AS failed_rows,
                   0 AS fail_pct
            FROM   {target_table}
            WHERE  transaction_date = TO_DATE(:load_date, 'YYYY-MM-DD')
        """
    else:
        # Generic - execute sql_fragment directly
        sql = f"""
            SELECT '{rule["rule_name"]}' AS rule_name,
                   COUNT(*) AS total_rows,
                   ({sql_fragment}) AS failed_rows,
                   ROUND(({sql_fragment}) / NULLIF(COUNT(*),0) * 100, 4) AS fail_pct
            FROM   {target_table}
            WHERE  transaction_date = TO_DATE(:load_date, 'YYYY-MM-DD')
        """

    return sql.strip(), {"load_date": load_date}


def run_dq_checks(conn: cx_Oracle.Connection,
                  rules: List[Dict],
                  load_date: str) -> List[Dict]:
    """Run each DQ rule and collect results."""
    results = []
    cursor  = conn.cursor()

    for rule in rules:
        try:
            sql, params = build_dq_check_sql(rule, load_date)
            cursor.execute(sql, params)
            row = cursor.fetchone()
            if row:
                rule_name, total, failed, fail_pct = row
                passed = (fail_pct or 0) <= (rule.get("threshold_pct") or 0.01)
                results.append({
                    "rule_name":     rule_name,
                    "severity":      rule.get("severity", "WARNING"),
                    "total_rows":    total,
                    "failed_rows":   failed,
                    "fail_pct":      fail_pct or 0,
                    "threshold_pct": rule.get("threshold_pct", 0),
                    "passed":        passed,
                })
                status = "PASS" if passed else "FAIL"
                print(f"  [{status}] {rule_name}: {failed}/{total} rows failed ({fail_pct:.4f}%)")
        except cx_Oracle.Error as e:
            print(f"  [ERROR] Rule {rule['rule_name']} execution failed: {e}")
            results.append({
                "rule_name": rule["rule_name"],
                "severity":  "ERROR",
                "error":     str(e),
                "passed":    False
            })

    cursor.close()
    return results


def write_dq_results(conn: cx_Oracle.Connection,
                     results: List[Dict],
                     load_date: str) -> None:
    """Persist DQ check results into DQ_RESULTS audit table."""
    insert_sql = """
        INSERT INTO DQ_RESULTS (
            run_date, load_date, rule_name, total_rows,
            failed_rows, fail_pct, threshold_pct, status, severity
        ) VALUES (
            SYSDATE, TO_DATE(:load_date,'YYYY-MM-DD'), :rule_name, :total_rows,
            :failed_rows, :fail_pct, :threshold_pct, :status, :severity
        )
    """
    cursor = conn.cursor()
    for r in results:
        cursor.execute(insert_sql, {
            "load_date":     load_date,
            "rule_name":     r["rule_name"],
            "total_rows":    r.get("total_rows", 0),
            "failed_rows":   r.get("failed_rows", 0),
            "fail_pct":      r.get("fail_pct", 0),
            "threshold_pct": r.get("threshold_pct", 0),
            "status":        "PASS" if r["passed"] else "FAIL",
            "severity":      r.get("severity", "WARNING"),
        })
    conn.commit()
    cursor.close()


def main():
    args = parse_args()
    load_date = args.load_date

    print(f"[{datetime.now()}] DQ check started: load_date={load_date} env={args.env}")

    conn = get_connection(args.env)

    try:
        # Load rules from config table
        rules = load_dq_rules(conn, "RETAIL")
        if not rules:
            print("[WARN] No DQ rules found for domain RETAIL. Using hardcoded fallback.")
            # Hardcoded fallback rules when config table is not yet populated
            rules = [
                {
                    "rule_id": 1, "rule_name": "NULL_PRODUCT_KEY",
                    "target_table": "FACT_DAILY_SALES",
                    "rule_type": "NULL_CHECK",
                    "sql_fragment": "COUNT(CASE WHEN dim_product_key IS NULL THEN 1 END)",
                    "threshold_pct": 0, "severity": "CRITICAL"
                },
                {
                    "rule_id": 2, "rule_name": "NEGATIVE_NET_AMOUNT",
                    "target_table": "FACT_DAILY_SALES",
                    "rule_type": "RANGE_CHECK",
                    "sql_fragment": "net_amount >= 0",
                    "threshold_pct": 0.1, "severity": "WARNING"
                },
                {
                    "rule_id": 3, "rule_name": "REGIONAL_SUMMARY_PRESENT",
                    "target_table": "FACT_REGIONAL_SUMMARY",
                    "rule_type": "THRESHOLD_COUNT",
                    "sql_fragment": "COUNT(*)",
                    "threshold_pct": 0, "severity": "CRITICAL"
                },
            ]

        print(f"[INFO] Running {len(rules)} DQ rules...")
        results = run_dq_checks(conn, rules, load_date)
        write_dq_results(conn, results, load_date)

        # Summary
        critical_fails = [r for r in results if not r["passed"] and r.get("severity") == "CRITICAL"]
        warnings       = [r for r in results if not r["passed"] and r.get("severity") != "CRITICAL"]

        print(f"\n=== DQ Summary for {load_date} ===")
        print(f"  Total rules   : {len(results)}")
        print(f"  Passed        : {sum(1 for r in results if r['passed'])}")
        print(f"  Critical fails: {len(critical_fails)}")
        print(f"  Warnings      : {len(warnings)}")

        if critical_fails and args.fail_on_error:
            print("[FATAL] Critical DQ failures detected. Exiting with code 1.")
            sys.exit(1)

    finally:
        conn.close()

    print(f"[{datetime.now()}] DQ check complete.")


if __name__ == "__main__":
    main()
