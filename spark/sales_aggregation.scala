// =============================================================================
// Spark Job   : SalesAggregation.scala
// Package     : com.company.retail
// Description : Reads daily sales from Oracle staging/fact tables, computes
//               advanced analytical aggregations, and writes to the analytical
//               layer. Reads STG_CUSTOMER_SALES (shared with CRM repo).
// =============================================================================

package com.company.retail

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.util.Properties

object SalesAggregation {

  case class Config(
    loadDate: String  = "",
    env: String       = "PROD",
    oraHost: String   = "",
    oraPort: String   = "1521",
    oraSid: String    = "",
    oraUser: String   = "",
    oraPass: String   = ""
  )

  def parseArgs(args: Array[String]): Config = {
    var cfg = Config()
    args.sliding(2, 2).foreach {
      case Array("--load-date", v) => cfg = cfg.copy(loadDate = v)
      case Array("--env",       v) => cfg = cfg.copy(env = v)
      case Array("--ora-host",  v) => cfg = cfg.copy(oraHost = v)
      case Array("--ora-user",  v) => cfg = cfg.copy(oraUser = v)
      case Array("--ora-pass",  v) => cfg = cfg.copy(oraPass = v)
      case Array("--ora-sid",   v) => cfg = cfg.copy(oraSid = v)
      case _ => // ignore unknown
    }
    require(cfg.loadDate.nonEmpty, "--load-date is required")
    cfg
  }

  def buildJdbcUrl(cfg: Config): String =
    s"jdbc:oracle:thin:@${cfg.oraHost}:${cfg.oraPort}:${cfg.oraSid}"

  def jdbcProps(cfg: Config): Properties = {
    val p = new Properties()
    p.put("user",   cfg.oraUser)
    p.put("password", cfg.oraPass)
    p.put("driver", "oracle.jdbc.OracleDriver")
    p.put("fetchsize", "10000")
    p
  }

  // -------------------------------------------------------------------------
  // Read from Oracle via JDBC with a pushed-down SQL query
  // -------------------------------------------------------------------------
  def readOracleTable(spark: SparkSession, url: String, props: Properties,
                      query: String): DataFrame = {
    spark.read.jdbc(url, s"($query)", props)
  }

  // -------------------------------------------------------------------------
  // Compute product sales ranking per region using window functions
  // -------------------------------------------------------------------------
  def computeProductRankings(factDf: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("region_code", "fiscal_year", "fiscal_month")
      .orderBy(col("total_revenue").desc)

    factDf
      .groupBy("region_code", "dim_product_key", "fiscal_year", "fiscal_month")
      .agg(
        sum("net_amount").alias("total_revenue"),
        sum("quantity").alias("total_units"),
        count("*").alias("transaction_count"),
        avg("net_amount").alias("avg_transaction_value")
      )
      .withColumn("revenue_rank",    rank().over(windowSpec))
      .withColumn("revenue_dense_rank", dense_rank().over(windowSpec))
      .withColumn("revenue_pct_share",
        col("total_revenue") / sum("total_revenue").over(
          Window.partitionBy("region_code", "fiscal_year", "fiscal_month")
        ) * 100
      )
  }

  // -------------------------------------------------------------------------
  // Customer lifetime value segmentation
  // Reads STG_CUSTOMER_SALES (shared staging table from retail schema)
  // Also used by CRM repo (crm_analytics_legacy/spark/customer_segmentation.scala)
  // -------------------------------------------------------------------------
  def computeCustomerSegments(spark: SparkSession, url: String,
                               props: Properties, loadDate: String): DataFrame = {
    val customerQuery =
      s"""SELECT c.customer_id,
         |       c.customer_code,
         |       c.region_code,
         |       c.loyalty_tier,
         |       c.lifetime_value,
         |       c.last_purchase_date,
         |       f.total_spend_ytd,
         |       f.txn_count_ytd
         |FROM   stg_customer_sales c
         |LEFT JOIN (
         |    SELECT dim_customer_key,
         |           SUM(net_amount)  AS total_spend_ytd,
         |           COUNT(*)         AS txn_count_ytd
         |    FROM   fact_daily_sales
         |    WHERE  TO_CHAR(transaction_date,'YYYY') = SUBSTR('$loadDate',1,4)
         |    GROUP BY dim_customer_key
         |) f ON f.dim_customer_key = c.customer_id
         |WHERE  c.load_date >= TO_DATE('$loadDate','YYYY-MM-DD') - 7
         |""".stripMargin

    val customerDf = readOracleTable(spark, url, props, customerQuery)

    customerDf
      .withColumn("clv_segment",
        when(col("lifetime_value") > 5000, "HIGH_VALUE")
        .when(col("lifetime_value") > 1000, "MID_VALUE")
        .when(col("lifetime_value") > 100,  "LOW_VALUE")
        .otherwise("PROSPECT")
      )
      .withColumn("churn_risk",
        when(col("last_purchase_date").isNull, "UNKNOWN")
        .when(datediff(lit(loadDate).cast("date"), col("last_purchase_date")) > 180, "HIGH")
        .when(datediff(lit(loadDate).cast("date"), col("last_purchase_date")) > 90,  "MEDIUM")
        .otherwise("LOW")
      )
  }

  // -------------------------------------------------------------------------
  // Main entry point
  // -------------------------------------------------------------------------
  def main(args: Array[String]): Unit = {
    val cfg = parseArgs(args)
    val url   = buildJdbcUrl(cfg)
    val props = jdbcProps(cfg)

    val spark = SparkSession.builder()
      .appName(s"RetailSalesAggregation-${cfg.loadDate}")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    import spark.implicits._

    println(s"[INFO] Starting SalesAggregation for loadDate=${cfg.loadDate} env=${cfg.env}")

    // -----------------------------------------------------------------------
    // 1. Read FACT_DAILY_SALES for load date
    // -----------------------------------------------------------------------
    val factQuery =
      s"""SELECT f.fact_sales_key, f.dim_product_key, f.dim_customer_key,
         |       f.dim_store_key, f.transaction_date,
         |       f.fiscal_year, f.fiscal_month, f.fiscal_week,
         |       f.quantity, f.unit_price, f.discount_amt,
         |       f.gross_amount, f.net_amount, f.tax_amount,
         |       f.region_code, f.currency_code
         |FROM   fact_daily_sales f
         |WHERE  f.transaction_date = TO_DATE('${cfg.loadDate}','YYYY-MM-DD')
         |""".stripMargin

    val factDf = readOracleTable(spark, url, props, factQuery).cache()
    val factCount = factDf.count()
    println(s"[INFO] Fact rows read: $factCount")

    if (factCount == 0) {
      println(s"[WARN] No fact rows for ${cfg.loadDate}. Exiting.")
      spark.stop()
      return
    }

    // -----------------------------------------------------------------------
    // 2. Compute product performance rankings
    // -----------------------------------------------------------------------
    val productRankDf = computeProductRankings(factDf)
    println(s"[INFO] Product rankings computed: ${productRankDf.count()} rows")

    // -----------------------------------------------------------------------
    // 3. Compute customer segments (reads shared STG_CUSTOMER_SALES)
    // -----------------------------------------------------------------------
    val customerSegDf = computeCustomerSegments(spark, url, props, cfg.loadDate)
    println(s"[INFO] Customer segments computed: ${customerSegDf.count()} rows")

    // -----------------------------------------------------------------------
    // 4. Daily sales summary with running totals (window functions)
    // -----------------------------------------------------------------------
    val dailyWindow = Window.partitionBy("region_code").orderBy("transaction_date")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val dailySummaryDf = factDf
      .groupBy("region_code", "transaction_date", "fiscal_year", "fiscal_month")
      .agg(
        count("*").alias("total_transactions"),
        sum("net_amount").alias("daily_revenue"),
        sum("quantity").alias("daily_units"),
        avg("net_amount").alias("avg_basket"),
        sum("discount_amt").alias("total_discounts"),
        countDistinct("dim_customer_key").alias("unique_customers"),
        countDistinct("dim_product_key").alias("unique_products")
      )
      .withColumn("running_revenue_ytd",
        sum("daily_revenue").over(dailyWindow)
      )
      .withColumn("revenue_vs_prev_day",
        col("daily_revenue") - lag("daily_revenue", 1).over(
          Window.partitionBy("region_code").orderBy("transaction_date")
        )
      )

    // -----------------------------------------------------------------------
    // 5. Write analytical outputs back to Oracle
    // -----------------------------------------------------------------------
    productRankDf
      .write
      .mode(SaveMode.Append)
      .jdbc(url, "RETAIL_PRODUCT_RANKINGS", props)

    customerSegDf.select("customer_id","clv_segment","churn_risk","region_code")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "RETAIL_CUSTOMER_SEGMENTS", props)

    dailySummaryDf
      .write
      .mode(SaveMode.Append)
      .jdbc(url, "RETAIL_DAILY_ANALYTICAL_SUMMARY", props)

    println(s"[INFO] SalesAggregation completed successfully for ${cfg.loadDate}")
    spark.stop()
  }
}
