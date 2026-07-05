package com.kaizen.spark.utility

import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}

/** Migrate report.biz_rt_order (hot) rows older than hot window into Iceberg cold.
  * Chuyển dòng hot cũ hơn cửa sổ hot sang Iceberg cold (nessie.serving.biz_rt_order).
  *
  * After this job, refresh ClickHouse serving.biz_rt_order Iceberg engine
  * (Airflow: run_clickhouse_setup layer=serving).
  */
object MigrateBizRtToColdJob {

  private val ServingTable = "nessie.serving.biz_rt_order"
  private val StgTable = "nessie.stg.biz_rt_order"
  private val MergeKeys = Seq("OrderId", "ReturnId")

  def main(args: Array[String]): Unit = {
    val jobName = "MigrateBizRtToColdJob"
    val cli = SparkIOUtils.parseArgs(args)
    val hotDays = cli.get("hotDays").map(_.toInt).getOrElse(14)

    println(s"=== Starting $jobName hotDays=$hotDays ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val result = Try {
      val today =
        java.time.LocalDate.now(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
      val cutoff = today.minusDays(hotDays.toLong).toString
      println(s"Migrating rows with ReportDate < $cutoff")

      val host = config.get("clickhouse", "host")
      val port = config.get("clickhouse", "port")
      val database = config.get("clickhouse", "database")
      val user = config.get("clickhouse", "username")
      val password = config.get("clickhouse", "password")
      val jdbcUrl = s"jdbc:clickhouse://$host:$port/$database"

      // Read deduped hot rows older than hot window / Đọc hot đã FINAL, cũ hơn cửa sổ
      val query =
        s"""SELECT
           |  ReportDate, OrderId, ReturnId, DocumentType, ShopCode, ShopName, OrderStatus,
           |  SalesAmount, ReturnAmount, NetRevenue, TotalReceipts, TotalExpenditures, NetCashFlow,
           |  PaymentMethodSummary, TotalOrderAmount, TotalReturnAmount, ReconciliationDiff,
           |  PendingOrder, UpdatedAt
           |FROM report.biz_rt_order FINAL
           |WHERE ReportDate < toDate('$cutoff')
           |""".stripMargin

      Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
      val hotDf = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("query", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()

      val count = hotDf.count()
      println(s"Rows to migrate: $count")
      if (count == 0) {
        println("Nothing to migrate — skip Iceberg write.")
      } else {
        val aligned = hotDf
          .withColumn("ReportDate", to_date(col("ReportDate")))
          .withColumn("ReturnId", coalesce(col("ReturnId"), lit("")))
          .withColumn("PendingOrder", col("PendingOrder").cast("byte"))
          .withColumn("UpdatedAt", col("UpdatedAt").cast("timestamp"))

        SparkIOUtils.ensureNamespaceExists(spark, "nessie.serving")
        SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")

        // Stage then merge into serving / Ghi stg rồi merge serving
        SparkIOUtils.writeToIceberg(spark, aligned, StgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, StgTable)

        if (!SparkIOUtils.tableExists(spark, ServingTable)) {
          println(s"Initializing $ServingTable")
          SparkIOUtils.writeToIceberg(spark, stgDf, ServingTable, "overwrite")
        } else {
          SparkIOUtils.mergeIcebergTable(spark, stgDf, ServingTable, MergeKeys)
        }

        Try(SparkIOUtils.expireTableSnapshots(spark, ServingTable))
        Try(SparkIOUtils.expireTableSnapshots(spark, StgTable))
        println(s"Migrated $count rows into $ServingTable")
      }
    }

    result match {
      case Success(_) =>
        println(s"✔ [$jobName] finished successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed:")
        e.printStackTrace()
        spark.stop()
        sys.exit(1)
    }
  }
}
