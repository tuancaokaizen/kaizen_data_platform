package com.kaizen.spark.push

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object PushServingToClickHouseJob {

  private val FinancialAggregateTables = Seq(
    "financial_report",
    "financial_report_detail",
    "financial_report_product"
  )

  private val FinancialOrderTables = Seq(
    "financial_report_order"
  )

  private val FundAggregateTables = Seq(
    "fund_report",
    "fund_report_shop"
  )

  private val FundOrderTables = Seq(
    "fund_report_order"
  )

  def main(args: Array[String]): Unit = {
    val jobName = "PushServingToClickHouseJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      val today =
        java.time.LocalDate.now(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
      val oneMonthAgo = today.minusDays(30).toString
      val oneWeekAgo = today.minusDays(7).toString
      println(
        s"Hot window: aggregate=30 days ($oneMonthAgo), order grain=7 days ($oneWeekAgo)"
      )

      val clickhouseHost = config.get("clickhouse", "host")
      val clickhousePort = config.get("clickhouse", "port")
      val clickhouseDb = config.get("clickhouse", "database")
      val jdbcUrl =
        s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDb"
      val user = config.get("clickhouse", "username")
      val password = config.get("clickhouse", "password")

      def truncateTable(tableName: String): Unit = {
        println(s"Truncating ClickHouse table: $tableName")
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
        val conn = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
        try {
          val stmt = conn.createStatement()
          stmt.execute(s"TRUNCATE TABLE IF EXISTS $tableName")
          stmt.close()
        } finally {
          conn.close()
        }
      }

      def pushServingTable(
          icebergTable: String,
          clickhouseTable: String,
          fromDate: String
      ): Unit = {
        if (!SparkIOUtils.tableExists(spark, icebergTable)) {
          println(s"WARNING: Serving table $icebergTable does not exist. Skipping.")
          return
        }
        println(s"Reading from $icebergTable (ReportDate >= $fromDate)...")
        var df = SparkIOUtils
          .readFromIceberg(spark, icebergTable)
          .filter(col("ReportDate") >= to_date(lit(fromDate)))
        // Iceberg may carry merge-only columns not present in ClickHouse report DDL
        // Bỏ cột chỉ phục vụ MERGE trên Nessie, không có trong bảng report ClickHouse
        if (df.columns.contains("PaymentMethodId")) {
          df = df.drop("PaymentMethodId")
        }

        truncateTable(clickhouseTable)
        println(s"Writing to ClickHouse table $clickhouseTable...")
        SparkIOUtils.writeToClickHouse(df, clickhouseTable, config)
      }

      FinancialAggregateTables.foreach { table =>
        pushServingTable(
          s"nessie.serving.$table",
          s"report.$table",
          oneMonthAgo
        )
      }

      FinancialOrderTables.foreach { table =>
        pushServingTable(s"nessie.serving.$table", s"report.$table", oneWeekAgo)
      }

      FundAggregateTables.foreach { table =>
        pushServingTable(
          s"nessie.serving.$table",
          s"report.$table",
          oneMonthAgo
        )
      }

      FundOrderTables.foreach { table =>
        pushServingTable(s"nessie.serving.$table", s"report.$table", oneWeekAgo)
      }

      println(s"✔ Serving data successfully pushed to ClickHouse report layer.")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }
}
