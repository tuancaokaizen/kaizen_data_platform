package com.kaizen.spark.push

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object PushServingToClickHouseJob {

  def main(args: Array[String]): Unit = {
    val jobName = "PushServingToClickHouseJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      // Get the date 30 days ago in Asia/Ho_Chi_Minh timezone
      val today = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Ho_Chi_Minh"))
      val oneMonthAgo = today.minusDays(30).toString
      println(s"Filtering serving reports from: $oneMonthAgo to today ($today)")

      val clickhouseHost = config.get("clickhouse", "host")
      val clickhousePort = config.get("clickhouse", "port")
      val clickhouseDb = config.get("clickhouse", "database")
      val jdbcUrl = s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDb"
      val user = config.get("clickhouse", "username")
      val password = config.get("clickhouse", "password")

      def truncateTable(tableName: String): Unit = {
        println(s"Truncating ClickHouse table: $tableName")
        val conn = java.sql.DriverManager.getConnection(jdbcUrl, user, password)
        try {
          val stmt = conn.createStatement()
          stmt.execute(s"TRUNCATE TABLE $tableName")
          stmt.close()
        } finally {
          conn.close()
        }
      }

      // 1. Push financial_report
      val finTable = "nessie.serving.financial_report"
      if (SparkIOUtils.tableExists(spark, finTable)) {
        println(s"Reading from $finTable...")
        val finDf = SparkIOUtils.readFromIceberg(spark, finTable)
          .filter(col("ReportDate") >= to_date(lit(oneMonthAgo)))
        
        truncateTable("report.financial_report")
        println(s"Writing 1-month financial_report to ClickHouse table report.financial_report...")
        SparkIOUtils.writeToClickHouse(finDf, "report.financial_report", config)
      } else {
        println(s"WARNING: Serving table $finTable does not exist. Skipping.")
      }

      // 2. Push fund_report
      val fundTable = "nessie.serving.fund_report"
      if (SparkIOUtils.tableExists(spark, fundTable)) {
        println(s"Reading from $fundTable...")
        val fundDf = SparkIOUtils.readFromIceberg(spark, fundTable)
          .filter(col("ReportDate") >= to_date(lit(oneMonthAgo)))
        
        truncateTable("report.fund_report")
        println(s"Writing 1-month fund_report to ClickHouse table report.fund_report...")
        SparkIOUtils.writeToClickHouse(fundDf, "report.fund_report", config)
      } else {
        println(s"WARNING: Serving table $fundTable does not exist. Skipping.")
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
