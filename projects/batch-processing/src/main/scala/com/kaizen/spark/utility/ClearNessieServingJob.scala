package com.kaizen.spark.utility

import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Failure, Success, Try}

object ClearNessieServingJob {

  // Report tables in stg + serving layers / Bảng báo cáo tầng stg + serving
  private val ReportTables: Seq[String] = Seq(
    "financial_report",
    "financial_report_detail",
    "financial_report_product",
    "financial_report_order",
    "fund_report",
    "fund_report_shop",
    "fund_report_order",
    // Legacy grains removed from schema / Grain cũ đã bỏ
    "fund_report_detail",
    "fund_report_product"
  )

  // Drop with PURGE when allowed; otherwise drop catalog entry only
  // Xóa PURGE khi được phép; nếu không chỉ xóa metadata catalog
  private def dropTable(spark: org.apache.spark.sql.SparkSession, table: String): Unit = {
    if (!SparkIOUtils.tableExists(spark, table)) {
      println(s"Table $table does not exist. Skipping.")
      return
    }

    println(s"Dropping table: $table...")
    try {
      spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
      println(s"Successfully dropped table: $table (purged from MinIO)")
    } catch {
      case _: Exception =>
        println(s"PURGE failed for $table, dropping catalog entry only...")
        spark.sql(s"DROP TABLE IF EXISTS $table")
        println(s"Dropped catalog entry: $table")
    }
  }

  def main(args: Array[String]): Unit = {
    val jobName = "ClearNessieServingJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)

    val clearResult = Try {
      val tables = for {
        layer <- Seq("stg", "serving")
        table <- ReportTables
      } yield s"nessie.$layer.$table"

      tables.foreach { table =>
        dropTable(spark, table)
      }
    }

    clearResult match {
      case Success(_) =>
        println(s"[$jobName] completed successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"[$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }
}
