package com.kaizen.spark.utility

import org.apache.spark.sql.SparkSession
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object IcebergMaintenanceJob {

  def main(args: Array[String]): Unit = {
    val jobName = "IcebergMaintenanceJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)

    val maintenanceResult = Try {
      // List of all tables to be compacted and cleaned up periodically
      val tables = Seq(
        // DW Facts
        "nessie.dw.fact_sales_transaction",
        "nessie.dw.fact_return_transaction",
        "nessie.dw.fact_payment_transaction",
        "nessie.dw.fact_promotion_cost",
        "nessie.dw.fact_cogs_transaction",
        // DW Dims
        "nessie.dw.dim_product",
        "nessie.dw.dim_customer",
        "nessie.dw.dim_shop",
        "nessie.dw.dim_employee",
        "nessie.dw.dim_promotion",
        "nessie.dw.dim_voucher",
        // Serving Reports / Báo cáo serving
        "nessie.serving.financial_report",
        "nessie.serving.financial_report_detail",
        "nessie.serving.financial_report_product",
        "nessie.serving.financial_report_order",
        "nessie.serving.fund_report",
        "nessie.serving.fund_report_shop",
        "nessie.serving.fund_report_order",
        // FND Incremental tables
        "nessie.fnd.order",
        "nessie.fnd.order_item",
        "nessie.fnd.return",
        "nessie.fnd.return_item",
        "nessie.fnd.payment",
        "nessie.fnd.transaction",
        "nessie.fnd.order_payment",
        "nessie.fnd.return_payment"
      )

      tables.foreach { table =>
        if (SparkIOUtils.tableExists(spark, table)) {
          println(s"==================================================")
          println(s"Starting Maintenance for table: $table")

          // 1. Compaction (rewrite_data_files) - merges small files into optimal-sized files (~512MB)
          try {
            println(s"Running Compaction (rewrite_data_files) for $table...")
            spark.sql(
              s"""
                 |CALL nessie.system.rewrite_data_files(
                 |  table => '$table',
                 |  options => map('target-file-size-bytes', '536870912') -- 512MB
                 |)
                 |""".stripMargin
            )
            println(s"✔ Compaction successful for $table.")
          } catch {
            case e: Exception =>
              println(s"❌ Compaction failed for $table: ${e.getMessage}")
          }

          // 2. Expire Snapshots (Retain last 5 snapshots, expire ones older than 7 days)
          try {
            println(s"Expiring old snapshots for $table...")
            val olderThanMs = System
              .currentTimeMillis() - (7 * 24 * 60 * 60 * 1000L) // 7 days ago
            spark.sql(
              s"""
                 |CALL nessie.system.expire_snapshots(
                 |  table => '$table',
                 |  older_than => TIMESTAMP_MILLIS($olderThanMs),
                 |  retain_last => 5
                 |)
                 |""".stripMargin
            )
            println(s"✔ Snapshot expiration successful for $table.")
          } catch {
            case e: Exception =>
              println(
                s"❌ Expiring snapshots failed for $table: ${e.getMessage}"
              )
          }

          // 3. Remove Orphan Files from storage
          try {
            println(s"Cleaning up orphan files for $table...")
            spark.sql(
              s"CALL nessie.system.remove_orphan_files(table => '$table')"
            )
            println(s"✔ Orphan files cleanup successful for $table.")
          } catch {
            case e: Exception =>
              println(
                s"❌ Orphan files cleanup failed for $table: ${e.getMessage}"
              )
          }
        } else {
          println(s"Table $table does not exist. Skipping maintenance.")
        }
      }

      // 4. Staging Tables Cleanup (No compaction needed, but expire snapshots & orphan files are critical)
      val stgTables = Seq(
        // Ingestion Staging Tables
        "nessie.stg.geography",
        "nessie.stg.shop",
        "nessie.stg.employee",
        "nessie.stg.product",
        "nessie.stg.product_medicine",
        "nessie.stg.product_supplement",
        "nessie.stg.customer",
        "nessie.stg.payment_type",
        "nessie.stg.payment_method",
        "nessie.stg.order",
        "nessie.stg.order_item",
        "nessie.stg.return",
        "nessie.stg.return_item",
        "nessie.stg.payment",
        "nessie.stg.transaction",
        "nessie.stg.order_payment",
        "nessie.stg.return_payment",
        "nessie.stg.employee_salary",
        "nessie.stg.promotion",
        "nessie.stg.voucher",
        "nessie.stg.voucher_series",
        "nessie.stg.gift",
        // DW Dimensions & Facts Staging Tables
        "nessie.stg.dim_product",
        "nessie.stg.dim_customer",
        "nessie.stg.dim_shop",
        "nessie.stg.dim_employee",
        "nessie.stg.dim_promotion",
        "nessie.stg.dim_voucher",
        "nessie.stg.fact_sales_transaction",
        "nessie.stg.fact_return_transaction",
        "nessie.stg.fact_payment_transaction",
        "nessie.stg.fact_promotion_cost",
        "nessie.stg.fact_cogs_transaction",
        // Serving Reports Staging Tables
        "nessie.stg.financial_report",
        "nessie.stg.fund_report"
      )

      stgTables.foreach { table =>
        if (SparkIOUtils.tableExists(spark, table)) {
          println(s"==================================================")
          println(s"Starting Staging Maintenance for table: $table")

          // Expire old snapshots (Retain only the last 1 snapshot older than 1 day for staging)
          try {
            println(s"Expiring old snapshots for $table...")
            val olderThanMs =
              System.currentTimeMillis() - (24 * 60 * 60 * 1000L) // 1 day ago
            spark.sql(
              s"""
                 |CALL nessie.system.expire_snapshots(
                 |  table => '$table',
                 |  older_than => TIMESTAMP_MILLIS($olderThanMs),
                 |  retain_last => 1
                 |)
                 |""".stripMargin
            )
            println(s"✔ Snapshot expiration successful for staging: $table.")
          } catch {
            case e: Exception =>
              println(
                s"❌ Expiring snapshots failed for $table: ${e.getMessage}"
              )
          }

          // Remove Orphan Files from storage
          try {
            println(s"Cleaning up orphan files for $table...")
            spark.sql(
              s"CALL nessie.system.remove_orphan_files(table => '$table')"
            )
            println(s"✔ Orphan files cleanup successful for staging: $table.")
          } catch {
            case e: Exception =>
              println(
                s"❌ Orphan files cleanup failed for $table: ${e.getMessage}"
              )
          }
        } else {
          println(s"Staging table $table does not exist. Skipping.")
        }
      }
    }

    maintenanceResult match {
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
