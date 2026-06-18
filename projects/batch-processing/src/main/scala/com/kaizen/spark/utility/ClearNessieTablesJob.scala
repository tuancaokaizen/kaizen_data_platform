package com.kaizen.spark.utility

import org.apache.spark.sql.SparkSession
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ClearNessieTablesJob {

  def main(args: Array[String]): Unit = {
    val jobName = "ClearNessieTablesJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)

    val clearResult = Try {
      // List of all tables to be dropped in Nessie Catalog
      val tables = Seq(
        // STAGING TABLES
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

        // FOUNDATION TABLES
        "nessie.fnd.geography",
        "nessie.fnd.shop",
        "nessie.fnd.employee",
        "nessie.fnd.product",
        "nessie.fnd.product_medicine",
        "nessie.fnd.product_supplement",
        "nessie.fnd.customer",
        "nessie.fnd.payment_type",
        "nessie.fnd.payment_method",
        "nessie.fnd.order",
        "nessie.fnd.order_item",
        "nessie.fnd.return",
        "nessie.fnd.return_item",
        "nessie.fnd.payment",
        "nessie.fnd.transaction",
        "nessie.fnd.order_payment",
        "nessie.fnd.return_payment",
        "nessie.fnd.employee_salary",
        "nessie.fnd.promotion",
        "nessie.fnd.voucher",
        "nessie.fnd.voucher_series",
        "nessie.fnd.gift",

        // WAREHOUSE TABLES
        "nessie.dw.dim_product",
        "nessie.dw.dim_customer",
        "nessie.dw.dim_shop",
        "nessie.dw.dim_employee",
        "nessie.dw.dim_promotion",
        "nessie.dw.dim_voucher",
        "nessie.dw.fact_sales_transaction",
        "nessie.dw.fact_return_transaction",
        "nessie.dw.fact_payment_transaction",
        "nessie.dw.fact_promotion_cost",
        "nessie.dw.fact_cogs_transaction"
      )

      tables.foreach { table =>
        if (SparkIOUtils.tableExists(spark, table)) {
          println(s"Dropping table: $table...")
          try {
            spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
            println(s"✔ Successfully dropped table: $table (Purged from MinIO)")
          } catch {
            case e: Exception =>
              println(s"❌ Failed to drop table $table: ${e.getMessage}")
          }
        } else {
          println(s"Table $table does not exist. Skipping.")
        }
      }
    }

    clearResult match {
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
