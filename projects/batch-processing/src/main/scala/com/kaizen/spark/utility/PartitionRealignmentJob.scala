package com.kaizen.spark.utility

import org.apache.spark.sql.SparkSession
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object PartitionRealignmentJob {

  def main(args: Array[String]): Unit = {
    val jobName = "PartitionRealignmentJob"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)

    val realignmentResult = Try {
      // List of target tables and their corresponding DDL partition adjustments
      val realignments = Seq(
        ("nessie.dw.fact_sales_transaction", "days(TransactionDate)"),
        ("nessie.dw.fact_return_transaction", "days(TransactionDate)"),
        ("nessie.dw.fact_payment_transaction", "days(CreatedDate)"),
        ("nessie.fnd.order", "days(TransactionDate)"),
        ("nessie.fnd.order_item", "days(CreatedDate)"),
        ("nessie.fnd.return", "days(CreatedDate)"),
        ("nessie.fnd.return_item", "days(CreatedDate)"),
        ("nessie.fnd.payment", "days(CreatedDate)"),
        ("nessie.fnd.transaction", "days(CreatedDate)"),
        ("nessie.fnd.order_payment", "days(CreatedDate)"),
        ("nessie.fnd.return_payment", "days(CreatedDate)")
      )

      realignments.foreach { case (table, partitionExpr) =>
        if (SparkIOUtils.tableExists(spark, table)) {
          println(s"--------------------------------------------------")
          println(s"Realignment started for table: $table")

          // 1. Alter table to evolve partitioning schema (non-blocking metadata change)
          try {
            println(s"Adding partition field '$partitionExpr' to $table...")
            spark.sql(s"ALTER TABLE $table ADD PARTITION FIELD $partitionExpr")
            println(s"Successfully altered partition schema for $table.")
          } catch {
            case e: Exception =>
              println(
                s"Partition field might already exist or table alter failed: ${e.getMessage}"
              )
          }

          // 2. Run Iceberg's rewrite_data_files procedure to rewrite historical data into partitions
          println(
            s"Invoking Iceberg rewrite_data_files procedure for $table to reorganize historical data..."
          )
          spark.sql(
            s"""
               |CALL nessie.system.rewrite_data_files(
               |  table => '$table',
               |  options => map('min-input-files', '1')
               |)
               |""".stripMargin
          )
          println(s"✔ Completed realignment for table: $table")
        } else {
          println(s"Table $table does not exist. Skipping.")
        }
      }
    }

    realignmentResult match {
      case Success(_) =>
        println(s"✔ [$jobName] finished successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }
}
