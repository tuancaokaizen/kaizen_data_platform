package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessFactPaymentTransaction {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessFactPaymentTransaction"
    println(s"=== Starting $jobName ===")
    fromDate.foreach(d => println(s"From Date: $d"))
    toDate.foreach(d => println(s"To Date: $d"))

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val fromTs = formatTimestamp(fromDate.getOrElse("1970-01-01"), isEndOfDay = false)
    val toTs = formatTimestamp(toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString), isEndOfDay = false)

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.fact_payment_transaction"
      val stgTable = "nessie.stg.fact_payment_transaction"

      println(s"Running Payment Fact Transformation for range: $fromTs to $toTs")

      // Load foundation data
      val transactionFnd = spark.table("nessie.fnd.transaction")
        .filter(
          (col("CreatedDate") >= fromTs && col("CreatedDate") < toTs) ||
          (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )

      if (transactionFnd.isEmpty) {
        println("No payment transactions found in the specified range. Skipping transformation.")
      } else {
        val paymentFnd = spark.table("nessie.fnd.payment")
          .filter(col("Status") === "SUCCESS")
        val orderPaymentFnd = spark.table("nessie.fnd.order_payment")
        val returnPaymentFnd = spark.table("nessie.fnd.return_payment")

        // Transform and map to Fact schema
        val factPaymentDf = transactionFnd
          .join(paymentFnd, transactionFnd("PaymentId") === paymentFnd("Id"), "inner")
          .join(orderPaymentFnd, transactionFnd("PaymentId") === orderPaymentFnd("PaymentId"), "left")
          .join(returnPaymentFnd, transactionFnd("PaymentId") === returnPaymentFnd("PaymentId"), "left")
          .select(
            transactionFnd("Id").as("TransactionId"),
            transactionFnd("PaymentId").as("PaymentId"),
            transactionFnd("PaymentMethodId").as("PaymentMethodId"),
            paymentFnd("PaymentType").as("PaymentType"),
            paymentFnd("Status").as("PaymentStatus"),
            coalesce(orderPaymentFnd("OrderId"), returnPaymentFnd("OrderId")).as("OrderId"),
            returnPaymentFnd("ReturnId").as("ReturnId"),
            transactionFnd("Amount").as("Amount"),
            transactionFnd("CreatedDate").as("CreatedDate"),
            transactionFnd("ModifiedDate").as("ModifiedDate")
          )

        // Write to Iceberg
        val targetExists = SparkIOUtils.tableExists(spark, targetTable)
        if (!targetExists) {
          println(s"Target table '$targetTable' does not exist. Initializing table...")
          SparkIOUtils.writeToIceberg(spark, factPaymentDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
        } else {
          println(s"Merging payment transaction items into target table '$targetTable'...")
          SparkIOUtils.writeToIceberg(spark, factPaymentDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("TransactionId"))
        }
        println(s"✔ Payment Fact Transformation completed for: $targetTable")
      }
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

  private def formatTimestamp(dateStr: String, isEndOfDay: Boolean): String = {
    val trimmed = dateStr.trim
    if (trimmed.length == 10) {
      if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
    } else {
      trimmed
    }
  }
}
