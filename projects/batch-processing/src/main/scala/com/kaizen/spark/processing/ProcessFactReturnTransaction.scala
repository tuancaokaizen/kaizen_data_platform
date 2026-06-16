package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessFactReturnTransaction {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessFactReturnTransaction"
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

      val targetTable = "nessie.dw.fact_return_transaction"
      val stgTable = "nessie.stg.fact_return_transaction"

      println(s"Running Return Fact Transformation for range: $fromTs to $toTs")

      // Load foundation data
      val returnFnd = spark.table("nessie.fnd.return")
        .filter(
          (col("CreatedDate") >= fromTs && col("CreatedDate") < toTs) ||
          (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )

      if (returnFnd.isEmpty) {
        println("No returns found in the specified range. Skipping returns transformation.")
      } else {
        val returnItemFnd = spark.table("nessie.fnd.return_item")
        val filteredReturnItems = returnItemFnd.filter(col("UnitPrice") > 0)
        val orderFnd = spark.table("nessie.fnd.order")

        // Transform and map to Fact schema
        val factReturnDf = filteredReturnItems
          .join(returnFnd, filteredReturnItems("ReturnId") === returnFnd("Id"), "inner")
          .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
          .select(
            filteredReturnItems("Id").as("TransactionId"),
            returnFnd("OrderId").as("OrderId"),
            filteredReturnItems("ReturnId").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            orderFnd("EmployeeSaleId").as("EmployeeSaleId"),
            filteredReturnItems("ProductId").as("ProductId"),
            lit(null).cast("string").as("VoucherSeriesCode"),
            filteredReturnItems("Quantity").as("Quantity"),
            filteredReturnItems("UnitPrice").as("UnitPrice"),
            filteredReturnItems("TotalAmount").as("TotalAmount"),
            returnFnd("CreatedDate").as("TransactionDate"),
            filteredReturnItems("CreatedDate").as("CreatedDate"),
            filteredReturnItems("ModifiedDate").as("ModifiedDate")
          )

        // Write to Iceberg
        val targetExists = SparkIOUtils.tableExists(spark, targetTable)
        if (!targetExists) {
          println(s"Target table '$targetTable' does not exist. Initializing table...")
          SparkIOUtils.writeToIceberg(spark, factReturnDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
        } else {
          println(s"Merging return items into target table '$targetTable'...")
          SparkIOUtils.writeToIceberg(spark, factReturnDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("TransactionId"))
        }
        println(s"✔ Return Fact Transformation completed for: $targetTable")
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
