package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessFactCogs {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessFactCogs"
    println(s"=== Starting $jobName ===")
    fromDate.foreach(d => println(s"From Date: $d"))
    toDate.foreach(d => println(s"To Date: $d"))

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val fromTs =
      formatTimestamp(fromDate.getOrElse("1970-01-01"), isEndOfDay = false)
    val toTs = formatTimestamp(
      toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString),
      isEndOfDay = false
    )

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.fact_cogs_transaction"
      val stgTable = "nessie.stg.fact_cogs_transaction"

      println(s"Running COGS Fact Transformation for range: $fromTs to $toTs")

      // Load foundation data
      val orderFnd = spark
        .table("nessie.fnd.order")
        .filter(
          (col("TransactionDate") >= fromTs && col("TransactionDate") < toTs) ||
            (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )
        .dropDuplicates("Id")

      val returnFnd = spark
        .table("nessie.fnd.return")
        .filter(
          (col("CreatedDate") >= fromTs && col("CreatedDate") < toTs) ||
            (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )
        .dropDuplicates("Id")

      if (orderFnd.isEmpty && returnFnd.isEmpty) {
        println(
          "No sales orders or returns found in the specified range. Skipping COGS transformation."
        )
      } else {
        val orderItemFnd = spark.table("nessie.fnd.order_item")
        val returnItemFnd = spark.table("nessie.fnd.return_item")
        val productFnd = spark.table("nessie.fnd.product").dropDuplicates("Id")

        // 1. Sales COGS (exclude promotional products where UnitPrice = 0)
        val salesCogsDf = orderItemFnd
          .filter(col("UnitPrice") > 0)
          .join(orderFnd, orderItemFnd("OrderId") === orderFnd("Id"), "inner")
          .join(
            productFnd,
            orderItemFnd("ProductId") === productFnd("Id"),
            "inner"
          )
          .select(
            concat(lit("COGS-SALES-"), orderItemFnd("Id")).as("TransactionId"),
            orderItemFnd("OrderId").as("OrderId"),
            lit(null).cast("string").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            orderItemFnd("ProductId").as("ProductId"),
            orderItemFnd("Quantity").cast("integer").as("Quantity"),
            productFnd("StockPrice").cast("double").as("StockPrice"),
            (orderItemFnd("Quantity") * productFnd("StockPrice"))
              .cast("double")
              .as("CostAmount"),
            orderFnd("TransactionDate").as("CreatedDate"),
            orderItemFnd("ModifiedDate").as("ModifiedDate")
          )

        // 2. Return COGS (exclude promotional products where UnitPrice = 0)
        val returnCogsDf = returnItemFnd
          .filter(col("UnitPrice") > 0)
          .join(
            returnFnd,
            returnItemFnd("ReturnId") === returnFnd("Id"),
            "inner"
          )
          .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
          .join(
            productFnd,
            returnItemFnd("ProductId") === productFnd("Id"),
            "inner"
          )
          .select(
            concat(lit("COGS-RETURN-"), returnItemFnd("Id"))
              .as("TransactionId"),
            returnFnd("OrderId").as("OrderId"),
            returnItemFnd("ReturnId").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            returnItemFnd("ProductId").as("ProductId"),
            (-returnItemFnd("Quantity")).cast("integer").as("Quantity"),
            productFnd("StockPrice").cast("double").as("StockPrice"),
            (-(returnItemFnd("Quantity") * productFnd("StockPrice")))
              .cast("double")
              .as("CostAmount"),
            returnFnd("CreatedDate").as("CreatedDate"),
            returnItemFnd("ModifiedDate").as("ModifiedDate")
          )

        // Union datasets
        val unionedDf =
          salesCogsDf.union(returnCogsDf).dropDuplicates("TransactionId")

        // Write to Iceberg Nessie
        val targetExists = SparkIOUtils.tableExists(spark, targetTable)
        if (!targetExists) {
          println(
            s"Target table '$targetTable' does not exist. Initializing table..."
          )
          SparkIOUtils.writeToIceberg(spark, unionedDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
        } else {
          println(s"Merging COGS items into target table '$targetTable'...")
          SparkIOUtils.writeToIceberg(spark, unionedDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.mergeIcebergTable(
            spark,
            stgDf,
            targetTable,
            Seq("TransactionId")
          )
        }
        println(s"✔ COGS Fact Transformation completed for: $targetTable")
      }
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.stg.fact_cogs_transaction"
          )
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.dw.fact_cogs_transaction"
          )
        }
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
