package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessFactSalesTransaction {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessFactSalesTransaction"
    println(s"=== Starting $jobName ===")
    fromDate.foreach(d => println(s"From Date: $d"))
    toDate.foreach(d => println(s"To Date: $d"))

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val fromTs =
      SparkIOUtils.formatIcebergDateBoundary(fromDate.getOrElse("1970-01-01"))
    val toTs = SparkIOUtils.formatIcebergDateBoundary(
      toDate.getOrElse(java.time.LocalDate.now().plusDays(1).toString)
    )

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.fact_sales_transaction"
      val stgTable = "nessie.stg.fact_sales_transaction"

      println(s"Running Sales Fact Transformation for range: $fromTs to $toTs")

      // Load foundation data
      val orderFnd = spark
        .table("nessie.fnd.order")
        .filter(
          (col("TransactionDate") >= fromTs && col("TransactionDate") < toTs) ||
            (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )
        .dropDuplicates("Id")

      if (orderFnd.isEmpty) {
        println(
          "No sales orders found in the specified range. Skipping sales transformation."
        )
      } else {
        val orderItemFnd = spark.table("nessie.fnd.order_item")
        val filteredOrderItems = orderItemFnd.filter(col("UnitPrice") > 0)

        val discountRatio = coalesce(
          when(
            orderFnd("TotalBill") > 0,
            orderFnd("TotalDiscount").cast("double") / orderFnd("TotalBill")
              .cast("double")
          ),
          lit(0.0)
        )

        // Transform and map to Fact schema
        val factSalesDf = filteredOrderItems
          .join(
            orderFnd,
            filteredOrderItems("OrderId") === orderFnd("Id"),
            "inner"
          )
          .select(
            filteredOrderItems("Id").as("TransactionId"),
            filteredOrderItems("OrderId").as("OrderId"),
            lit(null).cast("string").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            orderFnd("EmployeeSaleId").as("EmployeeSaleId"),
            filteredOrderItems("ProductId").as("ProductId"),
            orderFnd("VoucherSeriesCode").as("VoucherSeriesCode"),
            filteredOrderItems("Quantity").as("Quantity"),
            filteredOrderItems("UnitPrice").as("UnitPrice"),
            round(
              (filteredOrderItems("Quantity") * filteredOrderItems("UnitPrice"))
                .cast("double") * (lit(1.0) - discountRatio)
            )
              .cast("integer")
              .as("TotalAmount"),
            orderFnd("TransactionDate").as("TransactionDate"),
            filteredOrderItems("CreatedDate").as("CreatedDate"),
            filteredOrderItems("ModifiedDate").as("ModifiedDate")
          )
          .dropDuplicates("TransactionId")

        // Write to Iceberg
        val targetExists = SparkIOUtils.tableExists(spark, targetTable)
        if (!targetExists) {
          println(
            s"Target table '$targetTable' does not exist. Initializing table..."
          )
          SparkIOUtils.writeToIceberg(spark, factSalesDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
        } else {
          println(s"Merging sales items into target table '$targetTable'...")
          SparkIOUtils.writeToIceberg(spark, factSalesDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.mergeIcebergTable(
            spark,
            stgDf,
            targetTable,
            Seq("TransactionId")
          )
        }
        println(s"✔ Sales Fact Transformation completed for: $targetTable")
      }
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.stg.fact_sales_transaction"
          )
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.dw.fact_sales_transaction"
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

}
