package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessSrvFinancialReport {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessSrvFinancialReport"
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
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.serving")

      val targetTable = "nessie.serving.financial_report"
      val stgTable = "nessie.stg.financial_report"

      println(
        s"Generating Serving Financial Report for range: $fromTs to $toTs"
      )

      // Load DW facts and filter by date range
      val salesFact = spark
        .table("nessie.dw.fact_sales_transaction")
        .filter(
          col("TransactionDate") >= fromTs && col("TransactionDate") < toTs
        )

      val returnFact = spark
        .table("nessie.dw.fact_return_transaction")
        .filter(
          col("TransactionDate") >= fromTs && col("TransactionDate") < toTs
        )

      val cogsFact = spark
        .table("nessie.dw.fact_cogs_transaction")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)

      val promoFact = spark
        .table("nessie.dw.fact_promotion_cost")
        .filter(col("CreatedDate") >= fromTs && col("CreatedDate") < toTs)

      val dimShop = spark.table("nessie.dw.dim_shop")

      // Aggregate Sales by Date and Shop
      val salesAgg = salesFact
        .groupBy(
          to_date(col("TransactionDate")).as("ReportDate"),
          col("ShopCode")
        )
        .agg(
          sum(col("Quantity")).as("SalesQuantity"),
          sum(col("TotalAmount")).as("GrossRevenue")
        )

      // Aggregate Returns by Date and Shop
      val returnAgg = returnFact
        .groupBy(
          to_date(col("TransactionDate")).as("ReportDate"),
          col("ShopCode")
        )
        .agg(
          sum(col("Quantity")).as("ReturnQuantity"),
          sum(col("TotalAmount")).as("ReturnAmount")
        )

      // Aggregate COGS by Date and Shop
      val cogsAgg = cogsFact
        .groupBy(to_date(col("CreatedDate")).as("ReportDate"), col("ShopCode"))
        .agg(
          sum(col("CostAmount")).as("TotalCogs")
        )

      // Aggregate Promotion Costs by Date and Shop
      val promoAgg = promoFact
        .groupBy(to_date(col("CreatedDate")).as("ReportDate"), col("ShopCode"))
        .agg(
          sum(col("CostAmount")).as("TotalPromotionCost")
        )

      // Get list of all distinct (ReportDate, ShopCode) combinations from all facts
      val allKeys = salesAgg
        .select("ReportDate", "ShopCode")
        .union(returnAgg.select("ReportDate", "ShopCode"))
        .union(cogsAgg.select("ReportDate", "ShopCode"))
        .union(promoAgg.select("ReportDate", "ShopCode"))
        .distinct()

      // Join all aggregations together
      val reportDf = allKeys
        .join(salesAgg, Seq("ReportDate", "ShopCode"), "left")
        .join(returnAgg, Seq("ReportDate", "ShopCode"), "left")
        .join(cogsAgg, Seq("ReportDate", "ShopCode"), "left")
        .join(promoAgg, Seq("ReportDate", "ShopCode"), "left")
        .join(dimShop, Seq("ShopCode"), "left")
        .select(
          allKeys("ReportDate"),
          allKeys("ShopCode"),
          coalesce(dimShop("ShopName"), lit("Unknown")).as("ShopName"),
          coalesce(col("SalesQuantity"), lit(0L)).as("SalesQuantity"),
          coalesce(col("ReturnQuantity"), lit(0L)).as("ReturnQuantity"),
          coalesce(col("GrossRevenue"), lit(0.0)).as("GrossRevenue"),
          coalesce(col("ReturnAmount"), lit(0.0)).as("ReturnAmount"),
          (coalesce(col("GrossRevenue"), lit(0.0)) - coalesce(
            col("ReturnAmount"),
            lit(0.0)
          )).as("NetRevenue"),
          coalesce(col("TotalCogs"), lit(0.0)).as("TotalCogs"),
          coalesce(col("TotalPromotionCost"), lit(0.0))
            .as("TotalPromotionCost"),
          (coalesce(col("TotalCogs"), lit(0.0)) + coalesce(
            col("TotalPromotionCost"),
            lit(0.0)
          )).as("TotalCost"),
          ((coalesce(col("GrossRevenue"), lit(0.0)) - coalesce(
            col("ReturnAmount"),
            lit(0.0)
          )) -
            (coalesce(col("TotalCogs"), lit(0.0)) + coalesce(
              col("TotalPromotionCost"),
              lit(0.0)
            ))).as("GrossProfit")
        )
        // Add Profit Margin (prevent division by zero)
        .withColumn(
          "ProfitMargin",
          when(
            col("NetRevenue") > 0,
            round(col("GrossProfit") / col("NetRevenue"), 4)
          )
            .otherwise(lit(0.0))
        )

      // Write to Iceberg Serving
      val targetExists = SparkIOUtils.tableExists(spark, targetTable)
      if (!targetExists) {
        println(
          s"Target table '$targetTable' does not exist. Initializing table..."
        )
        SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
      } else {
        println(s"Merging report records into target table '$targetTable'...")
        SparkIOUtils.writeToIceberg(spark, reportDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.mergeIcebergTable(
          spark,
          stgDf,
          targetTable,
          Seq("ReportDate", "ShopCode")
        )
      }
      println(
        s"✔ Serving Financial Report generation completed for: $targetTable"
      )
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.stg.financial_report"
          )
          SparkIOUtils.expireTableSnapshots(
            spark,
            "nessie.serving.financial_report"
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
