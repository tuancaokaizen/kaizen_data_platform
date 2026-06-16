package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessFactPromotionCost {

  def main(args: Array[String]): Unit = {
    val parsedArgs = SparkIOUtils.parseArgs(args)
    val fromDate = parsedArgs.get("fromDate").filter(_.trim.nonEmpty)
    val toDate = parsedArgs.get("toDate").filter(_.trim.nonEmpty)

    val jobName = "ProcessFactPromotionCost"
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

      val targetTable = "nessie.dw.fact_promotion_cost"
      val stgTable = "nessie.stg.fact_promotion_cost"

      println(s"Running Promotion Cost Fact Transformation for range: $fromTs to $toTs")

      // Load foundation data
      val orderFnd = spark.table("nessie.fnd.order")
        .filter(
          (col("TransactionDate") >= fromTs && col("TransactionDate") < toTs) ||
          (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )
        .dropDuplicates("Id")

      val returnFnd = spark.table("nessie.fnd.return")
        .filter(
          (col("CreatedDate") >= fromTs && col("CreatedDate") < toTs) ||
          (col("ModifiedDate") >= fromTs && col("ModifiedDate") < toTs)
        )
        .dropDuplicates("Id")

      if (orderFnd.isEmpty && returnFnd.isEmpty) {
        println("No sales orders or returns found in the specified range. Skipping promotion cost transformation.")
      } else {
        val orderItemFnd = spark.table("nessie.fnd.order_item")
        val returnItemFnd = spark.table("nessie.fnd.return_item")
        val productFnd = spark.table("nessie.fnd.product")
        val giftFnd = spark.table("nessie.fnd.gift")
        val promotionFnd = spark.table("nessie.fnd.promotion")
        val dimVoucher = spark.table("nessie.dw.dim_voucher")

        // Gift promotion mapping lookup helper
        val giftPromotions = giftFnd
          .join(promotionFnd, giftFnd("PromotionCode") === promotionFnd("Code"), "inner")
          .select(
            giftFnd("ProductId").as("GiftProductId"),
            giftFnd("PromotionCode").as("GiftPromotionCode"),
            promotionFnd("StartDate").as("PromStartDate"),
            promotionFnd("EndDate").as("PromEndDate")
          )

        // 1. Sales Vouchers
        val salesVouchersDf = orderFnd
          .filter(col("TotalDiscount") > 0 && col("VoucherSeriesCode").isNotNull)
          .join(dimVoucher, orderFnd("VoucherSeriesCode") === dimVoucher("VoucherSeriesCode"), "left")
          .select(
            concat(lit("PROM-VOUCHER-"), orderFnd("Id")).as("TransactionId"),
            orderFnd("Id").as("OrderId"),
            lit(null).cast("string").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            coalesce(dimVoucher("PromotionCode"), lit("Unknown")).as("PromotionCode"),
            orderFnd("VoucherSeriesCode").as("VoucherSeriesCode"),
            lit(null).cast("string").as("ProductId"),
            orderFnd("TotalDiscount").cast("double").as("CostAmount"),
            lit("VOUCHER").as("PromotionType"),
            orderFnd("TransactionDate").as("CreatedDate"),
            orderFnd("ModifiedDate").as("ModifiedDate")
          )

        // 2. Return Vouchers
        val returnVoucherRefunds = returnItemFnd
          .groupBy("ReturnId")
          .agg(
            sum(
              (coalesce(col("Quantity"), lit(0)) * coalesce(col("UnitPrice"), lit(0))) - coalesce(col("TotalAmount"), lit(0))
            ).as("VoucherRefund")
          )
          .filter(col("VoucherRefund") > 0)

        val returnVouchersDf = returnFnd
          .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
          .join(dimVoucher, orderFnd("VoucherSeriesCode") === dimVoucher("VoucherSeriesCode"), "left")
          .join(returnVoucherRefunds, returnFnd("Id") === returnVoucherRefunds("ReturnId"), "inner")
          .select(
            concat(lit("PROM-VOUCHER-"), returnFnd("Id")).as("TransactionId"),
            returnFnd("OrderId").as("OrderId"),
            returnFnd("Id").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            coalesce(dimVoucher("PromotionCode"), lit("Unknown")).as("PromotionCode"),
            orderFnd("VoucherSeriesCode").as("VoucherSeriesCode"),
            lit(null).cast("string").as("ProductId"),
            (- returnVoucherRefunds("VoucherRefund")).cast("double").as("CostAmount"),
            lit("VOUCHER").as("PromotionType"),
            returnFnd("CreatedDate").as("CreatedDate"),
            returnFnd("ModifiedDate").as("ModifiedDate")
          )

        // 3. Sales Gifts
        val salesGiftsDf = orderItemFnd
          .filter(col("UnitPrice") === 0)
          .join(orderFnd, orderItemFnd("OrderId") === orderFnd("Id"), "inner")
          .join(productFnd, orderItemFnd("ProductId") === productFnd("Id"), "inner")
          .join(giftPromotions,
            orderItemFnd("ProductId") === giftPromotions("GiftProductId") &&
            orderFnd("TransactionDate") >= giftPromotions("PromStartDate") &&
            orderFnd("TransactionDate") <= giftPromotions("PromEndDate"),
            "left"
          )
          .select(
            concat(lit("PROM-GIFT-"), orderItemFnd("Id")).as("TransactionId"),
            orderItemFnd("OrderId").as("OrderId"),
            lit(null).cast("string").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            coalesce(giftPromotions("GiftPromotionCode"), lit("Unknown")).as("PromotionCode"),
            lit(null).cast("string").as("VoucherSeriesCode"),
            orderItemFnd("ProductId").as("ProductId"),
            (orderItemFnd("Quantity") * productFnd("RetailPrice")).cast("double").as("CostAmount"),
            lit("GIFT").as("PromotionType"),
            orderFnd("TransactionDate").as("CreatedDate"),
            orderItemFnd("ModifiedDate").as("ModifiedDate")
          )

        // 4. Return Gifts
        val returnGiftsDf = returnItemFnd
          .filter(col("UnitPrice") === 0)
          .join(returnFnd, returnItemFnd("ReturnId") === returnFnd("Id"), "inner")
          .join(orderFnd, returnFnd("OrderId") === orderFnd("Id"), "inner")
          .join(productFnd, returnItemFnd("ProductId") === productFnd("Id"), "inner")
          .join(giftPromotions,
            returnItemFnd("ProductId") === giftPromotions("GiftProductId") &&
            returnFnd("CreatedDate") >= giftPromotions("PromStartDate") &&
            returnFnd("CreatedDate") <= giftPromotions("PromEndDate"),
            "left"
          )
          .select(
            concat(lit("PROM-GIFT-"), returnItemFnd("Id")).as("TransactionId"),
            returnFnd("OrderId").as("OrderId"),
            returnItemFnd("ReturnId").as("ReturnId"),
            orderFnd("CustomerId").as("CustomerId"),
            orderFnd("ShopCode").as("ShopCode"),
            coalesce(giftPromotions("GiftPromotionCode"), lit("Unknown")).as("PromotionCode"),
            lit(null).cast("string").as("VoucherSeriesCode"),
            returnItemFnd("ProductId").as("ProductId"),
            (- (returnItemFnd("Quantity") * productFnd("RetailPrice"))).cast("double").as("CostAmount"),
            lit("GIFT").as("PromotionType"),
            returnFnd("CreatedDate").as("CreatedDate"),
            returnItemFnd("ModifiedDate").as("ModifiedDate")
          )

        // Union all datasets
        val unionedDf = salesVouchersDf
          .union(returnVouchersDf)
          .union(salesGiftsDf)
          .union(returnGiftsDf)
          .dropDuplicates("TransactionId")

        // Write to Iceberg Nessie
        val targetExists = SparkIOUtils.tableExists(spark, targetTable)
        if (!targetExists) {
          println(s"Target table '$targetTable' does not exist. Initializing table...")
          SparkIOUtils.writeToIceberg(spark, unionedDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
        } else {
          println(s"Merging promotion costs into target table '$targetTable'...")
          SparkIOUtils.writeToIceberg(spark, unionedDf, stgTable, "overwrite")
          val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
          SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("TransactionId"))
        }
        println(s"✔ Promotion Cost Fact Transformation completed for: $targetTable")
      }
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, "nessie.stg.fact_promotion_cost")
          SparkIOUtils.expireTableSnapshots(spark, "nessie.dw.fact_promotion_cost")
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
