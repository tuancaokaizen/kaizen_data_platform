package com.kaizen.batch.processing.report.supply

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, count, lit, max, sum, when}

/** nessie.serving.inventory_health: stock health per shop, product line and day.
  * On-hand, DOS, stockout rate, fill rate from lost sales, inventory value and expired quantity.
  * nessie.serving.inventory_health: sức khoẻ tồn theo cửa hàng, ngành hàng và ngày.
  * Tồn, DOS, tỷ lệ hết hàng, fill rate từ mất bán, giá trị tồn và số hết hạn.
  */
object InventoryHealth {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("InventoryHealth", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val product = spark.table("nessie.dw.dim_product")
      .select(col("ProductId"), col("ProductLineCode"), col("ProductLineName"))
    val shop = spark.table("nessie.dw.dim_shop").select(col("ShopCode"), col("ShopName"))

    val snapshot = spark.table("nessie.dw.fact_inventory_snapshot")
      .where(col("LocationType") === "SHOP" && DateUtils.inDateRange(col("SnapshotDate"), fromDate, toDate))
      .withColumnRenamed("LocationId", "ShopCode")
      .join(product, Seq("ProductId"), "left")
      .groupBy("SnapshotDate", "ShopCode", "ProductLineCode")
      .agg(
        max(col("ProductLineName")).as("ProductLineName"),
        sum(col("OnHandQuantity")).cast("long").as("OnHandQuantity"),
        sum(col("OnHandValue")).cast("long").as("OnHandValue"),
        sum(col("OnHandVolumeM3")).cast("decimal(14,4)").as("OnHandVolumeM3"),
        sum(col("ExpiredQuantity")).cast("long").as("ExpiredQuantity"),
        count(lit(1)).cast("long").as("SkuCount"),
        sum(when(col("IsStockout") === true, 1).otherwise(0)).cast("long").as("StockoutSkuCount"),
        avg(col("DaysOfSupply")).cast("double").as("AverageDaysOfSupply")
      )

    val lost = spark.table("nessie.dw.fact_lost_sale")
      .where(DateUtils.inDateRange(col("BusinessDate"), fromDate, toDate))
      .join(product, Seq("ProductId"), "left")
      .groupBy(col("BusinessDate").as("SnapshotDate"), col("ShopCode"), col("ProductLineCode"))
      .agg(
        sum(col("RequestedQuantity")).cast("long").as("RequestedQuantity"),
        sum(col("LostQuantity")).cast("long").as("LostQuantity"),
        sum(col("LostRevenueEstimate")).cast("long").as("LostRevenueEstimate")
      )

    val inventoryHealth = snapshot
      .join(lost, Seq("SnapshotDate", "ShopCode", "ProductLineCode"), "left")
      .join(shop, Seq("ShopCode"), "left")
      .select(
        col("SnapshotDate").as("ReportDate"),
        col("ShopCode"),
        coalesce(col("ShopName"), lit("Unknown")).as("ShopName"),
        coalesce(col("ProductLineCode"), lit("Unknown")).as("ProductLineCode"),
        coalesce(col("ProductLineName"), lit("Unknown")).as("ProductLineName"),
        col("OnHandQuantity"),
        col("OnHandValue"),
        col("OnHandVolumeM3"),
        col("ExpiredQuantity"),
        col("SkuCount"),
        col("StockoutSkuCount"),
        when(col("SkuCount") > 0, col("StockoutSkuCount") / col("SkuCount")).otherwise(lit(0.0)).cast("double").as("StockoutRate"),
        col("AverageDaysOfSupply"),
        coalesce(col("RequestedQuantity"), lit(0L)).as("RequestedQuantity"),
        coalesce(col("LostQuantity"), lit(0L)).as("LostQuantity"),
        coalesce(col("LostRevenueEstimate"), lit(0L)).as("LostRevenueEstimate"),
        when(coalesce(col("RequestedQuantity"), lit(0L)) > 0,
          (coalesce(col("RequestedQuantity"), lit(0L)) - coalesce(col("LostQuantity"), lit(0L))) / col("RequestedQuantity")
        ).otherwise(lit(1.0)).cast("double").as("FillRate")
      )

    SparkUtils.replaceDates(
      spark,
      inventoryHealth,
      "nessie.serving.inventory_health",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
