package com.kaizen.batch.processing.report.supply

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, countDistinct, lit, sum, when}

/** nessie.serving.shop_capacity_utilization: storage and receiving load per shop and day.
  * Volume % of StorageM3, receiving trips vs ReceivingSlotsPerDay.
  * nessie.serving.shop_capacity_utilization: tải kho và nhận hàng theo cửa hàng và ngày.
  * % thể tích so StorageM3, số lượt nhận so ReceivingSlotsPerDay.
  */
object ShopCapacityUtilization {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("ShopCapacityUtilization", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val shop = spark.table("nessie.dw.dim_shop").select(
      col("ShopCode"),
      col("ShopName"),
      col("StorageM3"),
      col("ColdStorageL"),
      col("ShelfSlots"),
      col("ReceivingSlotsPerDay")
    )

    val volume = spark.table("nessie.dw.fact_inventory_snapshot")
      .where(col("LocationType") === "SHOP" && DateUtils.inDateRange(col("SnapshotDate"), fromDate, toDate))
      .groupBy(col("SnapshotDate").as("ReportDate"), col("LocationId").as("ShopCode"))
      .agg(
        sum(col("OnHandVolumeM3")).cast("decimal(14,4)").as("OnHandVolumeM3"),
        sum(col("OnHandValue")).cast("long").as("OnHandValue"),
        sum(when(col("IsOverCapacity") === true, 1).otherwise(0)).cast("long").as("OverCapacitySkuCount")
      )

    // One receiving trip = one RefDocId of TRANSFER_IN or RECEIPT at the shop that day
    // Một lượt nhận = một RefDocId của TRANSFER_IN hoặc RECEIPT tại cửa hàng trong ngày
    val receiving = spark.table("nessie.dw.fact_stock_movement")
      .where(
        col("LocationType") === "SHOP" &&
          col("MovementType").isin("TRANSFER_IN", "RECEIPT") &&
          DateUtils.inDateRange(col("MovementDate"), fromDate, toDate)
      )
      .groupBy(col("MovementDate").as("ReportDate"), col("LocationId").as("ShopCode"))
      .agg(countDistinct(col("RefDocId")).cast("long").as("ReceivingCount"))

    val shopCapacityUtilization = volume
      .join(receiving, Seq("ReportDate", "ShopCode"), "left")
      .join(shop, Seq("ShopCode"), "left")
      .select(
        col("ReportDate"),
        col("ShopCode"),
        coalesce(col("ShopName"), lit("Unknown")).as("ShopName"),
        col("OnHandVolumeM3"),
        col("OnHandValue"),
        col("StorageM3").cast("decimal(8,2)"),
        col("ColdStorageL").cast("int"),
        col("ShelfSlots").cast("int"),
        col("ReceivingSlotsPerDay").cast("int"),
        coalesce(col("ReceivingCount"), lit(0L)).as("ReceivingCount"),
        col("OverCapacitySkuCount"),
        when(col("StorageM3") > 0, col("OnHandVolumeM3") / col("StorageM3")).otherwise(lit(0.0)).cast("double").as("StorageUtilization"),
        when(col("ReceivingSlotsPerDay") > 0, coalesce(col("ReceivingCount"), lit(0L)) / col("ReceivingSlotsPerDay"))
          .otherwise(lit(0.0)).cast("double").as("ReceivingUtilization")
      )

    SparkUtils.replaceDates(
      spark,
      shopCapacityUtilization,
      "nessie.serving.shop_capacity_utilization",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
