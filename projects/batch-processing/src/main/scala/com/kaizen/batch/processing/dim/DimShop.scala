package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, current_date, desc, lit, row_number}

/** nessie.dw.dim_shop: shop with its ward, province, manager name and the capacity in force today, rebuilt every run.
  * nessie.dw.dim_shop: cửa hàng kèm phường, tỉnh, tên quản lý và sức chứa đang hiệu lực hôm nay, dựng lại mỗi lần chạy.
  */
object DimShop {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimShop", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val shop = spark.table("nessie.fnd.shop")
    val ward = spark.table("nessie.fnd.geography")
      .select(col("Id").as("WardId"), col("Name").as("WardName"), col("ProvinceCode"))
    val manager = spark.table("nessie.fnd.employee")
      .select(col("Id").as("ManagerId"), col("Name").as("ManagerName"))

    // The capacity row in force today, the latest one when periods overlap
    // Dòng sức chứa đang hiệu lực hôm nay, lấy dòng mới nhất khi các kỳ chồng nhau
    val currentCapacity = spark.table("nessie.fnd.shop_capacity")
      .where(col("EffectiveFrom") <= current_date() && (col("EffectiveTo").isNull || col("EffectiveTo") >= current_date()))
      .withColumn("CapacityRank", row_number().over(Window.partitionBy("ShopCode").orderBy(desc("EffectiveFrom"), desc("Id"))))
      .where(col("CapacityRank") === 1)
      .select(
        col("ShopCode").as("Code"),
        col("StorageM3"),
        col("ColdStorageL"),
        col("ShelfSlots"),
        col("ReceivingSlotsPerDay"),
        col("StaffCount")
      )

    val dimShop = shop
      .join(broadcast(ward), Seq("WardId"), "left")
      .join(broadcast(manager), Seq("ManagerId"), "left")
      .join(broadcast(currentCapacity), Seq("Code"), "left")
      .select(
        col("Code").as("ShopCode"),
        col("Name").as("ShopName"),
        col("Address"),
        col("WardId"),
        coalesce(col("WardName"), lit("Unknown")).as("WardName"),
        col("ProvinceCode"),
        col("ManagerId"),
        coalesce(col("ManagerName"), lit("Unknown")).as("ManagerName"),
        col("Tier"),
        col("OpenDate").cast("date"),
        col("CloseDate").cast("date"),
        col("ServingWarehouseId"),
        col("Latitude").cast("decimal(9,6)"),
        col("Longitude").cast("decimal(9,6)"),
        col("StorageM3").cast("decimal(8,2)"),
        col("ColdStorageL").cast("int"),
        col("ShelfSlots").cast("int"),
        col("ReceivingSlotsPerDay").cast("int"),
        col("StaffCount").cast("int")
      )

    SparkUtils.overwriteTable(spark, dimShop, "nessie.dw.dim_shop")
  }
}
