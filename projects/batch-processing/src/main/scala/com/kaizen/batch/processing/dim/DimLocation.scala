package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, current_date, desc, lit, row_number}

/** nessie.dw.dim_location: every stock location, shops (LocationType SHOP) and distribution centres (LocationType DC),
  * keyed like the inventory tables by (LocationType, LocationId), with the storage capacity in force today.
  * nessie.dw.dim_location: mọi điểm giữ hàng, cửa hàng (LocationType SHOP) và kho phân phối (LocationType DC),
  * khoá giống bảng tồn kho theo (LocationType, LocationId), kèm sức chứa đang hiệu lực hôm nay.
  */
object DimLocation {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimLocation", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val ward = spark.table("nessie.fnd.geography").select(col("Id").as("WardId"), col("ProvinceCode"))

    // The capacity row in force today, the latest one when periods overlap
    // Dòng sức chứa đang hiệu lực hôm nay, lấy dòng mới nhất khi các kỳ chồng nhau
    val currentCapacity = spark.table("nessie.fnd.shop_capacity")
      .where(col("EffectiveFrom") <= current_date() && (col("EffectiveTo").isNull || col("EffectiveTo") >= current_date()))
      .withColumn("CapacityRank", row_number().over(Window.partitionBy("ShopCode").orderBy(desc("EffectiveFrom"), desc("Id"))))
      .where(col("CapacityRank") === 1)
      .select(col("ShopCode").as("Code"), col("StorageM3"), col("ColdStorageL"))

    val shopLocation = spark.table("nessie.fnd.shop")
      .join(broadcast(currentCapacity), Seq("Code"), "left")
      .select(
        lit("SHOP").as("LocationType"),
        col("Code").as("LocationId"),
        col("Name").as("LocationName"),
        col("WardId"),
        col("Latitude"),
        col("Longitude"),
        col("StorageM3"),
        col("ColdStorageL")
      )

    val warehouseLocation = spark.table("nessie.fnd.warehouse").select(
      lit("DC").as("LocationType"),
      col("Id").as("LocationId"),
      col("Name").as("LocationName"),
      col("WardId"),
      col("Latitude"),
      col("Longitude"),
      col("StorageM3"),
      col("ColdStorageL")
    )

    val dimLocation = shopLocation
      .unionByName(warehouseLocation)
      .join(broadcast(ward), Seq("WardId"), "left")
      .select(
        col("LocationType"),
        col("LocationId"),
        col("LocationName"),
        col("WardId"),
        col("ProvinceCode"),
        col("Latitude").cast("decimal(9,6)"),
        col("Longitude").cast("decimal(9,6)"),
        col("StorageM3").cast("decimal(10,2)"),
        col("ColdStorageL").cast("int")
      )

    SparkUtils.overwriteTable(spark, dimLocation, "nessie.dw.dim_location")
  }
}
