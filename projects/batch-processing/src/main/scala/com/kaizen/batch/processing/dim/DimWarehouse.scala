package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, count, lit}

/** nessie.dw.dim_warehouse: distribution centre with its ward, capacity and number of shops it serves, rebuilt every run.
  * nessie.dw.dim_warehouse: kho phân phối kèm phường, sức chứa và số cửa hàng được phục vụ, dựng lại mỗi lần chạy.
  */
object DimWarehouse {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimWarehouse", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val warehouse = spark.table("nessie.fnd.warehouse")
    val ward = spark.table("nessie.fnd.geography")
      .select(col("Id").as("WardId"), col("Name").as("WardName"), col("ProvinceCode"))
    val servedShop = spark.table("nessie.fnd.shop")
      .where(col("ServingWarehouseId").isNotNull)
      .groupBy(col("ServingWarehouseId").as("Id"))
      .agg(count(lit(1)).cast("int").as("ServedShopCount"))

    val dimWarehouse = warehouse
      .join(broadcast(ward), Seq("WardId"), "left")
      .join(broadcast(servedShop), Seq("Id"), "left")
      .select(
        col("Id").as("WarehouseId"),
        col("Name").as("WarehouseName"),
        col("WardId"),
        coalesce(col("WardName"), lit("Unknown")).as("WardName"),
        col("ProvinceCode"),
        col("Latitude").cast("decimal(9,6)"),
        col("Longitude").cast("decimal(9,6)"),
        col("StorageM3").cast("decimal(10,2)"),
        col("ColdStorageL").cast("int"),
        col("DockSlotsPerDay").cast("int"),
        coalesce(col("ServedShopCount"), lit(0)).as("ServedShopCount")
      )

    SparkUtils.overwriteTable(spark, dimWarehouse, "nessie.dw.dim_warehouse")
  }
}
