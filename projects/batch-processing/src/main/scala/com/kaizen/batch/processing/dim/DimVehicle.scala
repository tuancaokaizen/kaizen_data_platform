package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}

/** nessie.dw.dim_vehicle: vehicle with its carrier, capacity and home warehouse, rebuilt every run.
  * nessie.dw.dim_vehicle: xe kèm nhà vận chuyển, tải trọng và kho gốc, dựng lại mỗi lần chạy.
  */
object DimVehicle {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimVehicle", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val vehicle = spark.table("nessie.fnd.vehicle")
    val carrier = spark.table("nessie.fnd.carrier")
      .select(col("Id").as("CarrierId"), col("Name").as("CarrierName"))

    val dimVehicle = vehicle
      .join(broadcast(carrier), Seq("CarrierId"), "left")
      .select(
        col("Id").as("VehicleId"),
        col("CarrierId"),
        coalesce(col("CarrierName"), lit("Unknown")).as("CarrierName"),
        col("VehicleType"),
        col("CapacityKg").cast("int"),
        col("CapacityM3").cast("decimal(6,2)"),
        col("GrossWeightT").cast("decimal(4,1)"),
        col("IsCold").cast("boolean"),
        col("HomeWarehouseId"),
        col("Status")
      )

    SparkUtils.overwriteTable(spark, dimVehicle, "nessie.dw.dim_vehicle")
  }
}
