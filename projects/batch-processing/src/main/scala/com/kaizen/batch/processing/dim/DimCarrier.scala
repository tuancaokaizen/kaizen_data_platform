package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, count, lit}

/** nessie.dw.dim_carrier: transport carrier with its contract prices and fleet size, rebuilt every run.
  * nessie.dw.dim_carrier: nhà vận chuyển kèm đơn giá hợp đồng và số xe, dựng lại mỗi lần chạy.
  */
object DimCarrier {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimCarrier", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val carrier = spark.table("nessie.fnd.carrier")
    val fleet = spark.table("nessie.fnd.vehicle")
      .groupBy(col("CarrierId").as("Id"))
      .agg(count(lit(1)).cast("int").as("VehicleCount"))

    val dimCarrier = carrier
      .join(broadcast(fleet), Seq("Id"), "left")
      .select(
        col("Id").as("CarrierId"),
        col("Name").as("CarrierName"),
        col("Type").as("CarrierType"),
        col("FixedCostPerTrip").cast("long"),
        col("CostPerKm").cast("long"),
        col("CostPerKg").cast("long"),
        col("CostPerStop").cast("long"),
        col("SupportsColdChain").cast("boolean"),
        col("WeeklyCapacityKg").cast("int"),
        col("ContractMinKgPerWeek").cast("int"),
        coalesce(col("VehicleCount"), lit(0)).as("VehicleCount")
      )

    SparkUtils.overwriteTable(spark, dimCarrier, "nessie.dw.dim_carrier")
  }
}
