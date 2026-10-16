package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.dw.dim_geography: one row per ward with its province and sub-region, rebuilt every run.
  * nessie.dw.dim_geography: mỗi dòng một phường kèm tỉnh và tiểu vùng, dựng lại mỗi lần chạy.
  */
object DimGeography {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimGeography", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val dimGeography = spark.table("nessie.fnd.geography").select(
      col("Id").as("WardId"),
      col("Name").as("WardName"),
      col("AdminCode"),
      col("WardType"),
      col("ProvinceCode"),
      col("SubRegion"),
      col("Population").cast("int"),
      col("Latitude").cast("decimal(9,6)"),
      col("Longitude").cast("decimal(9,6)"),
      col("AreaKm2").cast("decimal(10,2)"),
      col("IsRoadAccessible").cast("boolean")
    )

    SparkUtils.overwriteTable(spark, dimGeography, "nessie.dw.dim_geography")
  }
}
