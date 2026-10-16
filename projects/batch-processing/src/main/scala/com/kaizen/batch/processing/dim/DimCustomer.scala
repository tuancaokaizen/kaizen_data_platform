package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}

/** nessie.dw.dim_customer: customer without personal data (no name, phone, address), with ward and province.
  * nessie.dw.dim_customer: khách hàng không kèm dữ liệu cá nhân (không tên, điện thoại, địa chỉ), có phường và tỉnh.
  */
object DimCustomer {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimCustomer", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val customer = spark.table("nessie.fnd.customer")
    val ward = spark.table("nessie.fnd.geography")
      .select(col("Id").as("WardId"), col("Name").as("WardName"), col("ProvinceCode"))

    val dimCustomer = customer
      .join(broadcast(ward), Seq("WardId"), "left")
      .select(
        col("Id").as("CustomerId"),
        col("WardId"),
        coalesce(col("WardName"), lit("Unknown")).as("WardName"),
        col("ProvinceCode"),
        col("YearOfBirth").cast("int"),
        col("Gender")
      )

    SparkUtils.overwriteTable(spark, dimCustomer, "nessie.dw.dim_customer")
  }
}
