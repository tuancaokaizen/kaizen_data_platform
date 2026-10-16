package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}

/** nessie.dw.dim_voucher: one row per voucher series (the code printed on an order) with its voucher and promotion.
  * nessie.dw.dim_voucher: mỗi dòng một mã voucher phát hành (mã ghi trên đơn) kèm voucher và chương trình khuyến mãi.
  */
object DimVoucher {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimVoucher", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val voucherSeries = spark.table("nessie.fnd.voucher_series")
    val voucher = spark.table("nessie.fnd.voucher")
      .select(
        col("Code").as("VoucherCode"),
        col("Name").as("VoucherName"),
        col("PromotionCode"),
        col("VoucherAmount"),
        col("MinOrderValue").as("MinimumOrderValue"),
        col("StartDate"),
        col("EndDate")
      )
    val promotion = spark.table("nessie.fnd.promotion").select(col("Code").as("PromotionCode"), col("Name").as("PromotionName"))

    val dimVoucher = voucherSeries
      .join(voucher, Seq("VoucherCode"), "left")
      .join(broadcast(promotion), Seq("PromotionCode"), "left")
      .select(
        col("SeriesCode").as("VoucherSeriesCode"),
        col("VoucherCode"),
        coalesce(col("VoucherName"), lit("Unknown")).as("VoucherName"),
        col("PromotionCode"),
        coalesce(col("PromotionName"), lit("Unknown")).as("PromotionName"),
        col("VoucherAmount").cast("long"),
        col("MinimumOrderValue").cast("long"),
        col("StartDate").cast("timestamp"),
        col("EndDate").cast("timestamp"),
        col("ExpiryDay").cast("int").as("ExpiryDays"),
        col("Status").as("SeriesStatus")
      )

    SparkUtils.overwriteTable(spark, dimVoucher, "nessie.dw.dim_voucher")
  }
}
