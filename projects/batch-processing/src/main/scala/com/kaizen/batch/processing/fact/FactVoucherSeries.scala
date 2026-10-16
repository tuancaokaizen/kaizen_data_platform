package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, broadcast, col, date_add, datediff, greatest, row_number, to_date, when}

/** nessie.dw.fact_voucher_series: one row per voucher code issued to a customer, from issue to use or expiry
  * (accumulating snapshot). Rebuilt for the codes issued or changed in [fromDate, toDate) and the codes used by the
  * orders sold or changed in that window. A code used by several orders keeps the first one.
  * nessie.dw.fact_voucher_series: mỗi dòng một mã voucher phát cho khách, từ lúc phát đến lúc dùng hoặc hết hạn
  * (accumulating snapshot). Dựng lại cho các mã phát hoặc thay đổi trong [fromDate, toDate) và các mã được dùng bởi
  * đơn bán hoặc thay đổi trong cửa sổ đó. Mã dùng cho nhiều đơn thì giữ đơn đầu tiên.
  */
object FactVoucherSeries {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactVoucherSeries", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val series = spark.table("nessie.fnd.voucher_series")
    val order = spark.table("nessie.fnd.order").where(col("VoucherSeriesCode").isNotNull)

    val changedSeries = series
      .where(DateUtils.inDateRange(to_date(col("CreatedDate")), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("SeriesCode").as("VoucherSeriesCode"))
      .union(
        order
          .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("VoucherSeriesCode"))
      )
      .distinct()

    // First order that used the code / Đơn đầu tiên dùng mã
    val usage = order
      .join(changedSeries, Seq("VoucherSeriesCode"))
      .withColumn("UseRank", row_number().over(Window.partitionBy("VoucherSeriesCode").orderBy(asc("TransactionDate"), asc("Id"))))
      .where(col("UseRank") === 1)
      .select(
        col("VoucherSeriesCode"),
        col("Id").as("OrderId"),
        col("OrderDate").as("UsedDate"),
        col("TransactionDate").as("UsedTime"),
        col("ShopCode"),
        col("CustomerId"),
        col("TotalBill").as("OrderAmount"),
        col("TotalDiscount").as("DiscountAmount"),
        col("ModifiedDate").as("OrderModifiedDate")
      )

    val voucher = spark.table("nessie.fnd.voucher")
      .select(col("Code").as("VoucherCode"), col("PromotionCode"), col("VoucherAmount"), col("MinOrderValue"))

    val issuedDate = to_date(col("CreatedDate"))
    val factVoucherSeries = series
      .withColumnRenamed("SeriesCode", "VoucherSeriesCode")
      .join(changedSeries, Seq("VoucherSeriesCode"))
      .join(broadcast(voucher), Seq("VoucherCode"), "left")
      .join(usage, Seq("VoucherSeriesCode"), "left")
      .select(
        col("VoucherSeriesCode"),
        col("VoucherCode"),
        col("PromotionCode"),
        col("VoucherAmount").cast("long"),
        col("MinOrderValue").cast("long"),
        issuedDate.as("IssuedDate"),
        col("CreatedDate").cast("timestamp").as("IssuedTime"),
        col("ExpiryDay").cast("int").as("ExpiryDays"),
        when(col("ExpiryDay").isNotNull, date_add(issuedDate, col("ExpiryDay").cast("int"))).as("ExpiryDate"),
        col("Status"),
        col("OrderId"),
        col("UsedDate").cast("date"),
        col("UsedTime").cast("timestamp"),
        col("ShopCode"),
        col("CustomerId"),
        col("OrderAmount").cast("long"),
        col("DiscountAmount").cast("long"),
        col("OrderId").isNotNull.as("IsUsed"),
        datediff(col("UsedDate"), issuedDate).as("DaysToUse"),
        greatest(col("ModifiedDate"), col("OrderModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factVoucherSeries,
      "nessie.dw.fact_voucher_series",
      keys = Seq("VoucherSeriesCode"),
      partition = Partition.Months("IssuedDate")
    )
  }
}
