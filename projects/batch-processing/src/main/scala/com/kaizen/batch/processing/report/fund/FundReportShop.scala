package com.kaizen.batch.processing.report.fund

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.serving.fund_report_shop: cash per day and shop, rolled up from fund_report.
  * nessie.serving.fund_report_shop: tiền theo ngày và cửa hàng, cộng dồn từ fund_report.
  */
object FundReportShop {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FundReportShop", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val fundReport = spark.table("nessie.serving.fund_report")
      .where(DateUtils.inDateRange(col("ReportDate"), fromDate, toDate))

    val fundReportShop = FundMeasures.rollUp(
      fundReport,
      dimensionColumns = Seq("ReportDate", "ShopCode", "ShopName"),
      keys = Seq("ReportDate", "ShopCode")
    )

    SparkUtils.replaceDates(
      spark,
      fundReportShop,
      "nessie.serving.fund_report_shop",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
