package com.kaizen.batch.processing.report.fund

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.serving.fund_report: cash per day, shop and payment method, rolled up from fund_report_order.
  * nessie.serving.fund_report: tiền theo ngày, cửa hàng và phương thức thanh toán, cộng dồn từ fund_report_order.
  */
object FundReport {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FundReport", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val fundReportOrder = spark.table("nessie.serving.fund_report_order")
      .where(DateUtils.inDateRange(col("ReportDate"), fromDate, toDate))

    val fundReport = FundMeasures.rollUp(
      fundReportOrder,
      dimensionColumns = Seq("ReportDate", "ShopCode", "ShopName", "PaymentMethodName"),
      keys = Seq("ReportDate", "ShopCode", "PaymentMethodName")
    )

    SparkUtils.replaceDates(
      spark,
      fundReport,
      "nessie.serving.fund_report",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
