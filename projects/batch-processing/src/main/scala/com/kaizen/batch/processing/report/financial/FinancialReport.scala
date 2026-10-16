package com.kaizen.batch.processing.report.financial

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.serving.financial_report: profit per day and shop, rolled up from financial_report_detail.
  * nessie.serving.financial_report: lãi lỗ theo ngày và cửa hàng, cộng dồn từ financial_report_detail.
  */
object FinancialReport {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FinancialReport", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val financialReportDetail = spark.table("nessie.serving.financial_report_detail")
      .where(DateUtils.inDateRange(col("ReportDate"), fromDate, toDate))

    val financialReport = FinancialMeasures.rollUp(
      financialReportDetail,
      dimensionColumns = Seq("ReportDate", "ShopCode", "ShopName"),
      keys = Seq("ReportDate", "ShopCode")
    )

    SparkUtils.replaceDates(
      spark,
      financialReport,
      "nessie.serving.financial_report",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
