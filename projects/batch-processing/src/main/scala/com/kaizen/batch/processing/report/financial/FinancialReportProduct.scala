package com.kaizen.batch.processing.report.financial

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.serving.financial_report_product: profit per day and product, rolled up from financial_report_detail.
  * nessie.serving.financial_report_product: lãi lỗ theo ngày và sản phẩm, cộng dồn từ financial_report_detail.
  */
object FinancialReportProduct {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FinancialReportProduct", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val financialReportDetail = spark.table("nessie.serving.financial_report_detail")
      .where(DateUtils.inDateRange(col("ReportDate"), fromDate, toDate))

    val financialReportProduct = FinancialMeasures.rollUp(
      financialReportDetail,
      dimensionColumns = Seq("ReportDate", "ProductId", "ProductName", "ProductLineCode", "ProductLineName"),
      keys = Seq("ReportDate", "ProductId")
    )

    SparkUtils.replaceDates(
      spark,
      financialReportProduct,
      "nessie.serving.financial_report_product",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
