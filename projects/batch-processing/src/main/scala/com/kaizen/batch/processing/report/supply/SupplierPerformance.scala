package com.kaizen.batch.processing.report.supply

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, count, date_trunc, lit, sum, when}

/** nessie.serving.supplier_performance: supplier delivery performance per calendar month.
  * Actual vs promised lead time, on-time rate and fill rate on received purchase lines.
  * nessie.serving.supplier_performance: hiệu suất NCC theo tháng lịch.
  * Lead time thực vs cam kết, tỷ lệ đúng hạn và fill rate trên dòng đơn mua đã nhận.
  */
object SupplierPerformance {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("SupplierPerformance", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val monthFrom = DateUtils.monthStart(fromDate).toString
    val monthTo = DateUtils.exclusiveMonthEnd(toDate).toString
    val supplier = spark.table("nessie.dw.dim_supplier").select(col("SupplierId"), col("SupplierName"))

    // Score a month by received date when present, else order date / Chấm điểm tháng theo ngày nhận nếu có, không thì ngày đặt
    val purchase = spark.table("nessie.dw.fact_purchase_order")
      .where(
        DateUtils.inDateRange(col("OrderDate"), monthFrom, monthTo) ||
          DateUtils.inDateRange(col("ReceivedDate"), monthFrom, monthTo)
      )
      .withColumn(
        "ReportDate",
        date_trunc("month", coalesce(col("ReceivedDate"), col("OrderDate"))).cast("date")
      )
      .where(DateUtils.inDateRange(col("ReportDate"), monthFrom, monthTo))

    val supplierPerformance = purchase
      .groupBy("ReportDate", "SupplierId")
      .agg(
        count(lit(1)).cast("long").as("PurchaseLineCount"),
        sum(when(col("IsReceived") === true, 1).otherwise(0)).cast("long").as("ReceivedLineCount"),
        sum(when(col("IsOnTime") === true, 1).otherwise(0)).cast("long").as("OnTimeLineCount"),
        avg(when(col("IsReceived") === true, col("LeadTimeDays"))).cast("double").as("AverageLeadTimeDays"),
        avg(col("PromisedLeadTimeDays")).cast("double").as("AveragePromisedLeadTimeDays"),
        avg(when(col("IsReceived") === true, col("FillRate"))).cast("double").as("AverageFillRate"),
        sum(col("OrderedQuantity")).cast("long").as("OrderedQuantity"),
        sum(col("ReceivedQuantity")).cast("long").as("ReceivedQuantity"),
        sum(col("OrderedAmount")).cast("long").as("OrderedAmount"),
        sum(col("ReceivedAmount")).cast("long").as("ReceivedAmount")
      )
      .join(supplier, Seq("SupplierId"), "left")
      .select(
        col("ReportDate"),
        col("SupplierId"),
        coalesce(col("SupplierName"), lit("Unknown")).as("SupplierName"),
        col("PurchaseLineCount"),
        col("ReceivedLineCount"),
        col("OnTimeLineCount"),
        when(col("ReceivedLineCount") > 0, col("OnTimeLineCount") / col("ReceivedLineCount"))
          .otherwise(lit(0.0)).cast("double").as("OnTimeRate"),
        col("AverageLeadTimeDays"),
        col("AveragePromisedLeadTimeDays"),
        (coalesce(col("AverageLeadTimeDays"), lit(0.0)) - coalesce(col("AveragePromisedLeadTimeDays"), lit(0.0)))
          .cast("double").as("LeadTimeVarianceDays"),
        col("AverageFillRate"),
        col("OrderedQuantity"),
        col("ReceivedQuantity"),
        col("OrderedAmount"),
        col("ReceivedAmount")
      )

    SparkUtils.replaceDates(
      spark,
      supplierPerformance,
      "nessie.serving.supplier_performance",
      dateColumn = "ReportDate",
      fromDate = monthFrom,
      toDate = monthTo,
      partition = Partition.Months("ReportDate")
    )
  }
}
