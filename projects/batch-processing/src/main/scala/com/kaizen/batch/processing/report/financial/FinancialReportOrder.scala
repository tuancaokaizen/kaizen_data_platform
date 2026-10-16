package com.kaizen.batch.processing.report.financial

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit, sum, when}

/** nessie.serving.financial_report_order: profit per document, a SALE row per order (ReturnId "") on the
  * order date and a RETURN row per return on the return date. Gift cost of the document is its promotion cost.
  * nessie.serving.financial_report_order: lãi lỗ theo chứng từ, mỗi đơn một dòng SALE (ReturnId "") theo
  * ngày đơn và mỗi phiếu trả một dòng RETURN theo ngày trả. Giá vốn quà tặng của chứng từ là chi phí khuyến mãi.
  */
object FinancialReportOrder {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FinancialReportOrder", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)

    // Value of a sold item, 0 for a gift / Giá trị của dòng bán, 0 với quà tặng
    val unlessGift = (column: String) => when(col("IsGift"), lit(0L)).otherwise(col(column))
    // Value of a gift, 0 for a sold item / Giá trị của quà tặng, 0 với dòng bán
    val onlyGift = (column: String) => when(col("IsGift"), col(column)).otherwise(lit(0L))

    val saleDocument = spark.table("nessie.dw.fact_sale_transaction")
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate))
      .groupBy(col("OrderDate").as("ReportDate"), col("ShopCode"), col("OrderId"), col("EmployeeSaleId"), col("OrderStatus"))
      .agg(
        sum(unlessGift("Quantity")).as("SalesQuantity"),
        sum(unlessGift("LineAmount")).as("GrossRevenue"),
        sum(unlessGift("CostAmount")).as("TotalCogs"),
        sum(onlyGift("CostAmount")).as("TotalPromotionCost")
      )
      .withColumn("DocumentType", lit("SALE"))
      .withColumn("ReturnId", lit(""))
      .withColumn("ReturnQuantity", lit(0L))
      .withColumn("ReturnAmount", lit(0L))

    // A return gives the cost back / Phiếu trả hoàn lại giá vốn
    val orderStatus = spark.table("nessie.fnd.order").select(col("Id").as("OrderId"), col("Status").as("OrderStatus"))
    val returnDocument = spark.table("nessie.dw.fact_return_transaction")
      .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate))
      .groupBy(col("ReturnDate").as("ReportDate"), col("ShopCode"), col("OrderId"), col("ReturnId"), col("EmployeeSaleId"))
      .agg(
        sum(unlessGift("Quantity")).as("ReturnQuantity"),
        sum(unlessGift("LineAmount")).as("ReturnAmount"),
        (-sum(unlessGift("CostAmount"))).as("TotalCogs"),
        (-sum(onlyGift("CostAmount"))).as("TotalPromotionCost")
      )
      .join(orderStatus, Seq("OrderId"), "left")
      .withColumn("OrderStatus", coalesce(col("OrderStatus"), lit("Unknown")))
      .withColumn("DocumentType", lit("RETURN"))
      .withColumn("SalesQuantity", lit(0L))
      .withColumn("GrossRevenue", lit(0L))

    val shop = spark.table("nessie.dw.dim_shop").select("ShopCode", "ShopName")

    val financialReportOrder = saleDocument
      .unionByName(returnDocument)
      .join(broadcast(shop), Seq("ShopCode"), "left")
      .withColumn("ShopName", coalesce(col("ShopName"), lit("Unknown")))
      .withColumn("EmployeeSaleId", coalesce(col("EmployeeSaleId"), lit("")))
      .transform(rows => FinancialMeasures.complete(
        rows,
        Seq("ReportDate", "ShopCode", "ShopName", "DocumentType", "OrderId", "ReturnId", "EmployeeSaleId", "OrderStatus")
      ))

    SparkUtils.replaceDates(
      spark,
      financialReportOrder,
      "nessie.serving.financial_report_order",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
