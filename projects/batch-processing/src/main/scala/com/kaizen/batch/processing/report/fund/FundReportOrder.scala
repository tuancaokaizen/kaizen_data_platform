package com.kaizen.batch.processing.report.fund

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit, row_number, sum, when}

/** nessie.serving.fund_report_order: cash and document value per payment day, order, return and payment method.
  * Runs first, FundReport and FundReportShop roll up from it.
  * Only SUCCESS payments count. Receipts and expenditures are the cash moved. The document value is the payment
  * total shared between its transactions; an order counts only when Completed or Returned, and a return paid
  * twice counts once (largest, then latest refund), so the extra cash shows in ReconciliationDiff.
  * nessie.serving.fund_report_order: tiền và giá trị chứng từ theo ngày thanh toán, đơn, phiếu trả và phương thức.
  * Chạy trước, FundReport và FundReportShop cộng dồn từ bảng này.
  * Chỉ tính phiếu SUCCESS. Thu và chi là tiền thực chuyển. Giá trị chứng từ là tổng phiếu chia cho các giao dịch;
  * đơn chỉ tính khi Completed hoặc Returned, phiếu trả hoàn tiền hai lần chỉ tính một lần (phiếu lớn nhất rồi mới nhất),
  * nên tiền chi thừa hiện ở ReconciliationDiff.
  */
object FundReportOrder {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FundReportOrder", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val isReceipt = col("PaymentTypeId") === FundMeasures.ReceiptTypeId
    val isRefund = col("PaymentTypeId") === FundMeasures.RefundTypeId

    // Share of the payment total carried by each transaction / Phần tổng phiếu mà mỗi giao dịch mang
    val successfulTransaction = spark.table("nessie.dw.fact_payment_transaction")
      .where(col("PaymentStatus") === "SUCCESS" && DateUtils.inDateRange(col("PaymentDate"), fromDate, toDate))
      .withColumn("PaymentAmount", sum("Amount").over(Window.partitionBy("PaymentId")))
      .withColumn(
        "DocumentAmount",
        when(col("PaymentAmount") =!= 0, col("PaymentTotal") * col("Amount") / col("PaymentAmount")).otherwise(lit(0.0))
      )

    // A return refunded twice counts once in the document value / Phiếu trả hoàn tiền hai lần chỉ tính một lần vào giá trị chứng từ
    val refundOrder = Window
      .partitionBy("ReturnId")
      .orderBy(col("PaymentTotal").desc, col("PaymentTime").desc, col("PaymentId").desc)
    val countedRefund = successfulTransaction
      .where(isRefund && col("ReturnId").isNotNull)
      .select("PaymentId", "ReturnId", "PaymentTotal", "PaymentTime")
      .distinct()
      .withColumn("RefundRank", row_number().over(refundOrder))
      .where(col("RefundRank") === 1)
      .select(col("PaymentId"), lit(true).as("IsCountedRefund"))

    val settledOrder = spark.table("nessie.fnd.order")
      .where(col("Status").isin("Completed", "Returned"))
      .select(col("Id").as("OrderId"), lit(true).as("IsSettledOrder"))
    val paymentMethod = spark.table("nessie.dw.dim_payment_method")
    val shop = spark.table("nessie.dw.dim_shop").select("ShopCode", "ShopName")

    val documentTransaction = successfulTransaction
      .join(countedRefund, Seq("PaymentId"), "left")
      .join(settledOrder, Seq("OrderId"), "left")
      .join(broadcast(paymentMethod), Seq("PaymentMethodId"), "left")
      .join(broadcast(shop), Seq("ShopCode"), "left")
      .select(
        col("PaymentDate").as("ReportDate"),
        coalesce(col("OrderId"), lit("")).as("OrderId"),
        when(isRefund, coalesce(col("ReturnId"), lit(""))).otherwise(lit("")).as("ReturnId"),
        col("ShopCode"),
        coalesce(col("ShopName"), lit("Unknown")).as("ShopName"),
        coalesce(col("PaymentMethodName"), lit("Unknown")).as("PaymentMethodName"),
        when(isReceipt, col("Amount")).otherwise(lit(0L)).as("TotalReceipts"),
        when(isRefund, col("Amount")).otherwise(lit(0L)).as("TotalExpenditures"),
        when(isReceipt && coalesce(col("IsSettledOrder"), lit(false)), col("DocumentAmount")).otherwise(lit(0.0)).as("TotalOrderAmount"),
        when(isRefund && coalesce(col("IsCountedRefund"), lit(false)), col("DocumentAmount")).otherwise(lit(0.0)).as("TotalReturnAmount")
      )

    val fundReportOrder = FundMeasures.rollUp(
      documentTransaction,
      dimensionColumns = Seq("ReportDate", "OrderId", "ReturnId", "ShopCode", "ShopName", "PaymentMethodName"),
      keys = Seq("ReportDate", "OrderId", "ReturnId", "ShopCode", "PaymentMethodName")
    )

    SparkUtils.replaceDates(
      spark,
      fundReportOrder,
      "nessie.serving.fund_report_order",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
