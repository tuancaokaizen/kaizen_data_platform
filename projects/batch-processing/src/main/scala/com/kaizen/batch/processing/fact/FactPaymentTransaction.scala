package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, greatest}

/** nessie.dw.fact_payment_transaction: one row per payment transaction of every status, for the payments
  * made or changed in [fromDate, toDate) (a PENDING payment that later succeeds is updated).
  * A payment split over several methods has several rows. PaymentTypeId 1 = receipt (sale), 2 = expenditure (refund).
  * A refund has no OrderId on the payment, it is taken from its return.
  * nessie.dw.fact_payment_transaction: mỗi dòng một giao dịch thanh toán mọi trạng thái, của các phiếu
  * lập hoặc thay đổi trong [fromDate, toDate) (phiếu PENDING sau đó thành công sẽ được cập nhật).
  * Phiếu chia nhiều phương thức có nhiều dòng. PaymentTypeId 1 = phiếu thu (bán), 2 = phiếu chi (hoàn tiền).
  * Phiếu hoàn không có OrderId trên phiếu, lấy từ phiếu trả.
  */
object FactPaymentTransaction {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactPaymentTransaction", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val payment = spark.table("nessie.fnd.payment")
    val transaction = spark.table("nessie.fnd.transaction")

    // A payment is rebuilt when its header or one of its transactions was made or changed in the window
    // Dựng lại phiếu khi đầu phiếu hoặc một giao dịch được lập hoặc thay đổi trong cửa sổ
    val changedPayment = payment
      .where(DateUtils.inDateRange(col("PaymentDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("PaymentId"))
      .union(
        transaction
          .where(DateUtils.inDateRange(col("PaymentDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("PaymentId"))
      )
      .distinct()

    val paymentHeader = payment.select(
      col("Id").as("PaymentId"),
      col("PaymentDate").as("HeaderPaymentDate"),
      col("CreatedDate").as("PaymentTime"),
      col("PaymentType").as("PaymentTypeId"),
      col("Status").as("PaymentStatus"),
      col("OrderId").as("PaymentOrderId"),
      col("ReturnId"),
      col("ShopCode"),
      col("TotalPayment").as("PaymentTotal"),
      col("Attempt"),
      col("ModifiedDate").as("PaymentModifiedDate")
    )
    val returnOrder = spark.table("nessie.fnd.return").select(col("Id").as("ReturnId"), col("OrderId").as("ReturnOrderId"))

    val factPaymentTransaction = transaction
      .join(changedPayment, Seq("PaymentId"))
      .join(paymentHeader, Seq("PaymentId"))
      .join(returnOrder, Seq("ReturnId"), "left")
      .select(
        col("Id").as("TransactionId"),
        col("PaymentId"),
        col("HeaderPaymentDate").cast("date").as("PaymentDate"),
        col("PaymentTime").cast("timestamp"),
        col("PaymentTypeId").cast("int"),
        col("PaymentStatus"),
        coalesce(col("PaymentOrderId"), col("ReturnOrderId")).as("OrderId"),
        col("ReturnId"),
        col("ShopCode"),
        col("PaymentMethodId").cast("int"),
        col("Amount").cast("long"),
        col("PaymentTotal").cast("long"),
        col("Attempt").cast("int"),
        greatest(col("PaymentModifiedDate"), col("ModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factPaymentTransaction,
      "nessie.dw.fact_payment_transaction",
      keys = Seq("TransactionId"),
      partition = Partition.Months("PaymentDate")
    )
  }
}
