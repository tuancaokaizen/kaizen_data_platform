package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, greatest}

/** nessie.dw.fact_sale_transaction: one row per order item of the orders sold or changed in [fromDate, toDate).
  * LineAmount comes from Postgres and is already net of line discount and voucher.
  * Gift items have IsGift, LineAmount 0 and a cost that the reports count as promotion cost.
  * nessie.dw.fact_sale_transaction: mỗi dòng một dòng hàng của các đơn bán hoặc thay đổi trong [fromDate, toDate).
  * LineAmount lấy từ Postgres, đã trừ giảm giá dòng và voucher.
  * Dòng quà tặng có IsGift, LineAmount 0 và giá vốn được báo cáo tính vào chi phí khuyến mãi.
  */
object FactSaleTransaction {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactSaleTransaction", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val order = spark.table("nessie.fnd.order")
    val orderItem = spark.table("nessie.fnd.order_item")

    // An order is rebuilt when its header or one of its items was sold or changed in the window
    // Dựng lại đơn khi đầu đơn hoặc một dòng hàng được bán hoặc thay đổi trong cửa sổ
    val changedOrder = order
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("OrderId"))
      .union(
        orderItem
          .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("OrderId"))
      )
      .distinct()

    val orderHeader = order.select(
      col("Id").as("OrderId"),
      col("TransactionDate"),
      col("ShopCode"),
      col("CustomerId"),
      col("EmployeeSaleId"),
      col("VoucherSeriesCode"),
      col("Status").as("OrderStatus"),
      col("ModifiedDate").as("OrderModifiedDate")
    )

    val factSaleTransaction = orderItem
      .join(changedOrder, Seq("OrderId"))
      .join(orderHeader, Seq("OrderId"))
      .select(
        col("Id").as("OrderItemId"),
        col("OrderId"),
        col("LineNo").cast("int").as("LineNumber"),
        col("OrderDate").cast("date"),
        col("TransactionDate").cast("timestamp"),
        col("ShopCode"),
        col("CustomerId"),
        col("EmployeeSaleId"),
        col("ProductId"),
        col("VoucherSeriesCode"),
        col("IsGift").cast("boolean"),
        col("Quantity").cast("int"),
        col("UnitPrice").cast("long"),
        (col("Quantity") * col("UnitPrice")).cast("long").as("GrossAmount"),
        col("DiscountAmount").cast("long"),
        col("LineAmount").cast("long"),
        col("UnitCost").cast("long"),
        (col("Quantity") * col("UnitCost")).cast("long").as("CostAmount"),
        col("OrderStatus"),
        greatest(col("OrderModifiedDate"), col("ModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factSaleTransaction,
      "nessie.dw.fact_sale_transaction",
      keys = Seq("OrderItemId"),
      partition = Partition.Months("OrderDate")
    )
  }
}
