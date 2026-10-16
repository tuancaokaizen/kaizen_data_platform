package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, count, countDistinct, greatest, hour, lit, max, sum, when}

/** nessie.dw.fact_order_transaction: one row per order of the orders sold, changed or returned in [fromDate, toDate).
  * Amounts are summed from the order items, so they match fact_sale_transaction; gift items count apart.
  * Return columns hold every return of the order so far, whatever its date.
  * nessie.dw.fact_order_transaction: mỗi dòng một đơn, cho các đơn bán, thay đổi hoặc bị trả trong [fromDate, toDate).
  * Số tiền cộng từ các dòng hàng nên khớp fact_sale_transaction; dòng quà tặng tính riêng.
  * Cột trả hàng gồm mọi lần trả của đơn tính đến nay, bất kể ngày trả.
  */
object FactOrderTransaction {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactOrderTransaction", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val order = spark.table("nessie.fnd.order")
    val orderItem = spark.table("nessie.fnd.order_item")
    val returnHeader = spark.table("nessie.fnd.return")
    val returnItem = spark.table("nessie.fnd.return_item")

    // An order is rebuilt when it was sold or changed in the window, or when one of its returns was
    // Dựng lại đơn khi đơn được bán hoặc thay đổi trong cửa sổ, hoặc khi một lần trả của đơn như vậy
    val changedOrder = order
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("OrderId"))
      .union(
        orderItem
          .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("OrderId"))
      )
      .union(
        returnHeader
          .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("OrderId"))
      )
      .union(
        returnItem
          .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("OrderId"))
      )
      .where(col("OrderId").isNotNull)
      .distinct()

    val isGift = col("IsGift") === true
    val itemTotal = orderItem
      .join(changedOrder, Seq("OrderId"))
      .groupBy("OrderId")
      .agg(
        count(lit(1)).cast("int").as("LineCount"),
        sum(when(isGift, 0).otherwise(col("Quantity"))).cast("int").as("Quantity"),
        sum(when(isGift, col("Quantity")).otherwise(0)).cast("int").as("GiftQuantity"),
        sum(col("Quantity") * col("UnitPrice")).cast("long").as("GrossAmount"),
        sum(col("DiscountAmount")).cast("long").as("DiscountAmount"),
        sum(col("LineAmount")).cast("long").as("NetAmount"),
        sum(when(isGift, 0).otherwise(col("Quantity") * col("UnitCost"))).cast("long").as("CostAmount"),
        sum(when(isGift, col("Quantity") * col("UnitCost")).otherwise(0)).cast("long").as("GiftCostAmount"),
        max(col("ModifiedDate")).as("ItemModifiedDate")
      )

    val returnTotal = returnItem
      .join(changedOrder, Seq("OrderId"))
      .groupBy("OrderId")
      .agg(
        countDistinct(col("ReturnId")).cast("int").as("ReturnCount"),
        sum(col("Quantity")).cast("int").as("ReturnQuantity"),
        sum(col("LineAmount")).cast("long").as("ReturnAmount"),
        max(col("ModifiedDate")).as("ReturnModifiedDate")
      )

    val factOrderTransaction = order
      .join(changedOrder, order("Id") === changedOrder("OrderId"))
      .join(itemTotal, Seq("OrderId"), "left")
      .join(returnTotal, Seq("OrderId"), "left")
      .select(
        col("OrderId"),
        col("OrderDate").cast("date"),
        col("TransactionDate").cast("timestamp"),
        hour(col("TransactionDate")).as("OrderHour"),
        col("ShopCode"),
        col("CustomerId"),
        col("EmployeeSaleId"),
        col("VoucherSeriesCode"),
        col("Status").as("OrderStatus"),
        coalesce(col("LineCount"), lit(0)).as("LineCount"),
        coalesce(col("Quantity"), lit(0)).as("Quantity"),
        coalesce(col("GiftQuantity"), lit(0)).as("GiftQuantity"),
        coalesce(col("GrossAmount"), lit(0L)).as("GrossAmount"),
        coalesce(col("DiscountAmount"), lit(0L)).as("DiscountAmount"),
        coalesce(col("NetAmount"), lit(0L)).as("NetAmount"),
        coalesce(col("CostAmount"), lit(0L)).as("CostAmount"),
        coalesce(col("GiftCostAmount"), lit(0L)).as("GiftCostAmount"),
        col("VoucherSeriesCode").isNotNull.as("HasVoucher"),
        (coalesce(col("GiftQuantity"), lit(0)) > 0).as("HasGift"),
        coalesce(col("ReturnCount"), lit(0)).as("ReturnCount"),
        coalesce(col("ReturnQuantity"), lit(0)).as("ReturnQuantity"),
        coalesce(col("ReturnAmount"), lit(0L)).as("ReturnAmount"),
        (coalesce(col("ReturnCount"), lit(0)) > 0).as("IsReturned"),
        (coalesce(col("NetAmount"), lit(0L)) - coalesce(col("ReturnAmount"), lit(0L))).cast("long").as("NetAfterReturnAmount"),
        greatest(col("ModifiedDate"), col("ItemModifiedDate"), col("ReturnModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factOrderTransaction,
      "nessie.dw.fact_order_transaction",
      keys = Seq("OrderId"),
      partition = Partition.Months("OrderDate")
    )
  }
}
