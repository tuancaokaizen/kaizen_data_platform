package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, datediff, greatest, to_date, when}

/** nessie.dw.fact_purchase_order: one row per purchase order item, from order to receipt at the distribution centre
  * (accumulating snapshot), for the orders placed or changed in [fromDate, toDate).
  * LeadTimeDays, IsOnTime and FillRate are null until the order is received.
  * nessie.dw.fact_purchase_order: mỗi dòng một dòng đơn mua, từ lúc đặt đến lúc nhận tại kho (accumulating snapshot),
  * cho các đơn đặt hoặc thay đổi trong [fromDate, toDate).
  * LeadTimeDays, IsOnTime và FillRate là null cho đến khi đơn được nhận.
  */
object FactPurchaseOrder {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactPurchaseOrder", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val purchaseOrder = spark.table("nessie.fnd.purchase_order")
    val purchaseOrderItem = spark.table("nessie.fnd.purchase_order_item")

    // An order is rebuilt when it was placed or changed in the window, or one of its items changed
    // Dựng lại đơn khi đơn được đặt hoặc thay đổi trong cửa sổ, hoặc một dòng của đơn thay đổi
    val changedOrder = purchaseOrder
      .where(DateUtils.inDateRange(to_date(col("OrderDate")), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("PurchaseOrderId"))
      .union(purchaseOrderItem.where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate)).select(col("PurchaseOrderId")))
      .distinct()

    val orderHeader = purchaseOrder.select(
      col("Id").as("PurchaseOrderId"),
      col("SupplierId"),
      col("WarehouseId"),
      col("Status"),
      col("OrderDate").as("OrderTime"),
      col("ExpectedDate"),
      col("ReceivedDate").as("ReceivedTime"),
      col("ModifiedDate").as("OrderModifiedDate")
    )
    val supplier = spark.table("nessie.fnd.supplier").select(col("Id").as("SupplierId"), col("LeadTimeMeanDays"))

    val orderDate = to_date(col("OrderTime"))
    val receivedDate = to_date(col("ReceivedTime"))
    val isReceived = col("ReceivedTime").isNotNull
    val factPurchaseOrder = purchaseOrderItem
      .join(changedOrder, Seq("PurchaseOrderId"))
      .join(orderHeader, Seq("PurchaseOrderId"))
      .join(broadcast(supplier), Seq("SupplierId"), "left")
      .select(
        col("Id").as("PurchaseOrderItemId"),
        col("PurchaseOrderId"),
        col("SupplierId"),
        col("WarehouseId"),
        col("ProductId"),
        col("Status"),
        orderDate.as("OrderDate"),
        col("OrderTime").cast("timestamp"),
        col("ExpectedDate").cast("date"),
        receivedDate.as("ReceivedDate"),
        col("ReceivedTime").cast("timestamp"),
        col("OrderedQty").cast("int").as("OrderedQuantity"),
        col("ReceivedQty").cast("int").as("ReceivedQuantity"),
        col("UnitCost").cast("long"),
        (col("OrderedQty") * col("UnitCost")).cast("long").as("OrderedAmount"),
        (col("ReceivedQty") * col("UnitCost")).cast("long").as("ReceivedAmount"),
        col("LeadTimeMeanDays").cast("decimal(5,2)").as("PromisedLeadTimeDays"),
        when(isReceived, datediff(receivedDate, orderDate)).as("LeadTimeDays"),
        isReceived.as("IsReceived"),
        when(isReceived, receivedDate <= col("ExpectedDate")).as("IsOnTime"),
        when(isReceived && col("OrderedQty") > 0, col("ReceivedQty") / col("OrderedQty")).cast("double").as("FillRate"),
        greatest(col("OrderModifiedDate"), col("ModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factPurchaseOrder,
      "nessie.dw.fact_purchase_order",
      keys = Seq("PurchaseOrderItemId"),
      partition = Partition.Months("OrderDate")
    )
  }
}
