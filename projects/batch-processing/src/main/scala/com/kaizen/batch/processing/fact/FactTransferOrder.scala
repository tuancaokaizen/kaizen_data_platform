package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, datediff, greatest, lit, max, to_date, when}

/** nessie.dw.fact_transfer_order: one row per transfer order item, from the replenishment request to the delivery at
  * the shop (accumulating snapshot), for the orders requested or changed in [fromDate, toDate), including changes of
  * their shipment stops. The delivery time is the latest arrival of a DELIVERED or PARTIAL stop of the order.
  * nessie.dw.fact_transfer_order: mỗi dòng một dòng lệnh điều chuyển, từ yêu cầu bổ sung hàng đến lúc giao tới cửa
  * hàng (accumulating snapshot), cho các lệnh yêu cầu hoặc thay đổi trong [fromDate, toDate), gồm cả thay đổi của
  * điểm dừng giao hàng. Thời điểm giao là lần đến muộn nhất của điểm dừng DELIVERED hoặc PARTIAL của lệnh.
  */
object FactTransferOrder {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactTransferOrder", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val transferOrder = spark.table("nessie.fnd.transfer_order")
    val transferOrderItem = spark.table("nessie.fnd.transfer_order_item")
    val shipmentStop = spark.table("nessie.fnd.shipment_stop")

    val changedOrder = transferOrder
      .where(DateUtils.inDateRange(col("RequestedDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("TransferOrderId"))
      .union(transferOrderItem.where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate)).select(col("TransferOrderId")))
      .union(shipmentStop.where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate)).select(col("TransferOrderId")))
      .where(col("TransferOrderId").isNotNull)
      .distinct()

    val delivery = shipmentStop
      .join(changedOrder, Seq("TransferOrderId"))
      .groupBy("TransferOrderId")
      .agg(
        max(when(col("Status").isin("DELIVERED", "PARTIAL"), col("ActualArrivalTime"))).as("DeliveredTime"),
        max(col("ModifiedDate")).as("StopModifiedDate")
      )

    val orderHeader = transferOrder.select(
      col("Id").as("TransferOrderId"),
      col("FromWarehouseId").as("WarehouseId"),
      col("ToShopCode").as("ShopCode"),
      col("Status"),
      col("RequestedDate"),
      col("PlannedDeliveryDate"),
      col("ModifiedDate").as("OrderModifiedDate")
    )

    val deliveredDate = to_date(col("DeliveredTime"))
    val isDelivered = col("DeliveredTime").isNotNull
    val factTransferOrder = transferOrderItem
      .join(changedOrder, Seq("TransferOrderId"))
      .join(orderHeader, Seq("TransferOrderId"))
      .join(delivery, Seq("TransferOrderId"), "left")
      .select(
        col("Id").as("TransferOrderItemId"),
        col("TransferOrderId"),
        col("WarehouseId"),
        col("ShopCode"),
        col("ProductId"),
        col("Status"),
        col("RequestedDate").cast("date"),
        col("PlannedDeliveryDate").cast("date"),
        deliveredDate.as("DeliveredDate"),
        col("DeliveredTime").cast("timestamp"),
        col("RequestedQty").cast("int").as("RequestedQuantity"),
        col("ShippedQty").cast("int").as("ShippedQuantity"),
        col("ReceivedQty").cast("int").as("ReceivedQuantity"),
        when(isDelivered, greatest(col("RequestedQty") - col("ReceivedQty"), lit(0))).cast("int").as("ShortQuantity"),
        isDelivered.as("IsDelivered"),
        when(isDelivered, deliveredDate <= col("PlannedDeliveryDate")).as("IsOnTime"),
        when(isDelivered, datediff(deliveredDate, col("RequestedDate"))).as("LeadTimeDays"),
        when(isDelivered && col("RequestedQty") > 0, col("ReceivedQty") / col("RequestedQty")).cast("double").as("FillRate"),
        greatest(col("OrderModifiedDate"), col("ModifiedDate"), col("StopModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factTransferOrder,
      "nessie.dw.fact_transfer_order",
      keys = Seq("TransferOrderItemId"),
      partition = Partition.Months("RequestedDate")
    )
  }
}
