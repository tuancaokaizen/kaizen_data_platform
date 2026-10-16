package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, coalesce, col, count, greatest, lit, max, round, row_number, sum, when}

/** nessie.dw.fact_shipment_stop: one row per shop stop of a delivery trip, for the trips of [fromDate, toDate) and the
  * trips whose stops, items or events changed in the window. All stops of a changed trip are rebuilt together because
  * the transport cost is shared between them by planned quantity (equally when the trip has no quantity).
  * A stop is on time when it arrives at most OnTimeToleranceMinutes late, in full when every planned unit arrived.
  * ReasonCode is the first reason given by an event of the stop (e.g. TRAFFIC, SHORT_PICK).
  * nessie.dw.fact_shipment_stop: mỗi dòng một điểm dừng giao tại cửa hàng của chuyến, cho các chuyến của
  * [fromDate, toDate) và các chuyến có điểm dừng, hàng hoặc sự kiện thay đổi trong cửa sổ. Mọi điểm dừng của chuyến
  * thay đổi được dựng lại cùng lúc vì chi phí vận chuyển chia cho chúng theo số lượng dự kiến (chia đều khi chuyến
  * không có số lượng). Điểm dừng đúng giờ khi trễ tối đa OnTimeToleranceMinutes, đủ hàng khi mọi đơn vị dự kiến đều
  * tới. ReasonCode là lý do đầu tiên do sự kiện của điểm dừng ghi (vd TRAFFIC, SHORT_PICK).
  */
object FactShipmentStop {

  private val OnTimeToleranceMinutes = 30

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactShipmentStop", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val shipment = spark.table("nessie.fnd.shipment")
    val shipmentStop = spark.table("nessie.fnd.shipment_stop")
    val shipmentItem = spark.table("nessie.fnd.shipment_item")
    val shipmentEvent = spark.table("nessie.fnd.shipment_event")

    // A trip is rebuilt when the trip, one of its stops, items or events is in the window
    // Dựng lại chuyến khi chuyến, một điểm dừng, dòng hàng hoặc sự kiện của chuyến nằm trong cửa sổ
    val changedShipment = shipment
      .where(DateUtils.inDateRange(col("ShipmentDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("ShipmentId"))
      .union(shipmentStop.where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate)).select(col("ShipmentId")))
      .union(
        shipmentItem
          .where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .join(shipmentStop.select(col("Id").as("ShipmentStopId"), col("ShipmentId")), Seq("ShipmentStopId"))
          .select(col("ShipmentId"))
      )
      .union(shipmentEvent.where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate)).select(col("ShipmentId")))
      .distinct()

    val stop = shipmentStop
      .withColumnRenamed("Id", "ShipmentStopId")
      .join(changedShipment, Seq("ShipmentId"))

    val itemTotal = shipmentItem
      .join(stop.select(col("ShipmentStopId")), Seq("ShipmentStopId"))
      .groupBy("ShipmentStopId")
      .agg(
        sum(col("Qty")).cast("long").as("PlannedQuantity"),
        sum(col("ReceivedQty")).cast("long").as("ReceivedQuantity"),
        max(col("ModifiedDate")).as("ItemModifiedDate")
      )

    val stopReason = shipmentEvent
      .where(col("ShipmentStopId").isNotNull && col("ReasonCode").isNotNull)
      .join(changedShipment, Seq("ShipmentId"))
      .withColumn("ReasonRank", row_number().over(Window.partitionBy("ShipmentStopId").orderBy(asc("EventSeq"), asc("Id"))))
      .where(col("ReasonRank") === 1)
      .select(col("ShipmentStopId"), col("ReasonCode"))

    val trip = shipment.select(
      col("Id").as("ShipmentId"),
      col("ShipmentDate"),
      col("WarehouseId"),
      col("CarrierId"),
      col("VehicleId"),
      col("TransportCost"),
      col("ModifiedDate").as("ShipmentModifiedDate")
    )

    // Share of the trip cost: by planned quantity, equally when the trip has none
    // Phần chi phí chuyến: theo số lượng dự kiến, chia đều khi chuyến không có số lượng
    val byTrip = Window.partitionBy("ShipmentId")
    val plannedQuantity = coalesce(col("PlannedQuantity"), lit(0L))
    val tripQuantity = sum(plannedQuantity).over(byTrip)
    val costShare = when(tripQuantity > 0, plannedQuantity / tripQuantity).otherwise(lit(1.0) / count(lit(1)).over(byTrip))

    val isDelivered = col("Status").isin("DELIVERED", "PARTIAL")
    val delayMinutes = ((col("ActualArrivalTime").cast("long") - col("PlannedArrivalTime").cast("long")) / 60).cast("int")
    val isOnTime = isDelivered && delayMinutes <= OnTimeToleranceMinutes
    val isInFull = isDelivered && coalesce(col("ReceivedQuantity"), lit(0L)) >= plannedQuantity
    val factShipmentStop = stop
      .join(trip, Seq("ShipmentId"))
      .join(itemTotal, Seq("ShipmentStopId"), "left")
      .join(stopReason, Seq("ShipmentStopId"), "left")
      .select(
        col("ShipmentStopId"),
        col("ShipmentId"),
        col("StopSeq").cast("int").as("StopSequence"),
        col("ShipmentDate").cast("date"),
        col("WarehouseId"),
        col("CarrierId"),
        col("VehicleId"),
        col("ShopCode"),
        col("TransferOrderId"),
        col("Status"),
        col("PlannedArrivalTime").cast("timestamp"),
        col("ActualArrivalTime").cast("timestamp"),
        delayMinutes.as("DelayMinutes"),
        isDelivered.as("IsDelivered"),
        isOnTime.as("IsOnTime"),
        isInFull.as("IsInFull"),
        (isOnTime && isInFull).as("IsOnTimeInFull"),
        plannedQuantity.as("PlannedQuantity"),
        coalesce(col("ReceivedQuantity"), lit(0L)).as("ReceivedQuantity"),
        col("ReasonCode"),
        round(col("TransportCost") * costShare).cast("long").as("AllocatedTransportCost"),
        greatest(col("ShipmentModifiedDate"), col("ModifiedDate"), col("ItemModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factShipmentStop,
      "nessie.dw.fact_shipment_stop",
      keys = Seq("ShipmentStopId"),
      partition = Partition.Months("ShipmentDate")
    )
  }
}
