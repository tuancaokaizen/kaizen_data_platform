package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, greatest, lit, max, sum, when}

/** nessie.dw.fact_shipment: one row per delivery trip from a distribution centre (accumulating snapshot), for the trips
  * of [fromDate, toDate) and the trips whose stops, items or events changed in the window.
  * A stop is on time when it arrives at most OnTimeToleranceMinutes after the planned time.
  * Load factors compare the loaded weight and volume with the vehicle capacity.
  * nessie.dw.fact_shipment: mỗi dòng một chuyến giao từ kho (accumulating snapshot), cho các chuyến của
  * [fromDate, toDate) và các chuyến có điểm dừng, hàng hoặc sự kiện thay đổi trong cửa sổ.
  * Điểm dừng đúng giờ khi đến trễ tối đa OnTimeToleranceMinutes so với giờ dự kiến.
  * Hệ số tải so khối lượng và thể tích đã xếp với tải trọng của xe.
  */
object FactShipment {

  private val OnTimeToleranceMinutes = 30

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactShipment", args(0)).getOrCreate()
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

    val stop = shipmentStop.join(changedShipment, Seq("ShipmentId"))
    val isDelivered = col("Status").isin("DELIVERED", "PARTIAL")
    val delayMinutes = (col("ActualArrivalTime").cast("long") - col("PlannedArrivalTime").cast("long")) / 60
    val stopTotal = stop
      .groupBy("ShipmentId")
      .agg(
        sum(when(isDelivered, 1).otherwise(0)).cast("int").as("DeliveredStopCount"),
        sum(when(isDelivered && delayMinutes <= OnTimeToleranceMinutes, 1).otherwise(0)).cast("int").as("OnTimeStopCount"),
        max(when(isDelivered, col("ActualArrivalTime"))).as("CompletedTime"),
        max(col("ModifiedDate")).as("StopModifiedDate")
      )
    val itemTotal = shipmentItem
      .join(stop.select(col("Id").as("ShipmentStopId"), col("ShipmentId")), Seq("ShipmentStopId"))
      .groupBy("ShipmentId")
      .agg(
        sum(col("Qty")).cast("long").as("PlannedQuantity"),
        sum(col("ReceivedQty")).cast("long").as("ReceivedQuantity"),
        max(col("ModifiedDate")).as("ItemModifiedDate")
      )
    val vehicle = spark.table("nessie.fnd.vehicle").select(col("Id").as("VehicleId"), col("CapacityKg"), col("CapacityM3"))

    val factShipment = shipment
      .withColumnRenamed("Id", "ShipmentId")
      .join(changedShipment, Seq("ShipmentId"))
      .join(stopTotal, Seq("ShipmentId"), "left")
      .join(itemTotal, Seq("ShipmentId"), "left")
      .join(broadcast(vehicle), Seq("VehicleId"), "left")
      .select(
        col("ShipmentId"),
        col("ShipmentDate").cast("date"),
        col("WarehouseId"),
        col("CarrierId"),
        col("VehicleId"),
        col("Status"),
        col("PlannedDepartureTime").cast("timestamp"),
        col("ActualDepartureTime").cast("timestamp"),
        ((col("ActualDepartureTime").cast("long") - col("PlannedDepartureTime").cast("long")) / 60).cast("int").as("DepartureDelayMinutes"),
        col("CompletedTime").cast("timestamp"),
        col("DistanceKm").cast("decimal(7,2)"),
        col("TotalWeightKg").cast("decimal(9,2)"),
        col("TotalVolumeM3").cast("decimal(8,3)"),
        col("StopCount").cast("int"),
        coalesce(col("DeliveredStopCount"), lit(0)).as("DeliveredStopCount"),
        coalesce(col("OnTimeStopCount"), lit(0)).as("OnTimeStopCount"),
        coalesce(col("PlannedQuantity"), lit(0L)).as("PlannedQuantity"),
        coalesce(col("ReceivedQuantity"), lit(0L)).as("ReceivedQuantity"),
        col("TransportCost").cast("long"),
        when(col("CapacityKg") > 0, col("TotalWeightKg") / col("CapacityKg")).cast("double").as("WeightLoadFactor"),
        when(col("CapacityM3") > 0, col("TotalVolumeM3") / col("CapacityM3")).cast("double").as("VolumeLoadFactor"),
        when(col("TotalWeightKg") > 0, col("TransportCost") / col("TotalWeightKg")).cast("double").as("CostPerKg"),
        when(col("DistanceKm") > 0, col("TransportCost") / col("DistanceKm")).cast("double").as("CostPerKm"),
        when(col("StopCount") > 0, col("TransportCost") / col("StopCount")).cast("double").as("CostPerStop"),
        greatest(col("ModifiedDate"), col("StopModifiedDate"), col("ItemModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factShipment,
      "nessie.dw.fact_shipment",
      keys = Seq("ShipmentId"),
      partition = Partition.Months("ShipmentDate")
    )
  }
}
