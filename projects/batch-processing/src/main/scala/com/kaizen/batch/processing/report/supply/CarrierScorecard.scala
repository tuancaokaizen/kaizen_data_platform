package com.kaizen.batch.processing.report.supply

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, count, date_trunc, lit, sum, when}

/** nessie.serving.carrier_scorecard: carrier delivery scorecard per ISO week.
  * OTIF, average delay, cost per kg / stop, and load factors from trips and stops.
  * nessie.serving.carrier_scorecard: bảng điểm hãng vận chuyển theo tuần ISO.
  * OTIF, trễ TB, chi phí/kg / điểm giao, và hệ số tải từ chuyến và điểm dừng.
  */
object CarrierScorecard {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("CarrierScorecard", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val weekFrom = DateUtils.weekStart(fromDate).toString
    val weekTo = DateUtils.exclusiveWeekEnd(toDate).toString
    val carrier = spark.table("nessie.dw.dim_carrier").select(col("CarrierId"), col("CarrierName"))

    val stops = spark.table("nessie.dw.fact_shipment_stop")
      .where(DateUtils.inDateRange(col("ShipmentDate"), weekFrom, weekTo))
      .withColumn("ReportDate", date_trunc("week", col("ShipmentDate")).cast("date"))
      .groupBy("ReportDate", "CarrierId")
      .agg(
        count(lit(1)).cast("long").as("StopCount"),
        sum(when(col("IsDelivered") === true, 1).otherwise(0)).cast("long").as("DeliveredStopCount"),
        sum(when(col("IsOnTime") === true, 1).otherwise(0)).cast("long").as("OnTimeStopCount"),
        sum(when(col("IsOnTimeInFull") === true, 1).otherwise(0)).cast("long").as("OnTimeInFullStopCount"),
        avg(when(col("IsDelivered") === true, col("DelayMinutes"))).cast("double").as("AverageDelayMinutes"),
        sum(col("AllocatedTransportCost")).cast("long").as("AllocatedTransportCost")
      )

    val trips = spark.table("nessie.dw.fact_shipment")
      .where(DateUtils.inDateRange(col("ShipmentDate"), weekFrom, weekTo))
      .withColumn("ReportDate", date_trunc("week", col("ShipmentDate")).cast("date"))
      .groupBy("ReportDate", "CarrierId")
      .agg(
        count(lit(1)).cast("long").as("ShipmentCount"),
        sum(col("TransportCost")).cast("long").as("TransportCost"),
        sum(col("TotalWeightKg")).cast("double").as("TotalWeightKg"),
        sum(col("TotalVolumeM3")).cast("double").as("TotalVolumeM3"),
        avg(col("WeightLoadFactor")).cast("double").as("AverageWeightLoadFactor"),
        avg(col("VolumeLoadFactor")).cast("double").as("AverageVolumeLoadFactor")
      )

    val carrierScorecard = stops
      .join(trips, Seq("ReportDate", "CarrierId"), "outer")
      .join(carrier, Seq("CarrierId"), "left")
      .select(
        col("ReportDate"),
        col("CarrierId"),
        coalesce(col("CarrierName"), lit("Unknown")).as("CarrierName"),
        coalesce(col("ShipmentCount"), lit(0L)).as("ShipmentCount"),
        coalesce(col("StopCount"), lit(0L)).as("StopCount"),
        coalesce(col("DeliveredStopCount"), lit(0L)).as("DeliveredStopCount"),
        coalesce(col("OnTimeStopCount"), lit(0L)).as("OnTimeStopCount"),
        coalesce(col("OnTimeInFullStopCount"), lit(0L)).as("OnTimeInFullStopCount"),
        when(coalesce(col("DeliveredStopCount"), lit(0L)) > 0,
          col("OnTimeInFullStopCount") / col("DeliveredStopCount")).otherwise(lit(0.0)).cast("double").as("OnTimeInFullRate"),
        col("AverageDelayMinutes"),
        coalesce(col("TransportCost"), lit(0L)).as("TransportCost"),
        coalesce(col("TotalWeightKg"), lit(0.0)).as("TotalWeightKg"),
        coalesce(col("TotalVolumeM3"), lit(0.0)).as("TotalVolumeM3"),
        when(coalesce(col("TotalWeightKg"), lit(0.0)) > 0, col("TransportCost") / col("TotalWeightKg"))
          .otherwise(lit(0.0)).cast("double").as("CostPerKg"),
        when(coalesce(col("StopCount"), lit(0L)) > 0, col("TransportCost") / col("StopCount"))
          .otherwise(lit(0.0)).cast("double").as("CostPerStop"),
        col("AverageWeightLoadFactor"),
        col("AverageVolumeLoadFactor")
      )

    SparkUtils.replaceDates(
      spark,
      carrierScorecard,
      "nessie.serving.carrier_scorecard",
      dateColumn = "ReportDate",
      fromDate = weekFrom,
      toDate = weekTo,
      partition = Partition.Months("ReportDate")
    )
  }
}
