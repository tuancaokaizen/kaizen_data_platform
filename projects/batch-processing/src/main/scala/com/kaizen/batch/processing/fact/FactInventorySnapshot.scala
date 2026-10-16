package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, date_sub, lit, min, sum, when}

/** nessie.dw.fact_inventory_snapshot: end-of-day stock per location and product (periodic snapshot), for the days
  * [fromDate, toDate) and the snapshots changed in the window.
  * Demand28DayQuantity is the outbound demand of the DemandDays days ending on the snapshot day: sales for a shop,
  * transfers out for a distribution centre. DaysOfSupply = on hand / average daily demand, null without demand.
  * Values use the current stock price of the product.
  * nessie.dw.fact_inventory_snapshot: tồn cuối ngày theo điểm và sản phẩm (periodic snapshot), cho các ngày
  * [fromDate, toDate) và các snapshot thay đổi trong cửa sổ.
  * Demand28DayQuantity là nhu cầu xuất của DemandDays ngày kết thúc ở ngày snapshot: bán hàng với cửa hàng,
  * điều chuyển đi với kho. DaysOfSupply = tồn / nhu cầu bình quân ngày, null khi không có nhu cầu.
  * Giá trị tính theo giá nhập hiện tại của sản phẩm.
  */
object FactInventorySnapshot {

  private val DemandDays = 28

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactInventorySnapshot", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val locationKeys = Seq("LocationType", "LocationId", "ProductId")
    val snapshot = spark.table("nessie.fnd.inventory_snapshot")
      .where(DateUtils.inDateRange(col("SnapshotDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .cache()

    // Demand is read from the earliest snapshot day minus the demand window
    // Nhu cầu được đọc từ ngày snapshot sớm nhất trừ đi cửa sổ nhu cầu
    val firstSnapshotDate = Option(snapshot.agg(min(col("SnapshotDate"))).head().getDate(0)).map(_.toString).getOrElse(fromDate)
    val dailyDemand = spark.table("nessie.fnd.stock_movement")
      .where(
        ((col("LocationType") === "SHOP" && col("MovementType") === "SALE") ||
          (col("LocationType") === "DC" && col("MovementType") === "TRANSFER_OUT")) &&
          col("MovementDate") >= date_sub(lit(firstSnapshotDate).cast("date"), DemandDays - 1)
      )
      .groupBy((locationKeys :+ "MovementDate").map(col): _*)
      .agg(sum(-col("Qty")).as("DemandQuantity"))

    val windowDemand = snapshot
      .select((locationKeys :+ "SnapshotDate").map(col): _*)
      .join(dailyDemand, locationKeys)
      .where(col("MovementDate") > date_sub(col("SnapshotDate"), DemandDays) && col("MovementDate") <= col("SnapshotDate"))
      .groupBy((locationKeys :+ "SnapshotDate").map(col): _*)
      .agg(sum(col("DemandQuantity")).as("Demand28DayQuantity"))

    val product = spark.table("nessie.fnd.product").select(col("Id").as("ProductId"), col("StockPrice"), col("UnitVolumeCm3"))

    val demand = coalesce(col("Demand28DayQuantity"), lit(0L))
    val factInventorySnapshot = snapshot
      .join(windowDemand, locationKeys :+ "SnapshotDate", "left")
      .join(broadcast(product), Seq("ProductId"), "left")
      .select(
        col("SnapshotDate").cast("date"),
        col("LocationType"),
        col("LocationId"),
        col("ProductId"),
        col("OnHandQty").cast("int").as("OnHandQuantity"),
        col("InTransitQty").cast("int").as("InTransitQuantity"),
        col("OnOrderQty").cast("int").as("OnOrderQuantity"),
        col("ExpiredQty").cast("int").as("ExpiredQuantity"),
        (coalesce(col("OnHandQty"), lit(0)) + coalesce(col("InTransitQty"), lit(0)) + coalesce(col("OnOrderQty"), lit(0)))
          .cast("int").as("InventoryPosition"),
        col("StockPrice").cast("long").as("UnitCost"),
        (col("OnHandQty") * col("StockPrice")).cast("long").as("OnHandValue"),
        (col("OnHandQty") * col("UnitVolumeCm3") / 1000000).cast("decimal(14,4)").as("OnHandVolumeM3"),
        coalesce(col("StockoutFlag"), lit(false)).as("IsStockout"),
        coalesce(col("OverCapacityFlag"), lit(false)).as("IsOverCapacity"),
        demand.cast("long").as("Demand28DayQuantity"),
        (demand / DemandDays).cast("double").as("AverageDailyDemand"),
        when(demand > 0, col("OnHandQty") * DemandDays / demand).cast("double").as("DaysOfSupply"),
        col("ModifiedDate").cast("timestamp")
      )

    SparkUtils.mergeInto(
      spark,
      factInventorySnapshot,
      "nessie.dw.fact_inventory_snapshot",
      keys = Seq("SnapshotDate", "LocationType", "LocationId", "ProductId"),
      partition = Partition.Months("SnapshotDate")
    )
    snapshot.unpersist()
  }
}
