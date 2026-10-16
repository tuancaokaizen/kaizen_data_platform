package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{ConfigUtils, DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, to_date}

/** Moves ClickHouse hot report.realtime_business_by_order rows older than the hot window into Iceberg cold.
  * Chuyển dòng hot report.realtime_business_by_order cũ hơn cửa sổ hot sang Iceberg cold.
  *
  * hotDays (default 14) matches the report views UNION split; ClickHouse TTL is hotDays + 2.
  * hotDays (mặc định 14) khớp view UNION; TTL ClickHouse = hotDays + 2.
  */
object MigrateRealtimeBusinessToCold {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): optional hotDays, default 14 / số ngày hot tuỳ chọn, mặc định 14
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("MigrateRealtimeBusinessToCold", args(0)).getOrCreate()
    process(spark, args(0), args.lift(3).getOrElse("14"))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, environment: String, hotDaysText: String): Unit = {
    val hotDays = if (hotDaysText.trim.isEmpty) 14 else hotDaysText.trim.toInt
    require(hotDays > 0, s"hotDays must be > 0, got $hotDays")
    val cutoff = DateUtils.today().minusDays(hotDays.toLong).toString
    val clickHouse = ConfigUtils.clickHouse(environment)

    val query =
      s"""SELECT
         |  ReportDate, OrderId, ReturnId, DocumentType, toString(ShopCode) AS ShopCode, ShopName,
         |  toString(OrderStatus) AS OrderStatus,
         |  SalesAmount, ReturnAmount, NetRevenue, TotalReceipts, TotalExpenditures, NetCashFlow,
         |  PaymentMethodSummary, TotalOrderAmount, TotalReturnAmount, ReconciliationDiff,
         |  PendingOrder, Version, UpdatedAt
         |FROM report.realtime_business_by_order FINAL
         |WHERE ReportDate < toDate('$cutoff')
         |""".stripMargin

    val hot = ClickHouseSource.readQuery(spark, clickHouse, query)
    val count = hot.count()
    if (count == 0) {
      println(s"MigrateRealtimeBusinessToCold: nothing to migrate (ReportDate < $cutoff)")
      return
    }

    val aligned = hot
      .withColumn("ReportDate", to_date(col("ReportDate")))
      .withColumn("ReturnId", coalesce(col("ReturnId"), lit("")))
      .withColumn("PendingOrder", col("PendingOrder").cast("byte"))
      .withColumn("Version", col("Version").cast("long"))
      .withColumn("UpdatedAt", col("UpdatedAt").cast("timestamp"))

    SparkUtils.overwriteTable(
      spark,
      aligned,
      RealtimeBusinessByOrder.StagingTable,
      partition = Partition.Months("ReportDate"),
      allowEmpty = true
    )
    val staged = spark.table(RealtimeBusinessByOrder.StagingTable)
    SparkUtils.mergeInto(
      spark,
      staged,
      RealtimeBusinessByOrder.ServingTable,
      keys = RealtimeBusinessByOrder.MergeKeys,
      partition = Partition.Months("ReportDate")
    )
    println(s"MigrateRealtimeBusinessToCold: migrated $count rows (ReportDate < $cutoff)")
  }
}
