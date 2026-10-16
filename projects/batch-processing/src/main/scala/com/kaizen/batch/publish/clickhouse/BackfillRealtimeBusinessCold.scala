package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{ConfigUtils, DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date}

/** Rebuilds realtime_business_by_order cold (and optionally seeds hot) from lakehouse fnd.
  * Why: Kafka retention cannot cover history before the stream started.
  * Dựng lại realtime_business_by_order cold (và tuỳ chọn seed hot) từ fnd.
  * Lý do: retention Kafka không phủ được lịch sử trước khi stream chạy.
  */
object BackfillRealtimeBusinessCold {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  // args(3): optional hotDays, default 14 / số ngày hot tuỳ chọn
  // args(4): optional seedHot true|false, default true / có seed ClickHouse hot không
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("BackfillRealtimeBusinessCold", args(0)).getOrCreate()
    process(
      spark,
      args(0),
      args(1),
      args(2),
      args.lift(3).getOrElse("14"),
      args.lift(4).getOrElse("true")
    )
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, environment: String, fromDate: String, toDate: String,
      hotDaysText: String, seedHotText: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val hotDays = if (hotDaysText.trim.isEmpty) 14 else hotDaysText.trim.toInt
    require(hotDays > 0, s"hotDays must be > 0, got $hotDays")
    val seedHot = !seedHotText.equalsIgnoreCase("false")
    val hotCutoff = DateUtils.today().minusDays(hotDays.toLong).toString

    val business = RealtimeBusinessByOrder.fromLakehouse(spark, fromDate, toDate).cache()
    val total = business.count()
    if (total == 0) {
      println(s"BackfillRealtimeBusinessCold: no rows in [$fromDate, $toDate)")
      business.unpersist()
      return
    }

    val cold = business.where(col("ReportDate") < to_date(lit(hotCutoff)))
    val coldCount = cold.count()
    if (coldCount > 0) {
      // Full overwrite — merge would keep stale SALE grains / Ghi đè toàn bộ — merge sẽ giữ grain SALE cũ
      SparkUtils.overwriteTable(
        spark,
        cold,
        RealtimeBusinessByOrder.ServingTable,
        partition = Partition.Months("ReportDate"),
        allowEmpty = false
      )
      println(s"BackfillRealtimeBusinessCold: wrote $coldCount cold rows (ReportDate < $hotCutoff)")
    }

    if (seedHot) {
      val hot = business.where(col("ReportDate") >= to_date(lit(hotCutoff)))
      val hotCount = hot.count()
      if (hotCount > 0) {
        ClickHouseSink.append(hot, ConfigUtils.clickHouse(environment), RealtimeBusinessByOrder.HotTable)
        println(s"BackfillRealtimeBusinessCold: seeded $hotCount hot rows (ReportDate >= $hotCutoff)")
      }
    }
    business.unpersist()
  }
}
