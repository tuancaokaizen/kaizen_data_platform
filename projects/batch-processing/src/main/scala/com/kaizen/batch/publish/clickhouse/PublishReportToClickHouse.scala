package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.{ConfigUtils, DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_sub, lit, to_date}

/** nessie.serving.<report> → ClickHouse report.<report>: replaces the ClickHouse table with the hot window of the report.
  * Day-grain reports keep 30 days, document-grain reports 7 days; older days stay readable in Iceberg.
  * nessie.serving.<report> → ClickHouse report.<report>: thay bảng ClickHouse bằng cửa sổ hot của báo cáo.
  * Báo cáo theo ngày giữ 30 ngày, báo cáo theo chứng từ giữ 7 ngày; ngày cũ hơn vẫn đọc được trong Iceberg.
  */
object PublishReportToClickHouse {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd, unused / không dùng
  // args(2): toDate yyyy-MM-dd, excluded: the hot window ends the day before / không bao gồm: cửa sổ hot kết thúc ngày trước đó
  // args(3): optional reports, e.g. "financial_report,fund_report"; missing or empty = every report
  //          báo cáo tuỳ chọn; thiếu hoặc rỗng = mọi báo cáo
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("PublishReportToClickHouse", args(0)).getOrCreate()
    process(spark, args(0), args(2), args.lift(3).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, environment: String, toDate: String, reportNames: String): Unit = {
    DateUtils.parseDate(toDate, "toDate")
    // Report → days kept in ClickHouse / Báo cáo → số ngày giữ trên ClickHouse
    val hotDays = Seq(
      "financial_report" -> 30,
      "financial_report_detail" -> 30,
      "financial_report_product" -> 30,
      "financial_report_order" -> 7,
      "fund_report" -> 30,
      "fund_report_shop" -> 30,
      "fund_report_order" -> 7,
      "inventory_health" -> 30,
      "shop_capacity_utilization" -> 30,
      "carrier_scorecard" -> 90,
      "supplier_performance" -> 365
    )
    val requested = SourceTableCatalog.splitList(reportNames)
    val unknown = requested.filterNot(name => hotDays.exists(_._1 == name))
    require(unknown.isEmpty, s"unknown reports ${unknown.mkString(", ")}, known: ${hotDays.map(_._1).mkString(", ")}")
    val chosen = if (requested.isEmpty) hotDays else hotDays.filter { case (name, _) => requested.contains(name) }
    val clickHouse = ConfigUtils.clickHouse(environment)

    chosen.foreach { case (report, days) =>
      // ClickHouse report columns are not nullable / Cột báo cáo ClickHouse không cho null
      val hotRows = spark.table(s"nessie.serving.$report")
        .where(col("ReportDate") >= date_sub(to_date(lit(toDate)), days) && col("ReportDate") < to_date(lit(toDate)))
        .na.fill("")
        .na.fill(0)

      ClickHouseSink.truncate(clickHouse, s"report.$report")
      ClickHouseSink.append(hotRows, clickHouse, s"report.$report")
      println(s"report.$report: replaced by the last $days days before $toDate")
    }
  }
}
