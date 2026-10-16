package com.kaizen.batch.ingest.postgres

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.{ConfigUtils, DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, from_utc_timestamp, lit, to_timestamp}

/** Postgres information_system → nessie.stg.<table>: each run replaces the bronze table with the rows read this run.
  * Full tables are read whole, incremental tables only rows modified in the load window (fromDate 00:00 − 1h, toDate 00:00).
  * Postgres information_system → nessie.stg.<bảng>: mỗi lần chạy thay bảng bronze bằng các dòng đọc được lần này.
  * Bảng full đọc cả bảng, bảng incremental chỉ đọc dòng sửa trong cửa sổ nạp (fromDate 00:00 − 1h, toDate 00:00).
  */
object PostgresToBronze {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  // args(3): groups of tables.yaml, "all" or e.g. "master,sales" / nhóm bảng trong tables.yaml
  // args(4): optional table names, e.g. "order,order_item"; missing or empty = every table of the groups
  //          tên bảng tuỳ chọn; thiếu hoặc rỗng = mọi bảng của nhóm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("PostgresToBronze", args(0)).getOrCreate()
    process(spark, args(0), args(1), args(2), args(3), args.lift(4).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, environment: String, fromDate: String, toDate: String, groups: String, tableNames: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val tables = SourceTableCatalog.load().select(groups, tableNames)
    val postgres = ConfigUtils.postgres(environment, "kaizen-batch-PostgresToBronze")
    val batchId = spark.sparkContext.applicationId

    tables.foreach { table =>
      val rows = PostgresSource
        .read(spark, postgres, table, fromDate, toDate)
        // Load time in Vietnam wall-clock like every other timestamp / Giờ nạp theo giờ Việt Nam như mọi timestamp khác
        .withColumn("_IngestedAt", from_utc_timestamp(current_timestamp(), DateUtils.VietnamZoneName))
        .withColumn("_BatchId", lit(batchId))
        .withColumn("_WindowFrom", to_timestamp(lit(DateUtils.windowStart(fromDate))))
        .withColumn("_WindowTo", to_timestamp(lit(DateUtils.windowEnd(toDate))))

      // An incremental table may have no change in the window, so an empty bronze table is valid
      // Bảng incremental có thể không có thay đổi trong cửa sổ, nên bảng bronze rỗng là hợp lệ
      SparkUtils.overwriteTable(spark, rows, s"nessie.stg.${table.name}", allowEmpty = true)
      println(s"nessie.stg.${table.name}: ${table.load.name} load from ${table.postgresName} done")
    }
  }
}
