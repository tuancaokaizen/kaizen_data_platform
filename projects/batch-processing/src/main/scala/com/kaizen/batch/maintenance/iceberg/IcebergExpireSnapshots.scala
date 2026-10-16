package com.kaizen.batch.maintenance.iceberg

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** Expires old snapshots of every table in the lakehouse namespaces, so MinIO keeps only the files still needed.
  * A failure on one table is reported and the other tables still run; the job fails at the end.
  * Xoá snapshot cũ của mọi bảng trong các namespace lakehouse, để MinIO chỉ giữ file còn cần.
  * Lỗi ở một bảng được báo và các bảng khác vẫn chạy; job báo lỗi ở cuối.
  */
object IcebergExpireSnapshots {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): optional namespaces, e.g. "stg,fnd"; missing or empty = stg, fnd, dw, serving
  //          namespace tuỳ chọn; thiếu hoặc rỗng = stg, fnd, dw, serving
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("IcebergExpireSnapshots", args(0)).getOrCreate()
    process(spark, args.lift(3).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, namespaces: String): Unit = {
    // Namespace → (snapshots kept at least, days kept) / Namespace → (số snapshot giữ tối thiểu, số ngày giữ)
    val retention = Seq(
      "stg" -> (1, 1),
      "fnd" -> (3, 2),
      "dw" -> (5, 7),
      "serving" -> (3, 3)
    )
    val requested = SourceTableCatalog.splitList(namespaces)
    val unknown = requested.filterNot(name => retention.exists(_._1 == name))
    require(unknown.isEmpty, s"unknown namespaces ${unknown.mkString(", ")}, known: ${retention.map(_._1).mkString(", ")}")
    val chosen = if (requested.isEmpty) retention else retention.filter { case (name, _) => requested.contains(name) }

    val failed = chosen.flatMap { case (namespace, (retainLast, olderThanDays)) =>
      SparkUtils.listTables(spark, namespace).flatMap { table =>
        try {
          SparkUtils.expireSnapshots(spark, namespace, table, retainLast, olderThanDays)
          None
        } catch {
          case error: Exception =>
            System.err.println(s"$namespace.$table: expire snapshots failed: ${error.getMessage}")
            Some(s"$namespace.$table")
        }
      }
    }
    require(failed.isEmpty, s"expire snapshots failed for ${failed.mkString(", ")}")
  }
}
