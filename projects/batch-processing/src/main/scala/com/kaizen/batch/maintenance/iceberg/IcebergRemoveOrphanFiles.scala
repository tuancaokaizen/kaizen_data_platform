package com.kaizen.batch.maintenance.iceberg

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** Deletes files under the lakehouse tables that no snapshot references (failed writes, expired snapshots).
  * Only files older than 3 days are touched, so a running write never loses its files.
  * Xoá file trong thư mục bảng lakehouse mà không snapshot nào tham chiếu (lần ghi lỗi, snapshot đã xoá).
  * Chỉ đụng file cũ hơn 3 ngày, để lần ghi đang chạy không bao giờ mất file.
  */
object IcebergRemoveOrphanFiles {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): optional namespaces, e.g. "stg"; missing or empty = stg, fnd, dw, serving
  //          namespace tuỳ chọn; thiếu hoặc rỗng = stg, fnd, dw, serving
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("IcebergRemoveOrphanFiles", args(0)).getOrCreate()
    process(spark, args.lift(3).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, namespaces: String): Unit = {
    val known = Seq("stg", "fnd", "dw", "serving")
    val minimumAgeDays = 3
    val requested = SourceTableCatalog.splitList(namespaces)
    val unknown = requested.filterNot(known.contains)
    require(unknown.isEmpty, s"unknown namespaces ${unknown.mkString(", ")}, known: ${known.mkString(", ")}")
    val chosen = if (requested.isEmpty) known else requested

    chosen.foreach { namespace =>
      SparkUtils.listTables(spark, namespace).foreach(table => SparkUtils.removeOrphanFiles(spark, namespace, table, minimumAgeDays))
    }
  }
}
