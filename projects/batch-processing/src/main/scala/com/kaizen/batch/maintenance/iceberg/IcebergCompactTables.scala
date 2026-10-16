package com.kaizen.batch.maintenance.iceberg

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** Merges the small files left by daily MERGE and DELETE writes into files of about 128 MB.
  * Gộp file nhỏ do MERGE và DELETE hằng ngày để lại thành file khoảng 128 MB.
  */
object IcebergCompactTables {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): optional namespaces, e.g. "fnd,dw"; missing or empty = fnd, dw, serving (stg is rewritten every run)
  //          namespace tuỳ chọn; thiếu hoặc rỗng = fnd, dw, serving (stg được ghi lại mỗi lần chạy)
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("IcebergCompactTables", args(0)).getOrCreate()
    process(spark, args.lift(3).getOrElse(""))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, namespaces: String): Unit = {
    val known = Seq("fnd", "dw", "serving")
    val targetFileBytes = 128L * 1024L * 1024L
    val requested = SourceTableCatalog.splitList(namespaces)
    val unknown = requested.filterNot(known.contains)
    require(unknown.isEmpty, s"unknown namespaces ${unknown.mkString(", ")}, known: ${known.mkString(", ")}")
    val chosen = if (requested.isEmpty) known else requested

    chosen.foreach { namespace =>
      SparkUtils.listTables(spark, namespace).foreach(table => SparkUtils.compactTable(spark, namespace, table, targetFileBytes))
    }
  }
}
