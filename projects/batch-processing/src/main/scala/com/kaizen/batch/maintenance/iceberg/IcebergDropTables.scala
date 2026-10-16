package com.kaizen.batch.maintenance.iceberg

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** Drops Kaizen lakehouse tables with their files, e.g. to rebuild a layer from Postgres. Manual use only, never scheduled.
  * Tables must be named one by one: there is no "drop everything".
  * Xoá bảng lakehouse của Kaizen cùng file dữ liệu, vd để dựng lại một tầng từ Postgres. Chỉ chạy tay, không lập lịch.
  * Phải ghi rõ từng bảng: không có lựa chọn "xoá hết".
  */
object IcebergDropTables {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused / không dùng
  // args(3): namespace, one of stg, fnd, dw, serving / một namespace trong stg, fnd, dw, serving
  // args(4): tables of that namespace, e.g. "fact_sales_line,fact_return_line" / các bảng của namespace đó
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("IcebergDropTables", args(0)).getOrCreate()
    process(spark, args(3), args(4))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, namespace: String, tableNames: String): Unit = {
    val known = Seq("stg", "fnd", "dw", "serving")
    require(known.contains(namespace), s"namespace must be one of ${known.mkString(", ")}, got '$namespace'")
    val tables = SourceTableCatalog.splitList(tableNames)
    require(tables.nonEmpty, "name the tables to drop one by one")
    val existing = SparkUtils.listTables(spark, namespace)
    val missing = tables.filterNot(existing.contains)
    require(missing.isEmpty, s"$namespace has no table ${missing.mkString(", ")}")

    tables.foreach { table =>
      SparkUtils.dropTable(spark, namespace, table)
      println(s"nessie.$namespace.$table dropped")
    }
  }
}
