package com.kaizen.batch.testing

import java.sql.{Date, Timestamp}

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.utils.iceberg.ColumnType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/** Builds rows with the exact columns of a tables.yaml table; columns not given are null.
  * Dates and timestamps are written as text, e.g. "2026-10-14" and "2026-10-14 10:00:00".
  * Dựng dòng đúng các cột của một bảng trong tables.yaml; cột không truyền là null.
  * Ngày và timestamp viết dạng chữ, vd "2026-10-14" và "2026-10-14 10:00:00".
  */
object SourceRows {
  private lazy val catalog = SourceTableCatalog.load()

  def apply(spark: SparkSession, tableName: String, rows: Seq[Map[String, Any]]): DataFrame = {
    val table = catalog.table(tableName)
    val unknown = rows.flatMap(_.keys).distinct.filterNot(name => table.columns.exists(_.name == name))
    require(unknown.isEmpty, s"$tableName has no column ${unknown.mkString(", ")}")

    val schema = StructType(table.columns.map(column => StructField(column.name, column.columnType.sparkType, nullable = true)))
    val values = rows.map(row => Row.fromSeq(table.columns.map(column => convert(row.get(column.name), column.columnType))))
    spark.createDataFrame(spark.sparkContext.parallelize(values), schema)
  }

  // Scala test values → the JVM type Spark expects for the column / Giá trị test → kiểu JVM Spark cần cho cột
  private def convert(value: Option[Any], columnType: ColumnType): Any = (value, columnType) match {
    case (None | Some(null), _)                           => null
    case (Some(text: String), ColumnType.DateValue)       => Date.valueOf(text)
    case (Some(text: String), ColumnType.TimestampValue)  => Timestamp.valueOf(text)
    case (Some(number: Int), ColumnType.LongValue)        => number.toLong
    case (Some(number: Int), ColumnType.DoubleValue)      => number.toDouble
    case (Some(number: Double), ColumnType.DecimalValue(_, _)) => new java.math.BigDecimal(number.toString)
    case (Some(other), _)                                 => other
  }
}
