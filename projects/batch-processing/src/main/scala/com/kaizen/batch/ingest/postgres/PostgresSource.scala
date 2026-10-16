package com.kaizen.batch.ingest.postgres

import com.kaizen.batch.config.{LoadMode, SourceTable}
import com.kaizen.batch.utils.DateUtils
import com.kaizen.batch.utils.iceberg.ColumnType
import com.kaizen.batch.utils.jdbc.JdbcOptions
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Source: reads the declared columns of a Postgres table over JDBC, the whole table or the rows of the load window.
  * Source: đọc các cột đã khai báo của bảng Postgres qua JDBC, cả bảng hoặc các dòng trong cửa sổ nạp.
  */
object PostgresSource {
  private val TimestampText = "YYYY-MM-DD HH24:MI:SS.US"

  def read(spark: SparkSession, postgres: JdbcOptions, table: SourceTable, fromDate: String, toDate: String): DataFrame = {
    val sourceTypes = sourceColumnTypes(spark, postgres, table)
    val query = selectQuery(table, sourceTypes, fromDate, toDate)
    val rows = spark.read.format("jdbc").options(postgres.readOptions(s"($query) AS source_rows")).load()
    toDeclaredTypes(rows, table)
  }

  /** SELECT run on Postgres. Timestamps leave Postgres as Vietnam wall-clock text, so no JDBC time zone can shift them.
    * Câu SELECT chạy trên Postgres. Timestamp rời Postgres dưới dạng chữ giờ Việt Nam để múi giờ JDBC không làm lệch.
    */
  def selectQuery(table: SourceTable, sourceTypes: Map[String, String], fromDate: String, toDate: String): String = {
    val missing = table.columns.map(_.name).filterNot(sourceTypes.contains)
    require(
      missing.isEmpty,
      s"${table.postgresName} has no column ${missing.mkString(", ")}: fix tables.yaml or the Postgres schema"
    )
    val incompatible = table.columns.filterNot(column => accepts(column.columnType, sourceTypes(column.name)))
    require(
      incompatible.isEmpty,
      s"${table.postgresName}: declared types do not fit Postgres: " +
        incompatible.map(column => s"${column.name} ${column.columnType.sqlName} vs ${sourceTypes(column.name)}").mkString(", ")
    )

    val projections = table.columns.map { column =>
      val name = column.name
      sourceTypes(name) match {
        case "timestamptz" => s"""to_char("$name" AT TIME ZONE '${DateUtils.VietnamZoneName}', '$TimestampText') AS "$name""""
        case "timestamp"   => s"""to_char("$name", '$TimestampText') AS "$name""""
        case _             => s""""$name""""
      }
    }
    val filter = table.load match {
      case LoadMode.Incremental =>
        val modified = table.modifiedColumn.get
        val start = DateUtils.postgresTimestamp(DateUtils.windowStart(fromDate))
        val end = DateUtils.postgresTimestamp(DateUtils.windowEnd(toDate))
        s""" WHERE "$modified" >= '$start'::timestamptz AND "$modified" < '$end'::timestamptz"""
      case LoadMode.Full => ""
    }
    s"SELECT ${projections.mkString(", ")} FROM ${table.postgresName}$filter"
  }

  /** Casts JDBC columns to the declared types, in declared order. / Ép cột JDBC về kiểu khai báo, đúng thứ tự khai báo. */
  def toDeclaredTypes(rows: DataFrame, table: SourceTable): DataFrame =
    rows.select(table.columns.map { column =>
      val value = col(s"`${column.name}`")
      val typed =
        if (column.columnType == ColumnType.TimestampValue) to_timestamp(value)
        else value.cast(column.columnType.sparkType)
      typed.as(column.name)
    }: _*)

  // Column name → Postgres udt_name of the source table / Tên cột → udt_name Postgres của bảng nguồn
  private def sourceColumnTypes(spark: SparkSession, postgres: JdbcOptions, table: SourceTable): Map[String, String] = {
    val query =
      s"""(SELECT column_name, udt_name FROM information_schema.columns
         | WHERE table_schema = '${table.sourceSchema}' AND table_name = '${table.sourceTable}') AS source_columns""".stripMargin
    spark.read
      .format("jdbc")
      .options(postgres.readOptions(query))
      .load()
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
  }

  // Postgres udt_name accepted for each declared type / udt_name Postgres chấp nhận cho từng kiểu khai báo
  private def accepts(columnType: ColumnType, udtName: String): Boolean = columnType match {
    case ColumnType.StringValue        => Set("text", "varchar", "bpchar", "uuid")(udtName)
    case ColumnType.IntegerValue       => Set("int2", "int4")(udtName)
    case ColumnType.LongValue          => Set("int2", "int4", "int8")(udtName)
    case ColumnType.BooleanValue       => udtName == "bool"
    case ColumnType.DateValue          => udtName == "date"
    case ColumnType.TimestampValue     => Set("timestamptz", "timestamp")(udtName)
    case ColumnType.DecimalValue(_, _) => udtName == "numeric"
  }
}
