package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.utils.jdbc.{JdbcOptions, JdbcStatements}
import org.apache.spark.sql.DataFrame

/** Sink: ClickHouse tables written over JDBC. / Sink: bảng ClickHouse ghi qua JDBC. */
object ClickHouseSink {

  /** Empties a table. / Làm rỗng một bảng. */
  def truncate(clickHouse: JdbcOptions, table: String): Unit =
    JdbcStatements.execute(clickHouse, Seq(s"TRUNCATE TABLE IF EXISTS $table"))

  /** Appends rows; the DataFrame columns must have the names of the ClickHouse columns.
    * Thêm dòng; cột DataFrame phải trùng tên cột ClickHouse.
    */
  def append(rows: DataFrame, clickHouse: JdbcOptions, table: String): Unit =
    rows.write.format("jdbc").options(clickHouse.writeOptions(table)).mode("append").save()
}
