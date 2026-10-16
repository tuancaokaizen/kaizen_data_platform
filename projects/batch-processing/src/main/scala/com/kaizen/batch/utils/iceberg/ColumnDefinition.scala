package com.kaizen.batch.utils.iceberg

/** One column of a lakehouse table. / Một cột của bảng lakehouse. */
final case class ColumnDefinition(name: String, columnType: ColumnType) {
  def sql: String = s"`$name` ${columnType.sqlName}"
}
