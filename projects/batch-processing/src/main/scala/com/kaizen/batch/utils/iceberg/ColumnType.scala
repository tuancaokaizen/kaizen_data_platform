package com.kaizen.batch.utils.iceberg

import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  TimestampType
}

/** Column type of a lakehouse table, written as text in tables.yaml and schema/.
  * Kiểu cột của bảng lakehouse, viết dạng chữ trong tables.yaml và schema/.
  */
sealed abstract class ColumnType(val sqlName: String) {
  def sparkType: DataType
  def isTemporal: Boolean = false
}

object ColumnType {
  case object StringValue extends ColumnType("string") {
    def sparkType: DataType = StringType
  }
  case object IntegerValue extends ColumnType("int") {
    def sparkType: DataType = IntegerType
  }
  case object LongValue extends ColumnType("long") {
    def sparkType: DataType = LongType
  }
  // Only for report measures that ClickHouse reads as Float64 / Chỉ cho chỉ số báo cáo mà ClickHouse đọc dạng Float64
  case object DoubleValue extends ColumnType("double") {
    def sparkType: DataType = DoubleType
  }
  case object BooleanValue extends ColumnType("boolean") {
    def sparkType: DataType = BooleanType
  }
  case object DateValue extends ColumnType("date") {
    def sparkType: DataType = DateType
    override def isTemporal: Boolean = true
  }
  case object TimestampValue extends ColumnType("timestamp") {
    def sparkType: DataType = TimestampType
    override def isTemporal: Boolean = true
  }
  final case class DecimalValue(precision: Int, scale: Int) extends ColumnType(s"decimal($precision,$scale)") {
    def sparkType: DataType = DecimalType(precision, scale)
  }

  private val DecimalPattern = """decimal\((\d+),\s*(\d+)\)""".r

  /** Parses string | int | long | double | boolean | date | timestamp | decimal(p,s).
    * Đọc string | int | long | double | boolean | date | timestamp | decimal(p,s).
    */
  def parse(text: String): Either[String, ColumnType] = text.trim.toLowerCase match {
    case "string"    => Right(StringValue)
    case "int"       => Right(IntegerValue)
    case "long"      => Right(LongValue)
    case "double"    => Right(DoubleValue)
    case "boolean"   => Right(BooleanValue)
    case "date"      => Right(DateValue)
    case "timestamp" => Right(TimestampValue)
    case DecimalPattern(precision, scale) if scale.toInt <= precision.toInt && precision.toInt <= 38 =>
      Right(DecimalValue(precision.toInt, scale.toInt))
    case other => Left(s"unknown column type '$other'")
  }

  /** Matches the type Spark reports for an existing table column.
    * So khớp với kiểu Spark trả về cho cột của bảng đã có.
    */
  def matches(columnType: ColumnType, existing: DataType): Boolean = columnType.sparkType == existing
}
