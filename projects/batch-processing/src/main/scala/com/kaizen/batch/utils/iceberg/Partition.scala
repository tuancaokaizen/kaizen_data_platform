package com.kaizen.batch.utils.iceberg

/** Iceberg partition of a table: none or one transform on one column.
  * Partition Iceberg của bảng: không có hoặc một phép biến đổi trên một cột.
  */
sealed abstract class Partition {

  /** Column the transform reads, if any. / Cột mà phép biến đổi đọc, nếu có. */
  def column: Option[String]

  /** Clause inside PARTITIONED BY (...). / Mệnh đề trong PARTITIONED BY (...). */
  def sql: Option[String]

  /** Transform name as Iceberg reports it (month, day, identity).
    * Tên phép biến đổi như Iceberg trả về (month, day, identity).
    */
  def icebergTransform: Option[String]
}

object Partition {
  case object Unpartitioned extends Partition {
    def column: Option[String] = None
    def sql: Option[String] = None
    def icebergTransform: Option[String] = None
    override def toString: String = "none"
  }

  final case class Months(name: String) extends Partition {
    def column: Option[String] = Some(name)
    def sql: Option[String] = Some(s"months(`$name`)")
    def icebergTransform: Option[String] = Some("month")
    override def toString: String = s"months($name)"
  }

  final case class Days(name: String) extends Partition {
    def column: Option[String] = Some(name)
    def sql: Option[String] = Some(s"days(`$name`)")
    def icebergTransform: Option[String] = Some("day")
    override def toString: String = s"days($name)"
  }

  final case class Identity(name: String) extends Partition {
    def column: Option[String] = Some(name)
    def sql: Option[String] = Some(s"`$name`")
    def icebergTransform: Option[String] = Some("identity")
    override def toString: String = s"identity($name)"
  }

  private val TransformPattern = """(months|days|identity)\(\s*([A-Za-z][A-Za-z0-9]*)\s*\)""".r

  /** Parses none | months(Column) | days(Column) | identity(Column).
    * Đọc none | months(Cột) | days(Cột) | identity(Cột).
    */
  def parse(text: String): Either[String, Partition] = text.trim match {
    case "none"                                => Right(Unpartitioned)
    case TransformPattern("months", columnName)   => Right(Months(columnName))
    case TransformPattern("days", columnName)     => Right(Days(columnName))
    case TransformPattern("identity", columnName) => Right(Identity(columnName))
    case other                                 => Left(s"unknown partition '$other'")
  }
}
