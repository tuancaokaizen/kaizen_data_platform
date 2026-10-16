package com.kaizen.batch.utils

import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.TemporalAdjusters
import java.time.{Clock, DayOfWeek, LocalDate, LocalDateTime, ZoneId}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DateType, TimestampType}

/** Dates of a run and Vietnam time helpers.
  * Ngày của một lần chạy và hàm tiện ích giờ Việt Nam.
  *
  * Every job receives fromDate and toDate (yyyy-MM-dd) from Airflow: the days [fromDate, toDate), toDate excluded.
  * Mọi job nhận fromDate và toDate (yyyy-MM-dd) từ Airflow: các ngày [fromDate, toDate), không gồm toDate.
  *
  * Lakehouse convention: Spark runs in UTC and timestamps store Vietnam wall-clock digits,
  * so 08:30 in a shop reads 08:30 in Iceberg, Trino and ClickHouse.
  * Quy ước lakehouse: Spark chạy UTC và timestamp lưu đúng số giờ Việt Nam,
  * nên 08:30 ở cửa hàng đọc ra 08:30 trong Iceberg, Trino và ClickHouse.
  */
object DateUtils {
  val VietnamZoneName: String = "Asia/Ho_Chi_Minh"
  val VietnamZone: ZoneId = ZoneId.of(VietnamZoneName)
  val StorageZoneName: String = "UTC"
  val PostgresOffset: String = "+07"

  /** Rows modified this long before fromDate are read again, so rows committed late in the previous run are not lost.
    * Dòng sửa trong khoảng này trước fromDate được đọc lại, để dòng commit muộn ở lần chạy trước không bị sót.
    */
  val LookbackHours: Int = 1

  private val TimestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** Current business date in Vietnam. / Ngày nghiệp vụ hiện tại ở Việt Nam. */
  def today(clock: Clock = Clock.systemUTC()): LocalDate = LocalDate.now(clock.withZone(VietnamZone))

  /** Parses yyyy-MM-dd, naming the argument in the error. / Đọc yyyy-MM-dd, báo tên tham số khi lỗi. */
  def parseDate(text: String, argumentName: String): LocalDate =
    try LocalDate.parse(text.trim)
    catch {
      case _: DateTimeParseException => throw new IllegalArgumentException(s"$argumentName must be yyyy-MM-dd, got '$text'")
    }

  /** Fails unless fromDate < toDate. / Báo lỗi nếu không có fromDate < toDate. */
  def checkDateRange(fromDate: String, toDate: String): Unit = {
    val from = parseDate(fromDate, "fromDate")
    val to = parseDate(toDate, "toDate")
    require(from.isBefore(to), s"fromDate must be before toDate (toDate is excluded), got [$fromDate, $toDate)")
  }

  /** Wall-clock text without offset, e.g. 2026-10-15 00:00:00. / Chuỗi giờ không kèm offset. */
  def format(value: LocalDateTime): String = value.format(TimestampFormat)

  /** Start of the load window: fromDate 00:00 minus the lookback, e.g. 2026-10-14 23:00:00.
    * Đầu cửa sổ nạp: fromDate 00:00 trừ lookback, vd 2026-10-14 23:00:00.
    */
  def windowStart(fromDate: String): String =
    format(parseDate(fromDate, "fromDate").atStartOfDay().minusHours(LookbackHours.toLong))

  /** End of the load window (excluded): toDate 00:00. / Cuối cửa sổ nạp (không gồm): toDate 00:00. */
  def windowEnd(toDate: String): String = format(parseDate(toDate, "toDate").atStartOfDay())

  /** Postgres timestamptz literal in Vietnam time, e.g. 2026-10-14 23:00:00+07.
    * Literal timestamptz cho Postgres theo giờ Việt Nam.
    */
  def postgresTimestamp(wallClock: String): String = s"$wallClock$PostgresOffset"

  /** The business date is one of the days [fromDate, toDate), e.g. inDateRange(col("OrderDate"), fromDate, toDate).
    * Ngày nghiệp vụ thuộc các ngày [fromDate, toDate).
    */
  def inDateRange(column: Column, fromDate: String, toDate: String): Column =
    column >= lit(fromDate).cast(DateType) && column < lit(toDate).cast(DateType)

  /** The lakehouse timestamp is inside the load window [windowStart, windowEnd), lookback included.
    * Timestamp lakehouse thuộc cửa sổ nạp [windowStart, windowEnd), có tính lookback.
    */
  def inLoadWindow(column: Column, fromDate: String, toDate: String): Column =
    column >= lit(windowStart(fromDate)).cast(TimestampType) && column < lit(windowEnd(toDate)).cast(TimestampType)

  /** Monday on or before the date (ISO week). / Thứ Hai đúng hoặc trước ngày (tuần ISO). */
  def weekStart(date: String): LocalDate =
    parseDate(date, "date").`with`(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))

  /** Exclusive end of the weeks that overlap [fromDate, toDate): Monday after the last overlapping week.
    * Cuối (không gồm) các tuần giao [fromDate, toDate): thứ Hai sau tuần chồng cuối cùng.
    */
  def exclusiveWeekEnd(toDate: String): LocalDate =
    weekStart(parseDate(toDate, "toDate").minusDays(1).toString).plusWeeks(1)

  /** First day of the month of the date. / Ngày đầu tháng của ngày. */
  def monthStart(date: String): LocalDate =
    parseDate(date, "date").withDayOfMonth(1)

  /** Exclusive end of the months that overlap [fromDate, toDate): first day of the month after the last overlapping month.
    * Cuối (không gồm) các tháng giao [fromDate, toDate): ngày 1 của tháng sau tháng chồng cuối cùng.
    */
  def exclusiveMonthEnd(toDate: String): LocalDate =
    monthStart(parseDate(toDate, "toDate").minusDays(1).toString).plusMonths(1)
}
