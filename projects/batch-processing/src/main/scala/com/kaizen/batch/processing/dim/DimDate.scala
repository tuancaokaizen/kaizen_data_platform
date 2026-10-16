package com.kaizen.batch.processing.dim

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_add, date_format, dayofmonth, dayofweek, lit, month, quarter, trunc, weekofyear, when, year}

/** nessie.dw.dim_date: one row per calendar day from FirstDate to LastDate, rebuilt every run.
  * DayOfWeek is ISO: 1 = Monday … 7 = Sunday.
  * nessie.dw.dim_date: mỗi dòng một ngày từ FirstDate đến LastDate, dựng lại mỗi lần chạy.
  * DayOfWeek theo ISO: 1 = thứ Hai … 7 = Chủ nhật.
  */
object DimDate {

  private val FirstDate = LocalDate.parse("2025-01-01")
  private val LastDate = LocalDate.parse("2030-12-31")

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: the calendar is fixed / không dùng: lịch cố định
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimDate", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val dayCount = ChronoUnit.DAYS.between(FirstDate, LastDate) + 1
    val days = spark.range(0, dayCount)
      .select(date_add(lit(FirstDate.toString).cast("date"), col("id").cast("int")).as("Date"))

    // Spark dayofweek is 1 = Sunday; shift it to ISO / dayofweek của Spark là 1 = Chủ nhật; đổi sang ISO
    val isoDayOfWeek = ((dayofweek(col("Date")) + 5) % 7) + 1

    val dimDate = days.select(
      date_format(col("Date"), "yyyyMMdd").cast("int").as("DateKey"),
      col("Date"),
      year(col("Date")).as("Year"),
      quarter(col("Date")).as("Quarter"),
      month(col("Date")).as("Month"),
      trunc(col("Date"), "month").as("MonthDate"),
      date_format(col("Date"), "yyyy-MM").as("YearMonth"),
      weekofyear(col("Date")).as("WeekOfYear"),
      dayofmonth(col("Date")).as("DayOfMonth"),
      isoDayOfWeek.as("DayOfWeek"),
      when(isoDayOfWeek === 1, "Thứ Hai")
        .when(isoDayOfWeek === 2, "Thứ Ba")
        .when(isoDayOfWeek === 3, "Thứ Tư")
        .when(isoDayOfWeek === 4, "Thứ Năm")
        .when(isoDayOfWeek === 5, "Thứ Sáu")
        .when(isoDayOfWeek === 6, "Thứ Bảy")
        .otherwise("Chủ Nhật")
        .as("DayName"),
      (isoDayOfWeek >= 6).as("IsWeekend")
    )

    SparkUtils.overwriteTable(spark, dimDate, "nessie.dw.dim_date")
  }
}
