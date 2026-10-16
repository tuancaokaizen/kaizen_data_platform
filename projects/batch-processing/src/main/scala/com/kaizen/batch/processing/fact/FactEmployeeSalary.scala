package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, countDistinct, greatest, lit, round, sum, trunc}

/** nessie.dw.fact_employee_salary: one row per employee salary month: base salary plus commission on the net sales
  * of the employee in that month. Built from nessie.dw.fact_sale_transaction and fact_return_transaction, so it runs
  * after them. Rebuilt for the months touching [fromDate, toDate), the salary rows changed in the window and the
  * months of sales or returns changed in the window.
  * CommissionRate is a percent; CommissionAmount = round(NetSalesAmount × CommissionRate / 100), never negative.
  * Gift items are left out.
  * nessie.dw.fact_employee_salary: mỗi dòng một tháng lương của nhân viên: lương cơ bản cộng hoa hồng trên doanh số
  * thuần của nhân viên trong tháng. Dựng từ nessie.dw.fact_sale_transaction và fact_return_transaction nên chạy sau
  * chúng. Dựng lại cho các tháng chạm [fromDate, toDate), các dòng lương thay đổi trong cửa sổ và các tháng có bán
  * hoặc trả hàng thay đổi trong cửa sổ.
  * CommissionRate là phần trăm; CommissionAmount = round(NetSalesAmount × CommissionRate / 100), không âm.
  * Dòng quà tặng không được tính.
  */
object FactEmployeeSalary {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactEmployeeSalary", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val salary = spark.table("nessie.fnd.employee_salary")
      .withColumn("MonthDate", trunc(col("SalaryMonth").cast("date"), "month"))
    // Written as a comparison: Spark 3.4 fails to push a bare boolean expression down to Iceberg
    // Viết dạng so sánh: Spark 3.4 lỗi khi đẩy biểu thức boolean trần xuống Iceberg
    val sale = spark.table("nessie.dw.fact_sale_transaction")
      .where(col("IsGift").isNull || col("IsGift") === false)
      .withColumn("MonthDate", trunc(col("OrderDate"), "month"))
    val saleReturn = spark.table("nessie.dw.fact_return_transaction")
      .where(col("IsGift").isNull || col("IsGift") === false)
      .withColumn("MonthDate", trunc(col("ReturnDate"), "month"))

    // Months that start before toDate and end on or after fromDate / Tháng bắt đầu trước toDate và kết thúc từ fromDate
    val monthInRange = col("MonthDate") >= trunc(lit(fromDate).cast("date"), "month") && col("MonthDate") < lit(toDate).cast("date")
    val changedSales = sale
      .where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("EmployeeSaleId").as("EmployeeId"), col("MonthDate"))
      .union(
        saleReturn
          .where(DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("EmployeeSaleId").as("EmployeeId"), col("MonthDate"))
      )
      .distinct()
    val changedSalary = salary
      .where(monthInRange || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .unionByName(salary.join(changedSales, Seq("EmployeeId", "MonthDate"), "left_semi"))
      .dropDuplicates("Id")

    val employeeMonth = changedSalary.select(col("EmployeeId"), col("MonthDate")).distinct()
    val salesTotal = sale
      .withColumnRenamed("EmployeeSaleId", "EmployeeId")
      .join(employeeMonth, Seq("EmployeeId", "MonthDate"))
      .groupBy("EmployeeId", "MonthDate")
      .agg(
        sum(col("LineAmount")).cast("long").as("SalesAmount"),
        countDistinct(col("OrderId")).cast("int").as("SalesOrderCount")
      )
    val returnTotal = saleReturn
      .withColumnRenamed("EmployeeSaleId", "EmployeeId")
      .join(employeeMonth, Seq("EmployeeId", "MonthDate"))
      .groupBy("EmployeeId", "MonthDate")
      .agg(sum(col("LineAmount")).cast("long").as("ReturnAmount"))

    val netSales = coalesce(col("SalesAmount"), lit(0L)) - coalesce(col("ReturnAmount"), lit(0L))
    val commission = greatest(round(netSales * coalesce(col("CommissionRate"), lit(0)) / 100), lit(0)).cast("long")
    val factEmployeeSalary = changedSalary
      .join(salesTotal, Seq("EmployeeId", "MonthDate"), "left")
      .join(returnTotal, Seq("EmployeeId", "MonthDate"), "left")
      .select(
        col("Id").as("EmployeeSalaryId"),
        col("EmployeeId"),
        col("ShopCode"),
        col("MonthDate").cast("date"),
        col("BaseSalary").cast("long"),
        col("CommissionRate").cast("int"),
        coalesce(col("SalesOrderCount"), lit(0)).as("SalesOrderCount"),
        coalesce(col("SalesAmount"), lit(0L)).as("SalesAmount"),
        coalesce(col("ReturnAmount"), lit(0L)).as("ReturnAmount"),
        netSales.cast("long").as("NetSalesAmount"),
        commission.as("CommissionAmount"),
        (coalesce(col("BaseSalary"), lit(0L)) + commission).cast("long").as("TotalSalary"),
        col("ModifiedDate").cast("timestamp")
      )

    SparkUtils.mergeInto(
      spark,
      factEmployeeSalary,
      "nessie.dw.fact_employee_salary",
      keys = Seq("EmployeeSalaryId"),
      partition = Partition.Months("MonthDate")
    )
  }
}
