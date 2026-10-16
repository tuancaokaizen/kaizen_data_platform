package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, countDistinct, lit, max, min, months_between, sum, trunc, when}

/** nessie.dw.fact_customer_monthly: one row per customer and month with purchases (periodic snapshot), built from
  * nessie.dw.fact_order_transaction, so it runs after that fact. Rebuilt for the (customer, month) pairs of the orders
  * sold or changed in [fromDate, toDate). FirstOrderMonth is the month of the first order ever, for cohorts.
  * Walk-in orders without a customer are left out.
  * nessie.dw.fact_customer_monthly: mỗi dòng một khách hàng và một tháng có mua (periodic snapshot), dựng từ
  * nessie.dw.fact_order_transaction nên chạy sau fact đó. Dựng lại cho các cặp (khách, tháng) của các đơn bán hoặc
  * thay đổi trong [fromDate, toDate). FirstOrderMonth là tháng của đơn đầu tiên, dùng cho phân tích cohort.
  * Đơn khách vãng lai không có mã khách bị bỏ qua.
  */
object FactCustomerMonthly {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactCustomerMonthly", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val customerOrder = spark.table("nessie.dw.fact_order_transaction")
      .where(col("CustomerId").isNotNull)
      .withColumn("MonthDate", trunc(col("OrderDate"), "month"))

    val changedMonth = customerOrder
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("CustomerId"), col("MonthDate"))
      .distinct()

    val firstOrderMonth = customerOrder
      .join(changedMonth.select(col("CustomerId")).distinct(), Seq("CustomerId"))
      .groupBy("CustomerId")
      .agg(min(col("MonthDate")).as("FirstOrderMonth"))

    val monthTotal = customerOrder
      .join(changedMonth, Seq("CustomerId", "MonthDate"))
      .groupBy("CustomerId", "MonthDate")
      .agg(
        count(lit(1)).cast("int").as("OrderCount"),
        sum(when(col("IsReturned"), 1).otherwise(0)).cast("int").as("ReturnedOrderCount"),
        sum(when(col("HasVoucher"), 1).otherwise(0)).cast("int").as("VoucherOrderCount"),
        countDistinct(col("ShopCode")).cast("int").as("ShopCount"),
        sum(col("Quantity")).cast("long").as("Quantity"),
        sum(col("GrossAmount")).cast("long").as("GrossAmount"),
        sum(col("DiscountAmount")).cast("long").as("DiscountAmount"),
        sum(col("NetAmount")).cast("long").as("NetAmount"),
        sum(col("ReturnAmount")).cast("long").as("ReturnAmount"),
        sum(col("NetAfterReturnAmount")).cast("long").as("NetAfterReturnAmount"),
        sum(col("CostAmount")).cast("long").as("CostAmount"),
        min(col("OrderDate")).as("FirstOrderDate"),
        max(col("OrderDate")).as("LastOrderDate"),
        max(col("ModifiedDate")).as("ModifiedDate")
      )

    val factCustomerMonthly = monthTotal
      .join(firstOrderMonth, Seq("CustomerId"))
      .select(
        col("CustomerId"),
        col("MonthDate").cast("date"),
        col("OrderCount"),
        col("ReturnedOrderCount"),
        col("VoucherOrderCount"),
        col("ShopCount"),
        col("Quantity"),
        col("GrossAmount"),
        col("DiscountAmount"),
        col("NetAmount"),
        col("ReturnAmount"),
        col("NetAfterReturnAmount"),
        col("CostAmount"),
        (col("NetAmount") / col("OrderCount")).cast("double").as("AverageOrderValue"),
        col("FirstOrderDate").cast("date"),
        col("LastOrderDate").cast("date"),
        col("FirstOrderMonth").cast("date"),
        months_between(col("MonthDate"), col("FirstOrderMonth")).cast("int").as("MonthsSinceFirstOrder"),
        (col("MonthDate") === col("FirstOrderMonth")).as("IsNewCustomer"),
        col("ModifiedDate").cast("timestamp")
      )

    SparkUtils.mergeInto(
      spark,
      factCustomerMonthly,
      "nessie.dw.fact_customer_monthly",
      keys = Seq("CustomerId", "MonthDate"),
      partition = Partition.Months("MonthDate")
    )
  }
}
