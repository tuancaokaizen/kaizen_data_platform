package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, greatest, lit}

/** nessie.dw.fact_return_transaction: one row per returned item of the returns made or changed in [fromDate, toDate).
  * Amounts are positive, the reports subtract them. IsGift comes from the sold item,
  * so a returned gift reverses promotion cost instead of revenue.
  * nessie.dw.fact_return_transaction: mỗi dòng một dòng hàng trả của các phiếu trả lập hoặc thay đổi trong [fromDate, toDate).
  * Số tiền dương, báo cáo tự trừ. IsGift lấy từ dòng bán,
  * nên quà tặng bị trả giảm chi phí khuyến mãi chứ không giảm doanh thu.
  */
object FactReturnTransaction {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactReturnTransaction", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val orderReturn = spark.table("nessie.fnd.return")
    val returnItem = spark.table("nessie.fnd.return_item")

    // A return is rebuilt when its header or one of its items was made or changed in the window
    // Dựng lại phiếu trả khi đầu phiếu hoặc một dòng được lập hoặc thay đổi trong cửa sổ
    val changedReturn = orderReturn
      .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("ReturnId"))
      .union(
        returnItem
          .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("ReturnId"))
      )
      .distinct()

    val returnHeader = orderReturn.select(
      col("Id").as("ReturnId"),
      col("ShopCode"),
      col("Reason"),
      col("ModifiedDate").as("ReturnModifiedDate")
    )
    val soldOrder = spark.table("nessie.fnd.order").select(col("Id").as("OrderId"), col("CustomerId"), col("EmployeeSaleId"))
    val soldItem = spark.table("nessie.fnd.order_item").select(col("Id").as("OrderItemId"), col("IsGift"))

    val factReturnTransaction = returnItem
      .join(changedReturn, Seq("ReturnId"))
      .join(returnHeader, Seq("ReturnId"))
      .join(soldOrder, Seq("OrderId"), "left")
      .join(soldItem, Seq("OrderItemId"), "left")
      .select(
        col("Id").as("ReturnItemId"),
        col("ReturnId"),
        col("OrderId"),
        col("OrderItemId"),
        col("ReturnDate").cast("date"),
        col("ShopCode"),
        col("CustomerId"),
        col("EmployeeSaleId"),
        col("ProductId"),
        coalesce(col("IsGift").cast("boolean"), lit(false)).as("IsGift"),
        col("Quantity").cast("int"),
        col("RestockQty").cast("int").as("RestockQuantity"),
        col("DamageQty").cast("int").as("DamageQuantity"),
        col("UnitPrice").cast("long"),
        col("LineAmount").cast("long"),
        col("UnitCost").cast("long"),
        (col("Quantity") * col("UnitCost")).cast("long").as("CostAmount"),
        col("Reason"),
        greatest(col("ReturnModifiedDate"), col("ModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factReturnTransaction,
      "nessie.dw.fact_return_transaction",
      keys = Seq("ReturnItemId"),
      partition = Partition.Months("ReturnDate")
    )
  }
}
