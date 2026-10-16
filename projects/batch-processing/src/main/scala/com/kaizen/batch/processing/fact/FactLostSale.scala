package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, greatest, lit}

/** nessie.dw.fact_lost_sale: one row per customer request the shop could not serve, for the days [fromDate, toDate)
  * and the rows changed in the window. LostQuantity = requested − substituted; LostRevenueEstimate values it at the
  * current retail price of the product.
  * nessie.dw.fact_lost_sale: mỗi dòng một yêu cầu của khách mà cửa hàng không đáp ứng được, cho các ngày
  * [fromDate, toDate) và các dòng thay đổi trong cửa sổ. LostQuantity = yêu cầu − đã thay thế; LostRevenueEstimate
  * tính theo giá bán lẻ hiện tại của sản phẩm.
  */
object FactLostSale {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactLostSale", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val retailPrice = spark.table("nessie.fnd.product").select(col("Id").as("ProductId"), col("RetailPrice"))

    val lostQuantity = greatest(col("RequestedQty") - coalesce(col("SubstitutedQty"), lit(0)), lit(0))
    val factLostSale = spark.table("nessie.fnd.lost_sale")
      .where(DateUtils.inDateRange(col("BusinessDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .join(broadcast(retailPrice), Seq("ProductId"), "left")
      .select(
        col("Id").as("LostSaleId"),
        col("BusinessDate").cast("date"),
        col("ShopCode"),
        col("ProductId"),
        col("SubstituteProductId"),
        col("RequestedQty").cast("int").as("RequestedQuantity"),
        coalesce(col("SubstitutedQty"), lit(0)).cast("int").as("SubstitutedQuantity"),
        lostQuantity.cast("int").as("LostQuantity"),
        col("RetailPrice").cast("long"),
        (lostQuantity * col("RetailPrice")).cast("long").as("LostRevenueEstimate"),
        col("Reason"),
        col("ModifiedDate").cast("timestamp")
      )

    SparkUtils.mergeInto(
      spark,
      factLostSale,
      "nessie.dw.fact_lost_sale",
      keys = Seq("LostSaleId"),
      partition = Partition.Months("BusinessDate")
    )
  }
}
