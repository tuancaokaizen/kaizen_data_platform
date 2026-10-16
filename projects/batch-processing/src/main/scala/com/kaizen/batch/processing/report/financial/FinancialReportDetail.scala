package com.kaizen.batch.processing.report.financial

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit, sum, when}

/** nessie.serving.financial_report_detail: profit per day, shop and product. Runs first, FinancialReport and
  * FinancialReportProduct roll up from it.
  * Sales count on the order date, returns on the return date. Gift items are not revenue: their cost is the
  * promotion cost of the shop, shared between the products sold that day by gross revenue.
  * nessie.serving.financial_report_detail: lãi lỗ theo ngày, cửa hàng và sản phẩm. Chạy trước, FinancialReport và
  * FinancialReportProduct cộng dồn từ bảng này.
  * Bán tính theo ngày đơn, trả theo ngày trả. Dòng quà tặng không phải doanh thu: giá vốn của nó là chi phí
  * khuyến mãi của cửa hàng, chia cho các sản phẩm bán trong ngày theo doanh thu gộp.
  */
object FinancialReportDetail {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FinancialReportDetail", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)

    val soldItem = spark.table("nessie.dw.fact_sale_transaction")
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate))
      .select(
        col("OrderDate").as("ReportDate"),
        col("ShopCode"),
        col("ProductId"),
        col("IsGift"),
        col("Quantity").as("SalesQuantity"),
        lit(0).as("ReturnQuantity"),
        col("LineAmount").as("GrossRevenue"),
        lit(0L).as("ReturnAmount"),
        col("CostAmount").as("Cost")
      )
    // A return gives the cost back / Hàng trả hoàn lại giá vốn
    val returnedItem = spark.table("nessie.dw.fact_return_transaction")
      .where(DateUtils.inDateRange(col("ReturnDate"), fromDate, toDate))
      .select(
        col("ReturnDate").as("ReportDate"),
        col("ShopCode"),
        col("ProductId"),
        col("IsGift"),
        lit(0).as("SalesQuantity"),
        col("Quantity").as("ReturnQuantity"),
        lit(0L).as("GrossRevenue"),
        col("LineAmount").as("ReturnAmount"),
        (-col("CostAmount")).as("Cost")
      )
    val item = soldItem.unionByName(returnedItem)

    val productDay = item
      .where(!col("IsGift"))
      .groupBy("ReportDate", "ShopCode", "ProductId")
      .agg(
        sum("SalesQuantity").as("SalesQuantity"),
        sum("ReturnQuantity").as("ReturnQuantity"),
        sum("GrossRevenue").as("GrossRevenue"),
        sum("ReturnAmount").as("ReturnAmount"),
        sum("Cost").as("TotalCogs")
      )
    val shopPromotion = item
      .where(col("IsGift"))
      .groupBy("ReportDate", "ShopCode")
      .agg(sum("Cost").as("ShopPromotionCost"))

    // Share the shop promotion cost by gross revenue / Chia chi phí khuyến mãi của cửa hàng theo doanh thu gộp
    val shopDay = Window.partitionBy("ReportDate", "ShopCode")
    val withPromotion = productDay
      .join(shopPromotion, Seq("ReportDate", "ShopCode"), "left")
      .withColumn("ShopGrossRevenue", sum("GrossRevenue").over(shopDay))
      .withColumn(
        "TotalPromotionCost",
        when(col("ShopGrossRevenue") > 0, coalesce(col("ShopPromotionCost"), lit(0L)) * col("GrossRevenue") / col("ShopGrossRevenue"))
          .otherwise(lit(0.0))
      )

    val shop = spark.table("nessie.dw.dim_shop").select("ShopCode", "ShopName")
    val product = spark.table("nessie.dw.dim_product").select("ProductId", "ProductName", "ProductLineCode", "ProductLineName")

    val financialReportDetail = withPromotion
      .join(broadcast(shop), Seq("ShopCode"), "left")
      .join(broadcast(product), Seq("ProductId"), "left")
      .withColumn("ShopName", coalesce(col("ShopName"), lit("Unknown")))
      .withColumn("ProductName", coalesce(col("ProductName"), lit("Unknown")))
      .withColumn("ProductLineCode", coalesce(col("ProductLineCode"), lit("Unknown")))
      .withColumn("ProductLineName", coalesce(col("ProductLineName"), lit("Không xác định")))
      .transform(rows => FinancialMeasures.complete(
        rows,
        Seq("ReportDate", "ShopCode", "ShopName", "ProductId", "ProductName", "ProductLineCode", "ProductLineName")
      ))

    SparkUtils.replaceDates(
      spark,
      financialReportDetail,
      "nessie.serving.financial_report_detail",
      dateColumn = "ReportDate",
      fromDate = fromDate,
      toDate = toDate,
      partition = Partition.Months("ReportDate")
    )
  }
}
