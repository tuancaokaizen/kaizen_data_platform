package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, broadcast, coalesce, col, count, desc, greatest, lit, row_number, when}

/** nessie.dw.fact_promotion_transaction: one row per order item that received a promotion, for the orders sold or
  * changed in [fromDate, toDate).
  *   VOUCHER: a sale item with a share of the order voucher (DiscountAmount > 0); the promotion comes from
  *            voucher_series → voucher, so it is exact.
  *   GIFT:    a gift item; Postgres does not store its promotion, so it is inferred from the gift offers of the product
  *            active at the order time, the latest started one first. IsPromotionInferred is true and
  *            PromotionCandidateCount tells how many offers matched (1 = unambiguous, 0 = none found).
  * PromotionCostAmount = DiscountAmount (revenue given up) + GiftCostAmount (cost of the goods given away).
  * nessie.dw.fact_promotion_transaction: mỗi dòng một dòng hàng được hưởng khuyến mãi, cho các đơn bán hoặc thay đổi
  * trong [fromDate, toDate).
  *   VOUCHER: dòng bán được phân bổ voucher của đơn (DiscountAmount > 0); chương trình lấy từ
  *            voucher_series → voucher nên chính xác.
  *   GIFT:    dòng quà tặng; Postgres không lưu chương trình, nên suy ra từ ưu đãi quà của sản phẩm đang chạy
  *            lúc mua, ưu tiên ưu đãi bắt đầu muộn nhất. IsPromotionInferred là true và PromotionCandidateCount
  *            cho biết số ưu đãi khớp (1 = rõ ràng, 0 = không tìm thấy).
  * PromotionCostAmount = DiscountAmount (doanh thu bỏ ra) + GiftCostAmount (giá vốn hàng tặng).
  */
object FactPromotionTransaction {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactPromotionTransaction", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    val order = spark.table("nessie.fnd.order")
    val orderItem = spark.table("nessie.fnd.order_item")

    // An order is rebuilt when its header or one of its items was sold or changed in the window
    // Dựng lại đơn khi đầu đơn hoặc một dòng hàng được bán hoặc thay đổi trong cửa sổ
    val changedOrder = order
      .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(col("Id").as("OrderId"))
      .union(
        orderItem
          .where(DateUtils.inDateRange(col("OrderDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
          .select(col("OrderId"))
      )
      .distinct()

    val orderHeader = order.select(
      col("Id").as("OrderId"),
      col("TransactionDate"),
      col("ShopCode"),
      col("CustomerId"),
      col("EmployeeSaleId"),
      col("VoucherSeriesCode"),
      col("Status").as("OrderStatus"),
      col("ModifiedDate").as("OrderModifiedDate")
    )

    // Written as a comparison: Spark 3.4 fails to push a bare boolean expression down to Iceberg
    // Viết dạng so sánh: Spark 3.4 lỗi khi đẩy biểu thức boolean trần xuống Iceberg
    val isGift = col("IsGift") === true
    val isSale = col("IsGift").isNull || col("IsGift") === false
    val orderLine = orderItem
      .join(changedOrder, Seq("OrderId"))
      .join(orderHeader, Seq("OrderId"))
      .withColumnRenamed("Id", "OrderItemId")

    val voucherPromotion = spark.table("nessie.fnd.voucher_series")
      .select(col("SeriesCode").as("VoucherSeriesCode"), col("VoucherCode"))
      .join(spark.table("nessie.fnd.voucher").select(col("Code").as("VoucherCode"), col("PromotionCode")), Seq("VoucherCode"), "left")

    val voucherLine = orderLine
      .where(isSale && col("VoucherSeriesCode").isNotNull && col("DiscountAmount") > 0)
      .join(broadcast(voucherPromotion), Seq("VoucherSeriesCode"), "left")
      .withColumn("PromotionType", lit("VOUCHER"))
      .withColumn("PromotionCandidateCount", when(col("PromotionCode").isNull, 0).otherwise(1))

    // Gift offers of the product active at the order time; the latest started one is taken
    // Ưu đãi quà của sản phẩm đang chạy lúc mua; lấy ưu đãi bắt đầu muộn nhất
    val giftOffer = spark.table("nessie.fnd.gift")
      .select(col("ProductId").as("OfferProductId"), col("PromotionCode"))
      .join(spark.table("nessie.fnd.promotion").select(col("Code").as("PromotionCode"), col("StartDate"), col("EndDate")), Seq("PromotionCode"))
    val giftLine = orderLine.where(isGift)
    val byItem = Window.partitionBy("OrderItemId")
    val giftPromotion = giftLine
      .select(col("OrderItemId"), col("ProductId"), col("TransactionDate"))
      .join(
        broadcast(giftOffer),
        col("ProductId") === col("OfferProductId") &&
          col("TransactionDate") >= col("StartDate") &&
          (col("EndDate").isNull || col("TransactionDate") <= col("EndDate"))
      )
      .withColumn("PromotionCandidateCount", count(lit(1)).over(byItem).cast("int"))
      .withColumn("OfferRank", row_number().over(byItem.orderBy(desc("StartDate"), asc("PromotionCode"))))
      .where(col("OfferRank") === 1)
      .select(col("OrderItemId"), col("PromotionCode"), col("PromotionCandidateCount"))
    val giftPromotionLine = giftLine
      .join(giftPromotion, Seq("OrderItemId"), "left")
      .withColumn("PromotionType", lit("GIFT"))
      .withColumn("VoucherCode", lit(null).cast("string"))
      .withColumn("PromotionCandidateCount", coalesce(col("PromotionCandidateCount"), lit(0)))

    val giftCost = when(col("PromotionType") === "GIFT", col("Quantity") * col("UnitCost")).otherwise(0)
    val factPromotionTransaction = voucherLine
      .unionByName(giftPromotionLine.select(voucherLine.columns.map(col): _*))
      .select(
        col("OrderItemId"),
        col("OrderId"),
        col("OrderDate").cast("date"),
        col("TransactionDate").cast("timestamp"),
        col("ShopCode"),
        col("CustomerId"),
        col("EmployeeSaleId"),
        col("ProductId"),
        col("PromotionType"),
        col("PromotionCode"),
        col("VoucherSeriesCode"),
        col("VoucherCode"),
        (col("PromotionType") === "GIFT").as("IsPromotionInferred"),
        col("PromotionCandidateCount").cast("int"),
        col("Quantity").cast("int"),
        (col("Quantity") * col("UnitPrice")).cast("long").as("GrossAmount"),
        col("DiscountAmount").cast("long"),
        col("LineAmount").cast("long"),
        giftCost.cast("long").as("GiftCostAmount"),
        (col("DiscountAmount") + giftCost).cast("long").as("PromotionCostAmount"),
        col("OrderStatus"),
        greatest(col("OrderModifiedDate"), col("ModifiedDate")).cast("timestamp").as("ModifiedDate")
      )

    SparkUtils.mergeInto(
      spark,
      factPromotionTransaction,
      "nessie.dw.fact_promotion_transaction",
      keys = Seq("OrderItemId"),
      partition = Partition.Months("OrderDate")
    )
  }
}
