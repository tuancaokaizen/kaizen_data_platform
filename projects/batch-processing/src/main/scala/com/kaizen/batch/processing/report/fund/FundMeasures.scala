package com.kaizen.batch.processing.report.fund

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, max, sum}

/** Fund report formulas, shared by every grain so the reports always agree.
  * Công thức báo cáo quỹ, dùng chung cho mọi grain để các báo cáo luôn khớp nhau.
  *
  *   NetCashFlow        = TotalReceipts - TotalExpenditures
  *   ReconciliationDiff = NetCashFlow - (TotalOrderAmount - TotalReturnAmount)
  *
  * Money is double like ClickHouse Float64. / Tiền là double như Float64 của ClickHouse.
  */
object FundMeasures {

  /** Payment.PaymentType of a receipt (sale) and of an expenditure (refund).
    * Payment.PaymentType của phiếu thu (bán) và phiếu chi (hoàn tiền).
    */
  val ReceiptTypeId: Int = 1
  val RefundTypeId: Int = 2

  /** Measures a report must have before `complete`. / Chỉ số một báo cáo phải có trước khi gọi `complete`. */
  val Base: Seq[String] = Seq("TotalReceipts", "TotalExpenditures", "TotalOrderAmount", "TotalReturnAmount")

  /** Measure columns of every fund report, in table order. / Cột chỉ số của mọi báo cáo quỹ, theo thứ tự bảng. */
  val Columns: Seq[String] =
    Seq("TotalReceipts", "TotalExpenditures", "NetCashFlow", "TotalOrderAmount", "TotalReturnAmount", "ReconciliationDiff")

  /** Adds the derived measures and returns dimensionColumns followed by Columns.
    * Thêm chỉ số dẫn xuất và trả về dimensionColumns rồi tới Columns.
    */
  def complete(rows: DataFrame, dimensionColumns: Seq[String]): DataFrame =
    Base
      .foldLeft(rows)((current, money) => current.withColumn(money, coalesce(col(money).cast("double"), lit(0.0))))
      .withColumn("NetCashFlow", col("TotalReceipts") - col("TotalExpenditures"))
      .withColumn("ReconciliationDiff", col("NetCashFlow") - (col("TotalOrderAmount") - col("TotalReturnAmount")))
      .select((dimensionColumns ++ Columns).map(col): _*)

  /** Sums a finer report per key, keeps the other dimension columns (one value per key), then recomputes the derived measures.
    * Cộng dồn báo cáo chi tiết hơn theo khoá, giữ các cột chiều còn lại (một giá trị mỗi khoá), rồi tính lại chỉ số dẫn xuất.
    *
    * @param dimensionColumns output columns before the measures, in table order / cột đầu ra trước chỉ số, theo thứ tự bảng
    * @param keys             grouping columns, a subset of dimensionColumns / cột nhóm, là tập con của dimensionColumns
    */
  def rollUp(rows: DataFrame, dimensionColumns: Seq[String], keys: Seq[String]): DataFrame = {
    val labels = dimensionColumns.filterNot(keys.contains)
    val aggregates = labels.map(label => max(col(label)).as(label)) ++ Base.map(measure => sum(col(measure)).as(measure))
    complete(rows.groupBy(keys.map(col): _*).agg(aggregates.head, aggregates.tail: _*), dimensionColumns)
  }
}
