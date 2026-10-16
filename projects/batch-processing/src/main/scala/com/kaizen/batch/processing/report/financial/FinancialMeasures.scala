package com.kaizen.batch.processing.report.financial

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, max, round, sum, when}

/** Financial report formulas, shared by every grain so the reports always agree.
  * Công thức báo cáo tài chính, dùng chung cho mọi grain để các báo cáo luôn khớp nhau.
  *
  *   NetRevenue   = GrossRevenue - ReturnAmount
  *   TotalCost    = TotalCogs + TotalPromotionCost
  *   GrossProfit  = NetRevenue - TotalCost
  *   ProfitMargin = round(GrossProfit / NetRevenue, 4) when NetRevenue > 0, else 0
  *
  * Money is double like ClickHouse Float64, quantities are long.
  * Tiền là double như Float64 của ClickHouse, số lượng là long.
  */
object FinancialMeasures {

  /** Measures a report must have before `complete`. / Chỉ số một báo cáo phải có trước khi gọi `complete`. */
  val Base: Seq[String] =
    Seq("SalesQuantity", "ReturnQuantity", "GrossRevenue", "ReturnAmount", "TotalCogs", "TotalPromotionCost")

  /** Measure columns of every financial report, in table order. / Cột chỉ số của mọi báo cáo tài chính, theo thứ tự bảng. */
  val Columns: Seq[String] = Seq(
    "SalesQuantity", "ReturnQuantity", "GrossRevenue", "ReturnAmount", "NetRevenue",
    "TotalCogs", "TotalPromotionCost", "TotalCost", "GrossProfit", "ProfitMargin"
  )

  /** Adds the derived measures and returns dimensionColumns followed by Columns.
    * Thêm chỉ số dẫn xuất và trả về dimensionColumns rồi tới Columns.
    */
  def complete(rows: DataFrame, dimensionColumns: Seq[String]): DataFrame =
    rows
      .withColumn("SalesQuantity", coalesce(col("SalesQuantity").cast("long"), lit(0L)))
      .withColumn("ReturnQuantity", coalesce(col("ReturnQuantity").cast("long"), lit(0L)))
      .withColumn("GrossRevenue", coalesce(col("GrossRevenue").cast("double"), lit(0.0)))
      .withColumn("ReturnAmount", coalesce(col("ReturnAmount").cast("double"), lit(0.0)))
      .withColumn("TotalCogs", coalesce(col("TotalCogs").cast("double"), lit(0.0)))
      .withColumn("TotalPromotionCost", coalesce(col("TotalPromotionCost").cast("double"), lit(0.0)))
      .withColumn("NetRevenue", col("GrossRevenue") - col("ReturnAmount"))
      .withColumn("TotalCost", col("TotalCogs") + col("TotalPromotionCost"))
      .withColumn("GrossProfit", col("NetRevenue") - col("TotalCost"))
      .withColumn("ProfitMargin", when(col("NetRevenue") > 0, round(col("GrossProfit") / col("NetRevenue"), 4)).otherwise(lit(0.0)))
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
