package com.kaizen.batch.processing.fact

import com.kaizen.batch.utils.iceberg.Partition
import com.kaizen.batch.utils.{DateUtils, SparkUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

/** nessie.dw.fact_stock_movement: one row per stock ledger entry (SALE, RETURN_RESTOCK, RECEIPT, TRANSFER_OUT,
  * TRANSFER_IN, EXPIRE, DAMAGE, ADJUST) for the days [fromDate, toDate) and the entries changed in the window.
  * Quantity is signed like the ledger: positive in, negative out; InQuantity and OutQuantity are both positive.
  * nessie.dw.fact_stock_movement: mỗi dòng một bút toán sổ kho (SALE, RETURN_RESTOCK, RECEIPT, TRANSFER_OUT,
  * TRANSFER_IN, EXPIRE, DAMAGE, ADJUST) cho các ngày [fromDate, toDate) và các bút toán thay đổi trong cửa sổ.
  * Quantity có dấu như sổ kho: dương là nhập, âm là xuất; InQuantity và OutQuantity đều dương.
  */
object FactStockMovement {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1): fromDate yyyy-MM-dd / ngày bắt đầu
  // args(2): toDate yyyy-MM-dd, excluded / ngày kết thúc, không bao gồm
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("FactStockMovement", args(0)).getOrCreate()
    process(spark, args(1), args(2))
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)

    val factStockMovement = spark.table("nessie.fnd.stock_movement")
      .where(DateUtils.inDateRange(col("MovementDate"), fromDate, toDate) || DateUtils.inLoadWindow(col("ModifiedDate"), fromDate, toDate))
      .select(
        col("Id").as("MovementId"),
        col("Seq").cast("long").as("Sequence"),
        col("MovementDate").cast("date"),
        col("MovementTime").cast("timestamp"),
        col("MovementType"),
        col("Reason"),
        col("LocationType"),
        col("LocationId"),
        col("ProductId"),
        col("LotId"),
        col("Qty").cast("int").as("Quantity"),
        when(col("Qty") > 0, col("Qty")).otherwise(lit(0)).cast("int").as("InQuantity"),
        when(col("Qty") < 0, -col("Qty")).otherwise(lit(0)).cast("int").as("OutQuantity"),
        col("UnitCost").cast("long"),
        (col("Qty") * col("UnitCost")).cast("long").as("ValueAmount"),
        col("RefDocType"),
        col("RefDocId"),
        col("RefLineId"),
        col("ModifiedDate").cast("timestamp")
      )

    SparkUtils.mergeInto(
      spark,
      factStockMovement,
      "nessie.dw.fact_stock_movement",
      keys = Seq("MovementId"),
      partition = Partition.Months("MovementDate")
    )
  }
}
