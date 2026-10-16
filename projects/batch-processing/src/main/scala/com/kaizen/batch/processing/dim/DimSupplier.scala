package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, count, lit}

/** nessie.dw.dim_supplier: supplier with its promised lead time and number of products, rebuilt every run.
  * nessie.dw.dim_supplier: nhà cung cấp kèm thời gian giao cam kết và số sản phẩm, dựng lại mỗi lần chạy.
  */
object DimSupplier {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimSupplier", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val supplier = spark.table("nessie.fnd.supplier")
    val suppliedProduct = spark.table("nessie.fnd.product")
      .where(col("SupplierId").isNotNull)
      .groupBy(col("SupplierId").as("Id"))
      .agg(count(lit(1)).cast("int").as("ProductCount"))

    val dimSupplier = supplier
      .join(broadcast(suppliedProduct), Seq("Id"), "left")
      .select(
        col("Id").as("SupplierId"),
        col("Name").as("SupplierName"),
        col("LeadTimeMeanDays").cast("decimal(5,2)").as("PromisedLeadTimeDays"),
        col("MinOrderValue").cast("long"),
        coalesce(col("ProductCount"), lit(0)).as("ProductCount")
      )

    SparkUtils.overwriteTable(spark, dimSupplier, "nessie.dw.dim_supplier")
  }
}
