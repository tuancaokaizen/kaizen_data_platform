package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, lit, when}

/** nessie.dw.dim_product: product with its medicine or supplement details; the product line is Product.Type.
  * nessie.dw.dim_product: sản phẩm kèm thông tin thuốc hoặc thực phẩm chức năng; ngành hàng là Product.Type.
  */
object DimProduct {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimProduct", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val product = spark.table("nessie.fnd.product")
    val medicine = spark.table("nessie.fnd.product_medicine")
      .select(col("ProductId").as("Id"), col("Indication"), col("IsPrescription"), col("SpecialtyDisease"))
    val supplement = spark.table("nessie.fnd.product_supplement")
      .select(col("ProductId").as("Id"), col("Brand"), col("PrimaryFunction"))

    // Vietnamese product line name shown in the reports / Tên ngành hàng tiếng Việt hiển thị trên báo cáo
    val productLineName = when(col("Type") === "medicine", lit("Thuốc"))
      .when(col("Type") === "supplement", lit("Thực phẩm chức năng"))
      .otherwise(coalesce(col("Type"), lit("Không xác định")))

    val dimProduct = product
      .join(medicine, Seq("Id"), "left")
      .join(supplement, Seq("Id"), "left")
      .select(
        col("Id").as("ProductId"),
        col("Name").as("ProductName"),
        coalesce(col("Type"), lit("Unknown")).as("ProductLineCode"),
        productLineName.as("ProductLineName"),
        col("Unit"),
        col("StockPrice").cast("long"),
        col("RetailPrice").cast("long"),
        col("TaxRate").cast("int"),
        col("IsPromotion").cast("boolean"),
        col("Status").cast("boolean").as("IsActive"),
        col("SupplierId"),
        coalesce(col("Brand"), lit("Unknown")).as("Brand"),
        coalesce(col("PrimaryFunction"), lit("Unknown")).as("PrimaryFunction"),
        coalesce(col("Indication"), lit("Unknown")).as("Indication"),
        coalesce(col("IsPrescription").cast("boolean"), lit(false)).as("IsPrescription"),
        coalesce(col("SpecialtyDisease"), lit("Unknown")).as("SpecialtyDisease"),
        col("IsColdChain").cast("boolean"),
        col("ShelfLifeDays").cast("int"),
        col("LaunchDate").cast("date"),
        col("DiscontinuedDate").cast("date")
      )

    SparkUtils.overwriteTable(spark, dimProduct, "nessie.dw.dim_product")
  }
}
