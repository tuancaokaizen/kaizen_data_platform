package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.dw.dim_promotion: promotion programs. / nessie.dw.dim_promotion: chương trình khuyến mãi. */
object DimPromotion {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimPromotion", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val dimPromotion = spark.table("nessie.fnd.promotion")
      .select(
        col("Code").as("PromotionCode"),
        col("Name").as("PromotionName"),
        col("Type").as("PromotionType"),
        col("StartDate").cast("timestamp"),
        col("EndDate").cast("timestamp"),
        col("Status").cast("boolean").as("IsActive")
      )

    SparkUtils.overwriteTable(spark, dimPromotion, "nessie.dw.dim_promotion")
  }
}
