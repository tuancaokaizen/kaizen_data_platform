package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/** nessie.dw.dim_payment_method: cash, card, transfer, wallet. / nessie.dw.dim_payment_method: phương thức thanh toán. */
object DimPaymentMethod {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimPaymentMethod", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val dimPaymentMethod = spark.table("nessie.fnd.payment_method")
      .select(col("Id").cast("int").as("PaymentMethodId"), col("Name").as("PaymentMethodName"))

    SparkUtils.overwriteTable(spark, dimPaymentMethod, "nessie.dw.dim_payment_method")
  }
}
