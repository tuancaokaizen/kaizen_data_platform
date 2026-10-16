package com.kaizen.batch.processing.dim

import com.kaizen.batch.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}

/** nessie.dw.dim_employee: employee with the name of the shop they work at.
  * nessie.dw.dim_employee: nhân viên kèm tên cửa hàng làm việc.
  */
object DimEmployee {

  // Input arguments / Tham số đầu vào:
  // args(0): environment PROD | LOCAL / môi trường
  // args(1), args(2): fromDate, toDate, unused: a dimension holds the current state / không dùng: dimension giữ trạng thái hiện tại
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSpark("DimEmployee", args(0)).getOrCreate()
    process(spark)
    spark.stop()
    System.exit(0)
  }

  def process(spark: SparkSession): Unit = {
    val employee = spark.table("nessie.fnd.employee")
    val shop = spark.table("nessie.fnd.shop").select(col("Code").as("ShopCode"), col("Name").as("ShopName"))

    val dimEmployee = employee
      .join(broadcast(shop), Seq("ShopCode"), "left")
      .select(
        col("Id").as("EmployeeId"),
        col("Name").as("EmployeeName"),
        col("Degree"),
        col("Status").cast("boolean").as("IsActive"),
        col("IsManager").cast("boolean"),
        col("ShopCode"),
        coalesce(col("ShopName"), lit("Unknown")).as("ShopName")
      )

    SparkUtils.overwriteTable(spark, dimEmployee, "nessie.dw.dim_employee")
  }
}
