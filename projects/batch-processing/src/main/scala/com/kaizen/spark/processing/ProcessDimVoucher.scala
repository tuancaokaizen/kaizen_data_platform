package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimVoucher {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimVoucher"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_voucher"
      val stgTable = "nessie.stg.dim_voucher"

      // 1. Fetch watermark from the target table to achieve incremental load
      val watermarkOpt = SparkIOUtils.getMaxWatermark(spark, targetTable, "ModifiedDate")

      println("Loading voucher, series, and promotion foundation tables...")
      val voucherSeriesFnd = spark.table("nessie.fnd.voucher_series")
      val voucherFnd = spark.table("nessie.fnd.voucher")
      val promotionFnd = spark.table("nessie.fnd.promotion")

      // 2. Filter delta for voucher series if watermark exists
      var deltaVoucherSeries = voucherSeriesFnd
      watermarkOpt.foreach { watermark =>
        println(s"Filtering voucher_series delta records with ModifiedDate > $watermark")
        deltaVoucherSeries = voucherSeriesFnd.filter(col("ModifiedDate") > watermark)
      }

      // 3. Transform, join and select fields
      println("Transforming dim_voucher...")
      val dimVoucherDf = deltaVoucherSeries
        .join(voucherFnd, deltaVoucherSeries("VoucherCode") === voucherFnd("Code"), "left")
        .join(promotionFnd, voucherFnd("PromotionCode") === promotionFnd("Code"), "left")
        .select(
          deltaVoucherSeries("SeriesCode").as("VoucherSeriesCode"),
          deltaVoucherSeries("VoucherCode").as("VoucherCode"),
          coalesce(voucherFnd("Name"), lit("Unknown")).as("VoucherName"),
          coalesce(voucherFnd("VoucherAmount"), lit(0.0)).as("VoucherAmount"),
          coalesce(voucherFnd("MinOrderValue"), lit(0.0)).as("MinOrderValue"),
          coalesce(deltaVoucherSeries("ExpiryDay"), lit(0)).as("ExpiryDay"),
          coalesce(voucherFnd("PromotionCode"), lit("Unknown")).as("PromotionCode"),
          coalesce(promotionFnd("Name"), lit("Unknown")).as("PromotionName"),
          coalesce(promotionFnd("Type"), lit("Unknown")).as("PromotionType"),
          deltaVoucherSeries("Status").as("Status"),
          deltaVoucherSeries("CreatedDate").as("CreatedDate"),
          deltaVoucherSeries("ModifiedDate").as("ModifiedDate")
        )

      // 4. Write to staging and merge/overwrite to target
      val targetExists = SparkIOUtils.tableExists(spark, targetTable)
      if (!targetExists) {
        println(s"Target table '$targetTable' does not exist. Performing initial load...")
        SparkIOUtils.writeToIceberg(spark, dimVoucherDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
      } else {
        println(s"Merging delta records into target table '$targetTable'...")
        SparkIOUtils.writeToIceberg(spark, dimVoucherDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("VoucherSeriesCode"))
      }

      println(s"✔ dim_voucher incremental load completed successfully for target: $targetTable")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }
}
