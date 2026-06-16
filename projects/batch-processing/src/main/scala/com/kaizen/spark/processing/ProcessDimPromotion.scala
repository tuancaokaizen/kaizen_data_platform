package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimPromotion {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimPromotion"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_promotion"
      val stgTable = "nessie.stg.dim_promotion"

      // 1. Fetch watermark from the target table to achieve incremental load
      val watermarkOpt = SparkIOUtils.getMaxWatermark(spark, targetTable, "ModifiedDate")

      println("Loading promotion foundation table...")
      var promotionFnd = spark.table("nessie.fnd.promotion")

      // 2. Filter delta if watermark exists
      watermarkOpt.foreach { watermark =>
        println(s"Filtering promotion delta records with ModifiedDate > $watermark")
        promotionFnd = promotionFnd.filter(col("ModifiedDate") > watermark)
      }

      // 3. Transform and select fields
      println("Transforming dim_promotion...")
      val dimPromotionDf = promotionFnd.select(
        col("Code").as("PromotionCode"),
        col("Name").as("PromotionName"),
        col("StartDate").as("StartDate"),
        col("EndDate").as("EndDate"),
        col("Status").as("Status"),
        col("Type").as("PromotionType"),
        col("CreatedDate").as("CreatedDate"),
        col("ModifiedDate").as("ModifiedDate")
      )

      // 4. Write to staging and merge/overwrite to target
      val targetExists = SparkIOUtils.tableExists(spark, targetTable)
      if (!targetExists) {
        println(s"Target table '$targetTable' does not exist. Performing initial load...")
        SparkIOUtils.writeToIceberg(spark, dimPromotionDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")
      } else {
        println(s"Merging delta records into target table '$targetTable'...")
        SparkIOUtils.writeToIceberg(spark, dimPromotionDf, stgTable, "overwrite")
        val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
        SparkIOUtils.mergeIcebergTable(spark, stgDf, targetTable, Seq("PromotionCode"))
      }

      println(s"✔ dim_promotion incremental load completed successfully for target: $targetTable")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, "nessie.stg.dim_promotion")
          SparkIOUtils.expireTableSnapshots(spark, "nessie.dw.dim_promotion")
        }
        spark.stop()
      case Failure(e) =>
        System.err.println(s"❌ [$jobName] failed with error:")
        e.printStackTrace()
        spark.stop()
        throw e
    }
  }
}
