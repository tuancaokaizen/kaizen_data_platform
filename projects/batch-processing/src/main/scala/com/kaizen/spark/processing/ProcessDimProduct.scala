package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimProduct {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimProduct"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_product"
      val stgTable = "nessie.stg.dim_product"

      println("Loading product foundation tables...")
      val product = spark.table("nessie.fnd.product")
      val supplement = spark.table("nessie.fnd.product_supplement")
      val medicine = spark.table("nessie.fnd.product_medicine")

      println("Transforming dim_product...")
      val dimProductDf = product
        .join(supplement, product("Id") === supplement("ProductId"), "left")
        .join(medicine, product("Id") === medicine("ProductId"), "left")
        .select(
          product("Id").as("ProductId"),
          product("Name").as("Name"),
          product("Type").as("Type"),
          product("Unit").as("Unit"),
          product("StockPrice").as("StockPrice"),
          product("RetailPrice").as("RetailPrice"),
          product("TaxRate").as("TaxRate"),
          product("IsPromotion").as("IsPromotion"),
          product("Status").as("Status"),
          coalesce(supplement("Brand"), lit("Unknown")).as("Brand"),
          coalesce(supplement("PrimaryFunction"), lit("Unknown")).as("PrimaryFunction"),
          coalesce(medicine("Indication"), lit("Unknown")).as("Indication"),
          coalesce(medicine("IsPrescription"), lit(false)).as("IsPrescription"),
          coalesce(medicine("SpecialtyDisease"), lit("Unknown")).as("SpecialtyDisease"),
          product("CreatedDate").as("CreatedDate"),
          product("ModifiedDate").as("ModifiedDate")
        )

      println(s"Writing dim_product to staging table '$stgTable'...")
      SparkIOUtils.writeToIceberg(spark, dimProductDf, stgTable, "overwrite")

      println(s"Writing dim_product to target table '$targetTable'...")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")

      println(s"✔ dim_product full load completed successfully for target: $targetTable")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, "nessie.stg.dim_product")
          SparkIOUtils.expireTableSnapshots(spark, "nessie.dw.dim_product")
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
