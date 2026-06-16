package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimShop {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimShop"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_shop"
      val stgTable = "nessie.stg.dim_shop"

      println("Loading shop, employee and geography foundation tables...")
      val shop = spark.table("nessie.fnd.shop")
      val employee = spark.table("nessie.fnd.employee")
      val geography = spark.table("nessie.fnd.geography")

      println("Transforming dim_shop...")
      val dimShopDf = shop
        .join(employee, shop("ManagerId") === employee("Id"), "left")
        .join(geography, shop("WardId") === geography("Id"), "left")
        .select(
          shop("Code").as("ShopCode"),
          shop("Name").as("ShopName"),
          shop("Address").as("Address"),
          shop("WardId").as("WardId"),
          coalesce(geography("Name"), lit("Unknown")).as("WardName"),
          shop("ManagerId").as("ManagerId"),
          coalesce(employee("Name"), lit("Unknown")).as("ManagerName"),
          shop("CreatedDate").as("CreatedDate"),
          shop("ModifiedDate").as("ModifiedDate")
        )

      println(s"Writing dim_shop to staging table '$stgTable'...")
      SparkIOUtils.writeToIceberg(spark, dimShopDf, stgTable, "overwrite")

      println(s"Writing dim_shop to target table '$targetTable'...")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")

      println(s"✔ dim_shop full load completed successfully for target: $targetTable")
    }

    jobResult match {
      case Success(_) =>
        println(s"✔ [$jobName] completed successfully.")
        Try {
          SparkIOUtils.expireTableSnapshots(spark, "nessie.stg.dim_shop")
          SparkIOUtils.expireTableSnapshots(spark, "nessie.dw.dim_shop")
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
