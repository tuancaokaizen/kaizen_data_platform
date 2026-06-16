package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimCustomer {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimCustomer"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_customer"
      val stgTable = "nessie.stg.dim_customer"

      println("Loading customer and geography foundation tables...")
      val customer = spark.table("nessie.fnd.customer")
      val geography = spark.table("nessie.fnd.geography")

      println("Transforming dim_customer...")
      val dimCustomerDf = customer
        .join(geography, customer("WardId") === geography("Id"), "left")
        .select(
          customer("Id").as("CustomerId"),
          customer("FullName").as("FullName"),
          customer("Phone").as("Phone"),
          customer("WardId").as("WardId"),
          coalesce(geography("Name"), lit("Unknown")).as("WardName"),
          customer("Address").as("Address"),
          customer("YearOfBirth").as("YearOfBirth"),
          customer("Gender").as("Gender"),
          customer("CreatedDate").as("CreatedDate"),
          customer("ModifiedDate").as("ModifiedDate")
        )

      println(s"Writing dim_customer to staging table '$stgTable'...")
      SparkIOUtils.writeToIceberg(spark, dimCustomerDf, stgTable, "overwrite")

      println(s"Writing dim_customer to target table '$targetTable'...")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")

      println(s"✔ dim_customer full load completed successfully for target: $targetTable")
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
