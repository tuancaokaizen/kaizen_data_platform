package com.kaizen.spark.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.utils.SparkIOUtils
import scala.util.{Try, Success, Failure}

object ProcessDimEmployee {

  def main(args: Array[String]): Unit = {
    val jobName = "ProcessDimEmployee"
    println(s"=== Starting $jobName ===")

    val spark = SparkIOUtils.createSparkSession(jobName)
    val config = new SparkConfig()

    val jobResult = Try {
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.stg")
      SparkIOUtils.ensureNamespaceExists(spark, "nessie.dw")

      val targetTable = "nessie.dw.dim_employee"
      val stgTable = "nessie.stg.dim_employee"

      println("Loading employee and shop foundation tables...")
      val employee = spark.table("nessie.fnd.employee")
      val shop = spark.table("nessie.fnd.shop")

      println("Transforming dim_employee...")
      val dimEmployeeDf = employee
        .join(shop, employee("ShopCode") === shop("Code"), "left")
        .select(
          employee("Id").as("EmployeeId"),
          employee("Name").as("EmployeeName"),
          employee("Degree").as("Degree"),
          employee("Phone").as("Phone"),
          employee("Address").as("Address"),
          employee("Status").as("Status"),
          employee("IsManager").as("IsManager"),
          employee("ShopCode").as("ShopCode"),
          coalesce(shop("Name"), lit("Unknown")).as("ShopName"),
          employee("CreatedDate").as("CreatedDate"),
          employee("ModifiedDate").as("ModifiedDate")
        )

      println(s"Writing dim_employee to staging table '$stgTable'...")
      SparkIOUtils.writeToIceberg(spark, dimEmployeeDf, stgTable, "overwrite")

      println(s"Writing dim_employee to target table '$targetTable'...")
      val stgDf = SparkIOUtils.readFromIceberg(spark, stgTable)
      SparkIOUtils.writeToIceberg(spark, stgDf, targetTable, "overwrite")

      println(s"✔ dim_employee full load completed successfully for target: $targetTable")
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
