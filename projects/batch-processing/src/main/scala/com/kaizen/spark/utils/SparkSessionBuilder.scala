package com.kaizen.spark.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def buildSession(appName: String = "KaizenSparkBatchJob", master: String = "local[*]"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
}
