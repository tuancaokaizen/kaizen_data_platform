package com.kaizen.spark.processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.kaizen.spark.utils.SparkSessionBuilder
import com.kaizen.spark.config.SparkConfig

object BatchAggregator {
  def aggregate(df: DataFrame): DataFrame = {
    df.groupBy(col("event_type"), to_date(col("timestamp")).as("event_date"))
      .count()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.buildSession("BatchAggregatorJob")
    val config = new SparkConfig()
    
    // Load, aggregate, and process
    spark.stop()
  }
}
