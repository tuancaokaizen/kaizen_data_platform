package com.kaizen.spark.processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.kaizen.spark.utils.SparkSessionBuilder
import com.kaizen.spark.config.SparkConfig
import com.kaizen.spark.ingest.PostgresIngest

object DataCleansing {
  def clean(df: DataFrame): DataFrame = {
    df.dropDuplicates()
      .na.fill("")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBuilder.buildSession("DataCleansingJob")
    val config = new SparkConfig()
    
    val postgresIngest = new PostgresIngest(spark, config)
    val rawDf = postgresIngest.ingestTable("raw_events")
    
    val cleanedDf = clean(rawDf)
    cleanedDf.show()
    
    spark.stop()
  }
}
