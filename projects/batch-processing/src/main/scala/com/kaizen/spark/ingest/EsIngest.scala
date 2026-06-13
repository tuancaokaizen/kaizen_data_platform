package com.kaizen.spark.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.kaizen.spark.config.SparkConfig

class EsIngest(spark: SparkSession, config: SparkConfig) {
  def ingestIndex(resource: String): DataFrame = {
    spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", config.get("elasticsearch", "hosts"))
      .option("es.resource", resource)
      .load()
  }
}
