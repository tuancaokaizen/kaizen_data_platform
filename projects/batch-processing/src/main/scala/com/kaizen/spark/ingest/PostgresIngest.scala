package com.kaizen.spark.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.kaizen.spark.config.SparkConfig

class PostgresIngest(spark: SparkSession, config: SparkConfig) {
  def ingestTable(tableName: String): DataFrame = {
    val jdbcUrl = s"jdbc:postgresql://${config.get("postgres", "host")}:${config.get("postgres", "port")}/${config.get("postgres", "database")}"
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", config.get("postgres", "username"))
      .option("password", config.get("postgres", "password"))
      .option("driver", "org.postgresql.Driver")
      .load()
  }
}
