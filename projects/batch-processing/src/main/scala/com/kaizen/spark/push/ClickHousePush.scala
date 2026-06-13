package com.kaizen.spark.push

import org.apache.spark.sql.DataFrame
import com.kaizen.spark.config.SparkConfig

class ClickHousePush(config: SparkConfig) {
  def push(df: DataFrame, tableName: String): Unit = {
    val clickhouseHost = config.get("clickhouse", "host")
    val clickhousePort = config.get("clickhouse", "port")
    val clickhouseDb = config.get("clickhouse", "database")
    
    val jdbcUrl = s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDb"
    
    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", config.get("clickhouse", "username"))
      .option("password", config.get("clickhouse", "password"))
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .mode("append")
      .save()
  }
}
