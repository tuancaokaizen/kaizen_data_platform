package com.kaizen.batch.publish.clickhouse

import com.kaizen.batch.utils.jdbc.JdbcOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Source: reads ClickHouse over JDBC. / Source: đọc ClickHouse qua JDBC. */
object ClickHouseSource {

  /** Runs a ClickHouse SELECT and returns the rows. / Chạy một SELECT ClickHouse và trả về các dòng. */
  def readQuery(spark: SparkSession, clickHouse: JdbcOptions, query: String): DataFrame =
    spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> clickHouse.url,
          "user" -> clickHouse.user,
          "password" -> clickHouse.password,
          "driver" -> clickHouse.driver,
          "query" -> query
        )
      )
      .load()
}
