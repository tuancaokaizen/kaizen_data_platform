package com.kaizen.batch.utils.jdbc

import com.kaizen.batch.utils.DateUtils

/** Connection options for Spark's JDBC reader and writer; the password never appears in toString.
  * Tuỳ chọn kết nối cho JDBC của Spark; mật khẩu không bao giờ xuất hiện trong toString.
  */
final case class JdbcOptions(
    url: String,
    user: String,
    password: String,
    driver: String,
    sessionInitStatement: Option[String]
) {

  /** Options for spark.read.format("jdbc") on one table or subquery.
    * Tuỳ chọn cho spark.read.format("jdbc") trên một bảng hoặc truy vấn con.
    */
  def readOptions(table: String, fetchSize: Int = JdbcOptions.DefaultFetchSize): Map[String, String] =
    Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "driver" -> driver,
      "dbtable" -> table,
      "fetchsize" -> fetchSize.toString
    ) ++ sessionInitStatement.map("sessionInitStatement" -> _)

  /** Options for DataFrame.write.format("jdbc") into one table. / Tuỳ chọn cho DataFrame.write.format("jdbc") vào một bảng. */
  def writeOptions(table: String, batchSize: Int = JdbcOptions.DefaultBatchSize): Map[String, String] =
    Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "driver" -> driver,
      "dbtable" -> table,
      "batchsize" -> batchSize.toString
    )

  override def toString: String = s"JdbcOptions($url, user=$user, password=***)"
}

object JdbcOptions {
  val DefaultFetchSize: Int = 10000
  val DefaultBatchSize: Int = 10000

  /** Postgres connection whose session runs in Vietnam time, so dates and timestamptz bounds follow the shop calendar.
    * Kết nối Postgres với session theo giờ Việt Nam, để ngày và mốc timestamptz theo lịch cửa hàng.
    */
  def postgres(
      host: String,
      port: Int,
      database: String,
      user: String,
      password: String,
      applicationName: String
  ): JdbcOptions =
    JdbcOptions(
      url = s"jdbc:postgresql://$host:$port/$database?ApplicationName=$applicationName",
      user = user,
      password = password,
      driver = "org.postgresql.Driver",
      sessionInitStatement = Some(s"SET TIME ZONE '${DateUtils.VietnamZoneName}'")
    )

  /** ClickHouse connection over HTTP (port 8123). / Kết nối ClickHouse qua HTTP (cổng 8123). */
  def clickHouse(host: String, port: Int, database: String, user: String, password: String): JdbcOptions =
    JdbcOptions(
      url = s"jdbc:clickhouse://$host:$port/$database",
      user = user,
      password = password,
      driver = "com.clickhouse.jdbc.ClickHouseDriver",
      sessionInitStatement = None
    )
}
