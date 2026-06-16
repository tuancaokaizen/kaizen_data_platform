package com.kaizen.spark.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.kaizen.spark.config.SparkConfig
import scala.annotation.tailrec

object SparkIOUtils {

  /**
   * Parses command-line arguments formatted as --key value into a Map.
   * Example: Array("--sourceTable", "public.Geography") -> Map("sourceTable" -> "public.Geography")
   */
  def parseArgs(args: Array[String]): Map[String, String] = {
    @tailrec
    def parse(list: List[String], map: Map[String, String]): Map[String, String] = list match {
      case Nil => map
      case key :: value :: tail if key.startsWith("--") && !value.startsWith("--") => 
        parse(tail, map + (key.substring(2) -> value))
      case key :: tail if key.startsWith("--") => 
        parse(tail, map + (key.substring(2) -> ""))
      case _ :: tail => 
        parse(tail, map)
    }
    parse(args.toList, Map.empty)
  }

  /**
   * Generates PostgreSQL JDBC URL from SparkConfig
   */
  def getPostgresJdbcUrl(config: SparkConfig): String = {
    val host = config.get("postgres", "host")
    val port = config.get("postgres", "port")
    val database = config.get("postgres", "database")
    s"jdbc:postgresql://$host:$port/$database"
  }

  /**
   * Initializes Spark session with Kryo serialization.
   * Leverages existing configuration if running in Spark cluster, falls back to local[*] if run locally.
   */
  def createSparkSession(jobName: String): SparkSession = {
    val builder = SparkSession.builder()
      .appName(jobName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")

    if (sys.props.contains("spark.master") || sys.env.contains("SPARK_MASTER")) {
      builder.getOrCreate()
    } else {
      builder.config("spark.master", "local[*]").getOrCreate()
    }
  }

  /**
   * Ensures Nessie/Iceberg namespace exists
   */
  def ensureNamespaceExists(spark: SparkSession, catalogAndNamespace: String): Unit = {
    println(s"Ensuring namespace '$catalogAndNamespace' exists...")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogAndNamespace")
  }

  /**
   * Check if a table exists in the catalog by attempting to load it.
   */
  def tableExists(spark: SparkSession, table: String): Boolean = {
    try {
      spark.read.format("iceberg").load(table)
      true
    } catch {
      case _: Exception => false
    }
  }

  private def quoteColumn(col: String): String = {
    val trimmed = col.trim
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) trimmed else s""""$trimmed""""
  }

  /**
   * Read table from PostgreSQL (supports SQL wrapping for incremental loads based on watermark).
   * Supports multiple watermark columns separated by comma (treated as OR condition).
   */
  def readFromPostgres(
      spark: SparkSession,
      table: String,
      config: SparkConfig,
      watermarkCol: Option[String] = None,
      minTimestamp: Option[String] = None,
      maxTimestamp: Option[String] = None
  ): DataFrame = {
    val jdbcUrl = getPostgresJdbcUrl(config)
    val dbTable = (watermarkCol, minTimestamp, maxTimestamp) match {
      case (Some(colsStr), Some(minTs), Some(maxTs)) =>
        val conditions = colsStr.split(",").map(quoteColumn).map { qCol =>
          s"($qCol >= '$minTs' AND $qCol < '$maxTs')"
        }.mkString(" OR ")
        s"""(SELECT * FROM $table WHERE $conditions) as temp_table"""
      case (Some(colsStr), Some(minTs), None) =>
        val conditions = colsStr.split(",").map(quoteColumn).map { qCol =>
          s"$qCol > '$minTs'"
        }.mkString(" OR ")
        s"""(SELECT * FROM $table WHERE $conditions) as temp_table"""
      case _ =>
        table
    }

    println(s"Reading from PostgreSQL table/query: $dbTable")
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbTable)
      .option("user", config.get("postgres", "username"))
      .option("password", config.get("postgres", "password"))
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  /**
   * Writes DataFrame to Iceberg.
   */
  def writeToIceberg(spark: SparkSession, df: DataFrame, table: String, mode: String): Unit = {
    val parts = table.split("\\.")
    if (parts.length > 1) {
      val namespace = parts.init.mkString(".")
      ensureNamespaceExists(spark, namespace)
    }

    println(s"Writing to Iceberg Nessie table: $table (mode: $mode)")
    df.write
      .format("iceberg")
      .mode(mode)
      .saveAsTable(table)

    if (mode == "overwrite") {
      try {
        println(s"Expiring old snapshots for $table to prevent duplicate S3 file reads...")
        spark.sql(s"CALL nessie.system.expire_snapshots(table => '$table', retain_last => 1)")
      } catch {
        case e: Exception => println(s"Warning: Could not expire snapshots for $table: ${e.getMessage}")
      }
    }
  }

  /**
   * Read from Iceberg catalog.
   */
  def readFromIceberg(spark: SparkSession, table: String): DataFrame = {
    spark.table(table)
  }

  /**
   * Queries current max watermark value from target Iceberg table.
   * If watermarkCol contains multiple columns, uses the first one (primary) to fetch max value.
   */
  def getMaxWatermark(spark: SparkSession, table: String, watermarkCol: String): Option[String] = {
    if (tableExists(spark, table)) {
      println(s"Fetching max watermark from Iceberg table: $table for column: $watermarkCol")
      val df = readFromIceberg(spark, table)
      
      val cols = watermarkCol.split(",").map(_.trim)
      val primaryCol = cols.head
      
      val maxRow = df.select(max(col(primaryCol))).collect()
      if (maxRow.nonEmpty && maxRow(0) != null && maxRow(0).get(0) != null) {
        val watermarkVal = maxRow(0).get(0).toString
        println(s"Found max watermark: $watermarkVal")
        Some(watermarkVal)
      } else {
        println(s"No max watermark found in $table (empty table)")
        None
      }
    } else {
      println(s"Table $table does not exist. Cannot fetch watermark.")
      None
    }
  }

  /**
   * Performs Iceberg MERGE INTO target using join keys from a source DataFrame.
   */
  def mergeIcebergTable(
      spark: SparkSession,
      sourceDf: DataFrame,
      targetTable: String,
      joinKeys: Seq[String]
  ): Unit = {
    val tempViewName = s"merge_source_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    sourceDf.createOrReplaceTempView(tempViewName)
    try {
      val joinCondition = joinKeys.map(k => s"t.$k = s.$k").mkString(" AND ")
      val sqlText =
        s"""MERGE INTO $targetTable t
           |USING $tempViewName s
           |ON $joinCondition
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin
      println(s"Executing Iceberg MERGE INTO: $targetTable using join condition: $joinCondition")
      spark.sql(sqlText)

      try {
        println(s"Expiring old snapshots for $targetTable to prevent duplicate S3 file reads...")
        spark.sql(s"CALL nessie.system.expire_snapshots(table => '$targetTable', retain_last => 1)")
      } catch {
        case e: Exception => println(s"Warning: Could not expire snapshots for $targetTable: ${e.getMessage}")
      }
    } finally {
      spark.catalog.dropTempView(tempViewName)
    }
  }

  /**
   * Writes a DataFrame to ClickHouse via JDBC.
   */
  def writeToClickHouse(df: DataFrame, tableName: String, config: SparkConfig): Unit = {
    val clickhouseHost = config.get("clickhouse", "host")
    val clickhousePort = config.get("clickhouse", "port")
    val clickhouseDb = config.get("clickhouse", "database")
    val jdbcUrl = s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDb"

    println(s"Writing to ClickHouse table: $tableName")
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

  /**
   * Reads an Elasticsearch resource index into a DataFrame.
   */
  def readFromElasticSearch(spark: SparkSession, resource: String, config: SparkConfig): DataFrame = {
    println(s"Reading from Elasticsearch index: $resource")
    spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", config.get("elasticsearch", "hosts"))
      .option("es.resource", resource)
      .load()
  }
}
