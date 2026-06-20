package com.kaizen.spark.utils

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  TimestampType
}
import com.kaizen.spark.config.SparkConfig
import scala.annotation.tailrec
import scala.collection.mutable

object SparkIOUtils {

  /** Business timezone for Postgres bounds / Múi giờ nghiệp vụ VN trên Postgres */
  val BusinessTimeZone: String = "Asia/Ho_Chi_Minh"

  /** Iceberg/Nessie stores VN wall-clock digits as UTC literal / Nessie lưu số giờ VN dạng UTC literal */
  val IcebergStorageTimeZone: String = "UTC"

  /** Fixed offset for timestamptz SQL literals (Vietnam has no DST) / Offset cố định +07 */
  private val BusinessTimeZoneOffset: String = "+07"

  /** Cache column metadata per table within one JVM / Cache metadata cột theo bảng */
  private val postgresColumnMetadataCache =
    mutable.Map.empty[String, Seq[(String, String, String)]]

  /** Parses command-line arguments formatted as --key value into a Map.
    * Example: Array("--sourceTable", "public.Geography") -> Map("sourceTable"
    * -> "public.Geography")
    */
  def parseArgs(args: Array[String]): Map[String, String] = {
    @tailrec
    def parse(
        list: List[String],
        map: Map[String, String]
    ): Map[String, String] = list match {
      case Nil => map
      case key :: value :: tail
          if key.startsWith("--") && !value.startsWith("--") =>
        parse(tail, map + (key.substring(2) -> value))
      case key :: tail if key.startsWith("--") =>
        parse(tail, map + (key.substring(2) -> ""))
      case _ :: tail =>
        parse(tail, map)
    }
    parse(args.toList, Map.empty)
  }

  /** Generates PostgreSQL JDBC URL from SparkConfig
    */
  def getPostgresJdbcUrl(config: SparkConfig): String = {
    val host = config.get("postgres", "host")
    val port = config.get("postgres", "port")
    val database = config.get("postgres", "database")
    s"jdbc:postgresql://$host:$port/$database"
  }

  /** Initializes Spark session with Kryo serialization. Leverages existing
    * configuration if running in Spark cluster, falls back to local[*] if run
    * locally.
    */
  def createSparkSession(jobName: String): SparkSession = {
    java.util.TimeZone.setDefault(
      java.util.TimeZone.getTimeZone(IcebergStorageTimeZone)
    )
    System.setProperty("user.timezone", IcebergStorageTimeZone)

    val builder = SparkSession
      .builder()
      .appName(jobName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.session.timeZone", IcebergStorageTimeZone)

    val spark =
      if (
        sys.props.contains("spark.master") || sys.env.contains("SPARK_MASTER")
      ) {
        builder.getOrCreate()
      } else {
        builder.config("spark.master", "local[*]").getOrCreate()
      }

    // Lake layer uses UTC literal wall-clock matching Postgres (+7) display / Nessie lưu số giờ như Postgres
    spark.conf.set("spark.sql.session.timeZone", IcebergStorageTimeZone)
    spark
  }

  /** Ensures Nessie/Iceberg namespace exists
    */
  def ensureNamespaceExists(
      spark: SparkSession,
      catalogAndNamespace: String
  ): Unit = {
    println(s"Ensuring namespace '$catalogAndNamespace' exists...")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogAndNamespace")
  }

  /** Check if a table exists in the catalog by attempting to load it.
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
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) trimmed
    else s""""$trimmed""""
  }

  /** True when string already carries an explicit timezone / Chuỗi đã có offset hoặc Z */
  private def hasExplicitTimeZone(ts: String): Boolean = {
    val trimmed = ts.trim
    trimmed.endsWith("Z") || trimmed.matches(".+[+-]\\d{2}(:\\d{2})?$")
  }

  /** Append +07 to naive timestamps so PG timestamptz bounds match VN calendar days.
    * Gắn +07 cho literal không timezone để filter khớp ngày VN trên timestamptz.
    */
  def toPostgresTimestamptzLiteral(ts: String): String = {
    val trimmed = ts.trim
    if (trimmed.isEmpty || hasExplicitTimeZone(trimmed)) trimmed
    else if (trimmed.length == 10) s"$trimmed 00:00:00$BusinessTimeZoneOffset"
    else s"$trimmed$BusinessTimeZoneOffset"
  }

  /** Format date bounds for Postgres JDBC (+07 timestamptz) / Mốc ngày cho filter Postgres */
  def formatBusinessDateBoundary(
      dateStr: String,
      isEndOfDay: Boolean = false
  ): String = {
    val trimmed = dateStr.trim
    val normalized =
      if (trimmed.length == 10) {
        if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
      } else trimmed
    toPostgresTimestamptzLiteral(normalized)
  }

  /** Format date bounds for Iceberg/Nessie (naive wall-clock, no offset) / Mốc ngày filter trên Nessie */
  def formatIcebergDateBoundary(
      dateStr: String,
      isEndOfDay: Boolean = false
  ): String = {
    val trimmed = dateStr.trim
    if (trimmed.length == 10) {
      if (isEndOfDay) s"$trimmed 23:59:59" else s"$trimmed 00:00:00"
    } else {
      trimmed
    }
  }

  /** Parse schema.table refs like payment."Transaction" / Parse tham chiếu bảng Postgres */
  private def parsePostgresTable(tableRef: String): (String, String) = {
    val pattern =
      """^(?:"([^"]+)"|([^".]+))\.(?:"([^"]+)"|([^".]+))$""".r
    tableRef.trim match {
      case pattern(qSchema, uSchema, qTable, uTable) =>
        (
          Option(qSchema).filter(_.nonEmpty).orElse(Option(uSchema)).get,
          Option(qTable).filter(_.nonEmpty).orElse(Option(uTable)).get
        )
      case other =>
        throw new IllegalArgumentException(
          s"Invalid PostgreSQL table reference: $other"
        )
    }
  }

  /** Escape single quotes for SQL literals / Escape dấu nháy đơn trong SQL */
  private def escapeSqlLiteral(value: String): String =
    value.replace("'", "''")

  /** True when Postgres column is timestamptz / Kiểm tra cột timestamptz trên Postgres */
  private def isPostgresTimestamptz(dataType: String, udtName: String): Boolean = {
    val normalizedType = Option(dataType).map(_.toLowerCase).getOrElse("")
    val normalizedUdt = Option(udtName).map(_.toLowerCase).getOrElse("")
    normalizedType == "timestamp with time zone" || normalizedUdt == "timestamptz"
  }

  /** Load ordered column names + data types from information_schema.
    * Lấy metadata cột từ information_schema theo thứ tự ordinal.
    */
  private def loadPostgresColumnMetadata(
      spark: SparkSession,
      config: SparkConfig,
      tableRef: String
  ): Seq[(String, String, String)] = {
    postgresColumnMetadataCache.getOrElseUpdate(
      tableRef, {
        val (schema, tableName) = parsePostgresTable(tableRef)
        val metaQuery =
          s"""(SELECT column_name, data_type, udt_name
             | FROM information_schema.columns
             | WHERE table_schema = '${escapeSqlLiteral(schema)}'
             |   AND table_name = '${escapeSqlLiteral(tableName)}'
             | ORDER BY ordinal_position) AS pg_column_metadata""".stripMargin

        spark.read
          .format("jdbc")
          .option("url", getPostgresJdbcUrl(config))
          .option("dbtable", metaQuery)
          .option("user", config.get("postgres", "username"))
          .option("password", config.get("postgres", "password"))
          .option("driver", "org.postgresql.Driver")
          .option(
            "sessionInitStatement",
            s"SET TIME ZONE '$BusinessTimeZone'"
          )
          .load()
          .collect()
          .map(row => (row.getString(0), row.getString(1), row.getString(2)))
          .toSeq
      }
    )
  }

  /** Spark parse pattern for VN offset timestamps / Pattern parse timestamp có offset +07 */
  private val BusinessTimestampParsePattern: String =
    "yyyy-MM-dd HH:mm:ss.SSSSSSX"

  /** Render timestamptz as VN local string to avoid JDBC TZ bugs / Xuất timestamptz dạng chuỗi giờ VN */
  private def projectPostgresColumn(
      columnName: String,
      dataType: String,
      udtName: String
  ): String = {
    val quoted = quoteColumn(columnName)
    if (isPostgresTimestamptz(dataType, udtName)) {
      s"to_char($quoted AT TIME ZONE '$BusinessTimeZone', 'YYYY-MM-DD HH24:MI:SS.US') AS $quoted"
    } else {
      quoted
    }
  }

  /** Parse VN local timestamp strings from JDBC into Spark timestamps.
    * Chỉ parse với offset +07 — KHÔNG dùng to_utc_timestamp (tránh trừ lệch 7h).
    */
  private def parseBusinessTimestampStrings(
      df: DataFrame,
      timestamptzColumns: Seq[String]
  ): DataFrame = {
    timestamptzColumns.foldLeft(df) { (acc, columnName) =>
      acc.withColumn(
        columnName,
        to_timestamp(
          concat(col(columnName), lit(BusinessTimeZoneOffset)),
          BusinessTimestampParsePattern
        )
      )
    }
  }

  /** True when Postgres column is naive timestamp / Cột timestamp không timezone trên Postgres */
  private def isPostgresNaiveTimestamp(
      dataType: String,
      udtName: String
  ): Boolean = {
    val normalizedType = Option(dataType).map(_.toLowerCase).getOrElse("")
    val normalizedUdt = Option(udtName).map(_.toLowerCase).getOrElse("")
    normalizedType == "timestamp without time zone" || normalizedUdt == "timestamp"
  }

  /** Fix naive JDBC timestamps misread as UTC on executors / Sửa timestamp naive bị JDBC đọc sai UTC */
  private def normalizeNaiveTimestampColumns(
      df: DataFrame,
      naiveTimestampColumns: Set[String]
  ): DataFrame = {
    naiveTimestampColumns.foldLeft(df) { (acc, columnName) =>
      acc.withColumn(
        columnName,
        to_utc_timestamp(col(columnName), BusinessTimeZone)
      )
    }
  }

  /** Store VN wall-clock digits so Nessie/Trino matches Postgres (+7) display.
    * Lưu số giờ VN (vd 00:03 ngày 04/6) thay vì instant UTC (17:03 ngày 03/6).
    */
  private def toIcebergWallClockStorage(column: Column): Column = {
    to_timestamp(
      concat(
        date_format(from_utc_timestamp(column, BusinessTimeZone), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        lit("Z")
      ),
      BusinessTimestampParsePattern
    )
  }

  private def applyIcebergWallClockStorage(
      df: DataFrame,
      timestampColumns: Seq[String]
  ): DataFrame = {
    if (timestampColumns.isEmpty) {
      df
    } else {
      println(
        s"Storing timestamp columns as Postgres-aligned wall clock in Nessie: ${timestampColumns.mkString(", ")}"
      )
      timestampColumns.foldLeft(df) { (acc, columnName) =>
        acc.withColumn(columnName, toIcebergWallClockStorage(col(columnName)))
      }
    }
  }

  /** Build JDBC subquery with timezone-safe projections / Tạo subquery JDBC chuẩn múi giờ */
  private def buildPostgresSelectSubquery(
      tableRef: String,
      columns: Seq[(String, String, String)],
      whereClause: Option[String]
  ): String = {
    if (columns.isEmpty) {
      throw new IllegalArgumentException(
        s"No columns found in information_schema for PostgreSQL table: $tableRef"
      )
    }
    val timestamptzColumns =
      columns.filter { case (_, dataType, udtName) =>
        isPostgresTimestamptz(dataType, udtName)
      }.map { case (name, _, _) => name }
    if (timestamptzColumns.nonEmpty) {
      println(
        s"Converting timestamptz columns to VN local strings for JDBC read: ${timestamptzColumns.mkString(", ")}"
      )
    }
    val projections =
      columns.map { case (name, dataType, udtName) =>
        projectPostgresColumn(name, dataType, udtName)
      }.mkString(", ")
    val whereSql = whereClause.map(clause => s" WHERE $clause").getOrElse("")
    s"(SELECT $projections FROM $tableRef$whereSql) AS temp_table"
  }

  /** Shared JDBC reader options for PostgreSQL / Tuỳ chọn JDBC dùng chung */
  private def postgresJdbcReader(
      spark: SparkSession,
      config: SparkConfig,
      dbTable: String
  ): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", getPostgresJdbcUrl(config))
      .option("dbtable", dbTable)
      .option("user", config.get("postgres", "username"))
      .option("password", config.get("postgres", "password"))
      .option("driver", "org.postgresql.Driver")
      // Align PG session TZ with business calendar / Đồng bộ session PG với lịch VN
      .option(
        "sessionInitStatement",
        s"SET TIME ZONE '$BusinessTimeZone'"
      )
      .load()
  }

  /** Read table from PostgreSQL (supports SQL wrapping for incremental loads
    * based on watermark). Supports multiple watermark columns separated by
    * comma (treated as OR condition).
    */
  def readFromPostgres(
      spark: SparkSession,
      table: String,
      config: SparkConfig,
      watermarkCol: Option[String] = None,
      minTimestamp: Option[String] = None,
      maxTimestamp: Option[String] = None
  ): DataFrame = {
    val columnMetadata = loadPostgresColumnMetadata(spark, config, table)
    val timestamptzColumns = columnMetadata.collect {
      case (name, dataType, udtName)
          if isPostgresTimestamptz(dataType, udtName) =>
        name
    }
    val naiveTimestampColumns = columnMetadata.collect {
      case (name, dataType, udtName)
          if isPostgresNaiveTimestamp(dataType, udtName) =>
        name
    }.toSet
    val normalizedMin = minTimestamp.map(toPostgresTimestamptzLiteral)
    val normalizedMax = maxTimestamp.map(toPostgresTimestamptzLiteral)
    val whereClause = (watermarkCol, normalizedMin, normalizedMax) match {
      case (Some(colsStr), Some(minTs), Some(maxTs)) =>
        Some(
          colsStr
            .split(",")
            .map(quoteColumn)
            .map { qCol =>
              s"($qCol >= '$minTs'::timestamptz AND $qCol < '$maxTs'::timestamptz)"
            }
            .mkString(" OR ")
        )
      case (Some(colsStr), Some(minTs), None) =>
        Some(
          colsStr
            .split(",")
            .map(quoteColumn)
            .map { qCol =>
              s"$qCol > '$minTs'::timestamptz"
            }
            .mkString(" OR ")
        )
      case _ =>
        None
    }

    val dbTable =
      buildPostgresSelectSubquery(table, columnMetadata, whereClause)

    println(s"Reading from PostgreSQL table/query: $dbTable")
    val rawDf = postgresJdbcReader(spark, config, dbTable)
    val parsedDf = parseBusinessTimestampStrings(rawDf, timestamptzColumns)
    val naiveDf = normalizeNaiveTimestampColumns(parsedDf, naiveTimestampColumns)
    val allTimestampColumns = timestamptzColumns ++ naiveTimestampColumns.toSeq
    applyIcebergWallClockStorage(naiveDf, allTimestampColumns)
  }

  /** Writes DataFrame to Iceberg.
    */
  def writeToIceberg(
      spark: SparkSession,
      df: DataFrame,
      table: String,
      mode: String
  ): Unit = {
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
  }

  /** Read from Iceberg catalog.
    */
  def readFromIceberg(spark: SparkSession, table: String): DataFrame = {
    spark.table(table)
  }

  /** Queries current max watermark value from target Iceberg table. If
    * watermarkCol contains multiple columns, uses the first one (primary) to
    * fetch max value.
    */
  def getMaxWatermark(
      spark: SparkSession,
      table: String,
      watermarkCol: String
  ): Option[String] = {
    if (tableExists(spark, table)) {
      println(
        s"Fetching max watermark from Iceberg table: $table for column: $watermarkCol"
      )
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

  /** True for numeric Spark types used in merge dedupe aggregation / Kiểu số để SUM khi gộp dup */
  private def isNumericMergeField(dataType: DataType): Boolean = dataType match {
    case _: IntegerType | _: LongType | _: ShortType | _: DoubleType |
        _: FloatType | _: DecimalType =>
      true
    case _ => false
  }

  /** Collapse duplicate merge keys in source (SUM metrics, FIRST dimensions).
    * Gộp dòng trùng merge key trước MERGE — tránh SparkException nhiều source match 1 target.
    */
  def dedupeMergeSource(sourceDf: DataFrame, joinKeys: Seq[String]): DataFrame = {
    val hasDuplicateKeys = sourceDf
      .groupBy(joinKeys.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .limit(1)
      .count() > 0

    if (!hasDuplicateKeys) {
      return sourceDf
    }

    val duplicateKeyGroups = sourceDf
      .groupBy(joinKeys.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .count()
    println(
      s"WARNING: Collapsing $duplicateKeyGroups duplicate merge-key group(s) before MERGE (keys: ${joinKeys.mkString(", ")})."
    )

    val nonKeyColumns = sourceDf.columns.filterNot(joinKeys.contains)
    if (nonKeyColumns.isEmpty) {
      return sourceDf.dropDuplicates(joinKeys)
    }

    val aggExprs = nonKeyColumns.map { columnName =>
      val field = sourceDf.schema(columnName)
      if (isNumericMergeField(field.dataType)) {
        sum(col(columnName)).as(columnName)
      } else {
        first(col(columnName), ignoreNulls = true).as(columnName)
      }
    }

    sourceDf.groupBy(joinKeys.map(col): _*).agg(aggExprs.head, aggExprs.tail: _*)
  }

  /** Map Spark type to Iceberg SQL type for ALTER TABLE / Map kiểu Spark sang DDL Iceberg */
  private def sparkTypeToIcebergSql(dataType: DataType): String = dataType match {
    case StringType => "string"
    case IntegerType => "int"
    case LongType => "long"
    case ShortType => "short"
    case DoubleType => "double"
    case FloatType => "float"
    case d: DecimalType => s"decimal(${d.precision},${d.scale})"
    case DateType => "date"
    case TimestampType => "timestamp"
    case BooleanType => "boolean"
    case _ => "string"
  }

  /** Add missing merge-key columns on brownfield Iceberg targets / Thêm cột merge key thiếu trên bảng cũ */
  private def ensureMergeKeyColumnsOnTarget(
      spark: SparkSession,
      targetTable: String,
      sourceDf: DataFrame,
      joinKeys: Seq[String]
  ): Unit = {
    val targetColumnSet = readFromIceberg(spark, targetTable).columns.toSet
    joinKeys.filterNot(targetColumnSet.contains).foreach { key =>
      if (!sourceDf.columns.contains(key)) {
        throw new IllegalArgumentException(
          s"Merge key '$key' is missing from source DataFrame for $targetTable"
        )
      }
      val sqlType = sparkTypeToIcebergSql(sourceDf.schema(key).dataType)
      println(
        s"Evolving target schema: ALTER TABLE $targetTable ADD COLUMN $key $sqlType"
      )
      spark.sql(s"ALTER TABLE $targetTable ADD COLUMN $key $sqlType")
    }
  }

  /** Performs Iceberg MERGE INTO target using join keys from a source
    * DataFrame.
    */
  def mergeIcebergTable(
      spark: SparkSession,
      sourceDf: DataFrame,
      targetTable: String,
      joinKeys: Seq[String]
  ): Unit = {
    val dedupedSource = dedupeMergeSource(sourceDf, joinKeys)
    ensureMergeKeyColumnsOnTarget(spark, targetTable, dedupedSource, joinKeys)

    // Align source columns to target schema / Chỉ merge cột có trong cả source và target
    val targetColumns = readFromIceberg(spark, targetTable).columns
    val sourceColumnSet = dedupedSource.columns.toSet
    val alignedColumns = targetColumns.filter(sourceColumnSet.contains)
    val missingInSource = targetColumns.filterNot(sourceColumnSet.contains)
    if (missingInSource.nonEmpty) {
      println(
        s"WARNING: Target columns missing in source will not be updated: ${missingInSource.mkString(", ")}"
      )
    }
    val missingJoinKeysInAligned =
      joinKeys.filterNot(alignedColumns.contains)
    if (missingJoinKeysInAligned.nonEmpty) {
      throw new IllegalStateException(
        s"Merge keys not present in aligned source/target columns for $targetTable: ${missingJoinKeysInAligned.mkString(", ")}"
      )
    }
    val alignedSource = dedupedSource.select(alignedColumns.map(col): _*)

    val tempViewName =
      s"merge_source_${java.util.UUID.randomUUID().toString.replace("-", "")}"
    alignedSource.createOrReplaceTempView(tempViewName)
    try {
      val joinCondition = joinKeys.map(k => s"t.$k = s.$k").mkString(" AND ")
      val sqlText =
        s"""MERGE INTO $targetTable t
           |USING $tempViewName s
           |ON $joinCondition
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin
      println(
        s"Executing Iceberg MERGE INTO: $targetTable using join condition: $joinCondition"
      )
      spark.sql(sqlText)
    } finally {
      spark.catalog.dropTempView(tempViewName)
    }
  }

  /** Writes a DataFrame to ClickHouse via JDBC.
    */
  def writeToClickHouse(
      df: DataFrame,
      tableName: String,
      config: SparkConfig
  ): Unit = {
    val clickhouseHost = config.get("clickhouse", "host")
    val clickhousePort = config.get("clickhouse", "port")
    val clickhouseDb = config.get("clickhouse", "database")
    val jdbcUrl =
      s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDb"

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

  /** Reads an Elasticsearch resource index into a DataFrame.
    */
  def readFromElasticSearch(
      spark: SparkSession,
      resource: String,
      config: SparkConfig
  ): DataFrame = {
    println(s"Reading from Elasticsearch index: $resource")
    spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", config.get("elasticsearch", "hosts"))
      .option("es.resource", resource)
      .load()
  }

  /** Safe, standalone snapshot expiration for Iceberg tables. Cleans up older
    * snapshot metadata and data files in S3/MinIO.
    */
  def expireTableSnapshots(spark: SparkSession, table: String): Unit = {
    // No-op: We keep historical snapshots now that ClickHouse queries active files natively via the Iceberg engine.
    println(
      s"Skipping snapshot expiration for table: $table (retaining snapshots)."
    )
  }
}
