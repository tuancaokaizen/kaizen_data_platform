package com.kaizen.batch.utils

import java.util.{TimeZone, UUID}

import com.kaizen.batch.utils.iceberg.{IcebergProperties, Partition}
import org.apache.spark.sql.functions.{col, days, lit, months}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/** Spark session and Iceberg writes shared by every job.
  * Spark session và các kiểu ghi Iceberg dùng chung cho mọi job.
  *
  * Table names are written as in Spark: catalog.namespace.table, e.g. nessie.dw.fact_sale_transaction.
  * A missing table is created from the DataFrame schema with the given partition and the default properties.
  * Tên bảng viết như trong Spark: catalog.namespace.table, vd nessie.dw.fact_sale_transaction.
  * Bảng chưa có thì được tạo theo schema của DataFrame với partition truyền vào và thuộc tính mặc định.
  *
  * Schema evolution: new source columns are ADD COLUMN'd before write; overwrite also drops target cols absent from source;
  * overwrite/append pass merge-schema.
  * Tiến hoá schema: cột mới ở nguồn được ADD COLUMN trước khi ghi; overwrite còn DROP cột đích không có trên nguồn;
  * overwrite/append bật merge-schema.
  */
object SparkUtils {
  private val log = LoggerFactory.getLogger(getClass)

  val Catalog: String = "nessie"

  // Iceberg write option that evolves the table schema on append/overwrite / Tuỳ chọn ghi Iceberg tiến hoá schema khi append/overwrite
  private val MergeSchemaOption = "merge-schema" -> "true"

  // MinIO keys come from the pod environment and are passed to the executors through the Spark conf
  // Khoá MinIO lấy từ biến môi trường của pod và truyền xuống executor qua Spark conf
  private val S3CredentialVariables = Seq(
    "AWS_ACCESS_KEY_ID" -> "spark.hadoop.fs.s3a.access.key",
    "AWS_SECRET_ACCESS_KEY" -> "spark.hadoop.fs.s3a.secret.key"
  )

  /** Builder with the Spark settings of the environment (block SPARK_SESSION_<ENVIRONMENT>).
    * Options passed with spark-submit --conf (memory, cores...) win over the conf file.
    * Builder với cấu hình Spark của môi trường (khối SPARK_SESSION_<ENVIRONMENT>).
    * Tham số truyền bằng spark-submit --conf (bộ nhớ, số core...) được ưu tiên hơn file conf.
    */
  def getSpark(appName: String, environment: String): SparkSession.Builder = {
    // Timestamps keep Vietnam wall-clock digits only when the JVM and Spark both run in UTC
    // Timestamp chỉ giữ đúng số giờ Việt Nam khi JVM và Spark cùng chạy UTC
    TimeZone.setDefault(TimeZone.getTimeZone(DateUtils.StorageZoneName))
    System.setProperty("user.timezone", DateUtils.StorageZoneName)

    val credentials = S3CredentialVariables.flatMap { case (variable, key) =>
      sys.env.get(variable).filter(_.nonEmpty).map(key -> _)
    }
    // spark-submit --conf values arrive as system properties; the builder would override them, so they are skipped
    // Giá trị spark-submit --conf đến dưới dạng system property; builder sẽ ghi đè nên bỏ qua chúng
    val settings = (ConfigUtils.section(environment, "SPARK_SESSION") ++ credentials).filterNot { case (key, _) =>
      sys.props.contains(key)
    }
    settings.foldLeft(SparkSession.builder().appName(appName)) { case (builder, (key, value)) =>
      builder.config(key, value)
    }
  }

  def tableExists(spark: SparkSession, table: String): Boolean =
    try {
      spark.table(table).schema
      true
    } catch {
      case _: AnalysisException => false
    }

  /** Creates the table from the DataFrame schema when it does not exist yet.
    * Tạo bảng theo schema của DataFrame nếu chưa có.
    */
  def createTableIfNotExists(spark: SparkSession, rows: DataFrame, table: String, partition: Partition): Unit =
    if (!tableExists(spark, table)) {
      val namespace = table.split('.').dropRight(1).map(part => s"`$part`").mkString(".")
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $namespace")
      val writer = IcebergProperties.Default.foldLeft(rows.limit(0).writeTo(table).using("iceberg")) {
        case (builder, (key, value)) => builder.tableProperty(key, value)
      }
      log.info(s"Create $table partitioned by $partition")
      partition match {
        case Partition.Unpartitioned  => writer.create()
        case Partition.Months(column) => writer.partitionedBy(months(col(column))).create()
        case Partition.Days(column)   => writer.partitionedBy(days(col(column))).create()
        case Partition.Identity(column) => writer.partitionedBy(col(column)).create()
      }
    }

  /** Replaces the whole content of the table. An empty result fails unless allowEmpty, so a broken source never wipes a dimension.
    * Thay toàn bộ nội dung bảng. Kết quả rỗng báo lỗi trừ khi allowEmpty, để nguồn hỏng không xoá sạch dimension.
    */
  def overwriteTable(
      spark: SparkSession,
      rows: DataFrame,
      table: String,
      partition: Partition = Partition.Unpartitioned,
      allowEmpty: Boolean = false
  ): Unit = {
    require(allowEmpty || !rows.isEmpty, s"$table: the new content is empty, refusing to overwrite")
    createTableIfNotExists(spark, rows, table, partition)
    evolveSchema(spark, rows, table)
    // Bronze overwrite: source schema is authoritative — drop Iceberg cols not in this write
    // Overwrite bronze: schema nguồn là chuẩn — xoá cột Iceberg không có trong lần ghi này
    dropObsoleteColumns(spark, rows, table)
    log.info(s"Overwrite $table")
    rows.writeTo(table).option(MergeSchemaOption._1, MergeSchemaOption._2).overwrite(lit(true))
  }

  /** Replaces the days [fromDate, toDate) of a report: deletes them, then appends the new rows of those days.
    * Rows of other days are dropped, so a run never touches dates outside its range.
    * Thay các ngày [fromDate, toDate) của báo cáo: xoá chúng rồi thêm dòng mới của các ngày đó.
    * Dòng của ngày khác bị bỏ, để một lần chạy không bao giờ đụng ngày ngoài khoảng.
    */
  def replaceDates(
      spark: SparkSession,
      rows: DataFrame,
      table: String,
      dateColumn: String,
      fromDate: String,
      toDate: String,
      partition: Partition
  ): Unit = {
    DateUtils.checkDateRange(fromDate, toDate)
    createTableIfNotExists(spark, rows, table, partition)
    evolveSchema(spark, rows, table)
    log.info(s"Replace $table days [$fromDate, $toDate)")
    // Iceberg DELETE has no DataFrame API / DELETE của Iceberg không có DataFrame API
    spark.sql(s"DELETE FROM $table WHERE `$dateColumn` >= DATE '$fromDate' AND `$dateColumn` < DATE '$toDate'")
    rows
      .where(DateUtils.inDateRange(col(dateColumn), fromDate, toDate))
      .writeTo(table)
      .option(MergeSchemaOption._1, MergeSchemaOption._2)
      .append()
  }

  /** MERGE by key: updates matched rows, inserts new ones. The source must hold at most one row per key.
    * Source DataFrame columns are the catalog / explicit SELECT list (never SELECT * from Iceberg).
    * Target is aligned to that list: ADD new source columns, DROP legacy target columns Iceberg MERGE INSERT forbids omitting.
    * With newerColumn, a matched row is updated only when the source value is not older than the target value.
    * MERGE theo khoá: cập nhật dòng khớp, thêm dòng mới. Nguồn chỉ được có tối đa một dòng mỗi khoá.
    * Cột DataFrame nguồn là catalog / SELECT tường minh (không SELECT * từ Iceberg).
    * Đích được căn theo danh sách đó: ADD cột nguồn mới, DROP cột thừa cũ vì MERGE INSERT của Iceberg bắt buộc đủ cột.
    * Có newerColumn thì dòng khớp chỉ được cập nhật khi giá trị nguồn không cũ hơn giá trị đích.
    */
  def mergeInto(
      spark: SparkSession,
      rows: DataFrame,
      table: String,
      keys: Seq[String],
      partition: Partition,
      newerColumn: Option[String] = None
  ): Unit = {
    require(keys.nonEmpty, s"MERGE into $table needs a key")
    val missingKeys = keys.filterNot(rows.columns.contains)
    require(missingKeys.isEmpty, s"MERGE into $table: source lacks key columns ${missingKeys.mkString(", ")}")
    createTableIfNotExists(spark, rows, table, partition)
    evolveSchema(spark, rows, table)
    // Align Iceberg to catalog SELECT list before MERGE INSERT / Căn Iceberg theo danh sách SELECT catalog trước MERGE INSERT
    dropObsoleteColumns(spark, rows, table)
    val columns = rows.columns.toSeq

    // Iceberg MERGE has no DataFrame API in Spark 3.4, so the source goes through a temporary view
    // MERGE của Iceberg chưa có DataFrame API trong Spark 3.4, nên nguồn đi qua một temporary view
    val view = s"merge_source_${UUID.randomUUID().toString.replace("-", "")}"
    rows.select(columns.map(name => col(s"`$name`")): _*).createOrReplaceTempView(view)
    try {
      val on = keys.map(name => s"t.`$name` = s.`$name`").mkString(" AND ")
      val newer = newerColumn.map(name => s" AND (t.`$name` IS NULL OR s.`$name` >= t.`$name`)").getOrElse("")
      val update = columns.filterNot(keys.contains).map(name => s"t.`$name` = s.`$name`")
      // Key-only tables have nothing to update / Bảng chỉ có khoá thì không có gì để cập nhật
      val updateClause = if (update.isEmpty) "" else s"WHEN MATCHED$newer THEN UPDATE SET ${update.mkString(", ")}\n"
      val insertColumns = columns.map(name => s"`$name`").mkString(", ")
      val insertValues = columns.map(name => s"s.`$name`").mkString(", ")
      log.info(s"Merge into $table on ${keys.mkString(", ")} columns=${columns.mkString(",")}")
      spark.sql(
        s"""MERGE INTO $table t
           |USING $view s
           |ON $on
           |${updateClause}WHEN NOT MATCHED THEN INSERT ($insertColumns) VALUES ($insertValues)""".stripMargin
      )
    } finally {
      spark.catalog.dropTempView(view)
    }
  }

  /** Adds source columns missing on the target; fails on type changes. Does not drop or rename columns.
    * Thêm cột nguồn còn thiếu trên đích; đổi kiểu thì báo lỗi. Không xoá hay đổi tên cột.
    */
  def evolveSchema(spark: SparkSession, rows: DataFrame, table: String): Unit = {
    val targetFields = spark.table(table).schema.fields.map(field => field.name -> field.dataType).toMap
    val sourceFields = rows.schema.fields

    // Reject incompatible type changes on shared columns / Từ chối đổi kiểu không tương thích trên cột chung
    sourceFields.foreach { field =>
      targetFields.get(field.name).foreach { targetType =>
        require(
          field.dataType == targetType,
          s"$table.${field.name}: cannot change type from $targetType to ${field.dataType}"
        )
      }
    }

    // ADD COLUMN for each new source field / ADD COLUMN cho mỗi trường nguồn mới
    sourceFields.filterNot(field => targetFields.contains(field.name)).foreach { field =>
      val sqlType = sparkTypeToIcebergSql(field.dataType)
      log.info(s"Evolve $table: ADD COLUMN `${field.name}` $sqlType")
      spark.sql(s"ALTER TABLE $table ADD COLUMN `${field.name}` $sqlType")
    }
  }

  /** Drops Iceberg columns absent from the source DataFrame (catalog / SELECT list).
    * Xoá cột Iceberg không có trên DataFrame nguồn (catalog / danh sách SELECT).
    */
  def dropObsoleteColumns(spark: SparkSession, rows: DataFrame, table: String): Unit = {
    val sourceNames = rows.columns.toSet
    spark.table(table).schema.fields.map(_.name).filterNot(sourceNames.contains).foreach { name =>
      log.info(s"Evolve $table: DROP COLUMN `$name` (absent from source)")
      spark.sql(s"ALTER TABLE $table DROP COLUMN `$name`")
    }
  }

  /** Map Spark type to Iceberg SQL type for ALTER TABLE / Map kiểu Spark sang DDL Iceberg */
  private def sparkTypeToIcebergSql(dataType: DataType): String = dataType match {
    case StringType      => "string"
    case IntegerType     => "int"
    case LongType        => "long"
    case ShortType       => "short"
    case DoubleType      => "double"
    case FloatType       => "float"
    case d: DecimalType  => s"decimal(${d.precision},${d.scale})"
    case DateType        => "date"
    case TimestampType   => "timestamp"
    case BooleanType     => "boolean"
    case BinaryType      => "binary"
    case other           =>
      throw new IllegalArgumentException(s"Unsupported Iceberg column type for schema evolution: $other")
  }

  /** Tables of a namespace, empty when the namespace does not exist. / Các bảng của namespace, rỗng nếu chưa có. */
  def listTables(spark: SparkSession, namespace: String): Seq[String] =
    if (!spark.sql(s"SHOW NAMESPACES IN `$Catalog`").collect().exists(_.getString(0) == namespace)) Seq.empty
    else spark.sql(s"SHOW TABLES IN `$Catalog`.`$namespace`").collect().map(_.getAs[String]("tableName")).toSeq.sorted

  /** Expires snapshots older than olderThanDays, keeping at least retainLast.
    * Xoá snapshot cũ hơn olderThanDays, giữ tối thiểu retainLast.
    */
  def expireSnapshots(spark: SparkSession, namespace: String, table: String, retainLast: Int, olderThanDays: Int): Unit = {
    log.info(s"Expire snapshots of $namespace.$table older than $olderThanDays days, keep $retainLast")
    spark.sql(
      s"""CALL `$Catalog`.system.expire_snapshots(
         |  table => '`$namespace`.`$table`',
         |  older_than => TIMESTAMP_MILLIS(${millisAgo(olderThanDays)}),
         |  retain_last => $retainLast
         |)""".stripMargin
    )
  }

  /** Merges small data files into files of about targetFileBytes. / Gộp file nhỏ thành file khoảng targetFileBytes. */
  def compactTable(spark: SparkSession, namespace: String, table: String, targetFileBytes: Long): Unit = {
    log.info(s"Compact $namespace.$table")
    spark.sql(
      s"""CALL `$Catalog`.system.rewrite_data_files(
         |  table => '`$namespace`.`$table`',
         |  options => map('target-file-size-bytes', '$targetFileBytes')
         |)""".stripMargin
    )
  }

  /** Deletes files under the table folder that no snapshot references and that are older than olderThanDays.
    * Xoá file trong thư mục bảng không snapshot nào tham chiếu và cũ hơn olderThanDays.
    */
  def removeOrphanFiles(spark: SparkSession, namespace: String, table: String, olderThanDays: Int): Unit = {
    log.info(s"Remove orphan files of $namespace.$table older than $olderThanDays days")
    spark.sql(
      s"""CALL `$Catalog`.system.remove_orphan_files(
         |  table => '`$namespace`.`$table`',
         |  older_than => TIMESTAMP_MILLIS(${millisAgo(olderThanDays)})
         |)""".stripMargin
    )
  }

  /** Drops a table with its files. / Xoá bảng cùng file dữ liệu. */
  def dropTable(spark: SparkSession, namespace: String, table: String): Unit = {
    log.warn(s"Drop table $Catalog.$namespace.$table with its files")
    spark.sql(s"DROP TABLE IF EXISTS `$Catalog`.`$namespace`.`$table` PURGE")
  }

  private def millisAgo(days: Int): Long = System.currentTimeMillis() - days.toLong * 24L * 60L * 60L * 1000L
}
