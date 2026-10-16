package com.kaizen.batch.config

import java.nio.charset.StandardCharsets

import com.kaizen.batch.utils.iceberg.{ColumnDefinition, ColumnType, Partition}
import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.{DumperOptions, LoaderOptions, Yaml}

import scala.collection.JavaConverters._
import scala.io.Source

/** How a source table is read each run. / Cách đọc bảng nguồn mỗi lần chạy. */
sealed abstract class LoadMode(val name: String)

object LoadMode {
  // Whole table each run (small master data) / Cả bảng mỗi lần chạy (master data nhỏ)
  case object Full extends LoadMode("full")
  // Rows whose modified column falls in the run window / Dòng có cột sửa đổi nằm trong cửa sổ chạy
  case object Incremental extends LoadMode("incremental")

  def parse(text: String): Either[String, LoadMode] = text match {
    case Full.name        => Right(Full)
    case Incremental.name => Right(Incremental)
    case other            => Left(s"load must be full or incremental, got '$other'")
  }
}

/** One entry of tables.yaml: a Postgres table copied into stg.<name> and fnd.<name>.
  * Một mục của tables.yaml: bảng Postgres chép vào stg.<name> và fnd.<name>.
  */
final case class SourceTable(
    name: String,
    group: String,
    sourceSchema: String,
    sourceTable: String,
    load: LoadMode,
    key: Seq[String],
    modifiedColumn: Option[String],
    partition: Partition,
    columns: Seq[ColumnDefinition]
) {

  /** Quoted Postgres name, e.g. "order"."Order". / Tên Postgres có nháy, vd "order"."Order". */
  def postgresName: String = s""""$sourceSchema"."$sourceTable""""
}

/** Source tables declared in tables.yaml. / Danh mục bảng nguồn khai báo trong tables.yaml. */
final case class SourceTableCatalog(tables: Seq[SourceTable]) {

  def group(name: String): Seq[SourceTable] = tables.filter(_.group == name)

  def table(name: String): SourceTable =
    tables.find(_.name == name).getOrElse(throw new NoSuchElementException(s"Unknown source table '$name'"))

  /** Tables chosen by the job arguments, in group order then file order.
    * Bảng được chọn theo tham số job, theo thứ tự nhóm rồi thứ tự trong file.
    *
    * @param groups "all" or comma-separated groups, e.g. "master,sales" / "all" hoặc các nhóm cách nhau dấu phẩy
    * @param tableNames empty for every table of the groups, or comma-separated names, e.g. "order,order_item"
    *                   rỗng là mọi bảng của nhóm, hoặc tên bảng cách nhau dấu phẩy
    */
  def select(groups: String, tableNames: String): Seq[SourceTable] = {
    val requestedGroups = SourceTableCatalog.splitList(groups)
    val chosenGroups = if (requestedGroups == Seq("all")) SourceTableCatalog.Groups else requestedGroups
    val unknownGroups = chosenGroups.filterNot(SourceTableCatalog.Groups.contains)
    require(
      chosenGroups.nonEmpty && unknownGroups.isEmpty,
      s"groups must be 'all' or some of ${SourceTableCatalog.Groups.mkString(", ")}, got '$groups'"
    )
    val inGroups = SourceTableCatalog.Groups.filter(chosenGroups.contains).flatMap(group)

    val requestedTables = SourceTableCatalog.splitList(tableNames)
    val unknownTables = requestedTables.filterNot(name => inGroups.exists(_.name == name))
    require(unknownTables.isEmpty, s"tables ${unknownTables.mkString(", ")} are not in groups ${chosenGroups.mkString(", ")}")
    if (requestedTables.isEmpty) inGroups else inGroups.filter(table => requestedTables.contains(table.name))
  }
}

object SourceTableCatalog {
  val ResourceName: String = "tables.yaml"

  // Ingest groups in run order / Nhóm ingest theo thứ tự chạy
  val Groups: Seq[String] = Seq("master", "sales", "payment", "inventory", "supply", "hr")

  // Postgres schemas holding business data; datagen holds simulation parameters only
  // Schema Postgres chứa dữ liệu nghiệp vụ; datagen chỉ chứa tham số giả lập
  val BusinessSchemas: Set[String] =
    Set("public", "shop", "customer", "product", "promotion", "order", "payment", "inventory", "logistics", "procurement")

  private val Fields = Set("table", "group", "source", "load", "key", "modified_column", "partition", "columns")
  private val TableNamePattern = "^[a-z][a-z0-9]*(_[a-z0-9]+)*$".r
  private val ColumnNamePattern = "^[A-Z][A-Za-z0-9]*$".r
  private val SourcePattern = "^([a-z_]+)\\.([A-Z][A-Za-z0-9]*)$".r

  /** "a, b,,c" → Seq(a, b, c). */
  def splitList(text: String): Seq[String] = text.split(',').map(_.trim).filter(_.nonEmpty).toSeq

  /** Reads tables.yaml from the classpath. / Đọc tables.yaml từ classpath. */
  def load(): SourceTableCatalog = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(ResourceName))
      .getOrElse(throw new IllegalStateException(s"$ResourceName is missing from the classpath"))
    val text =
      try Source.fromInputStream(stream, StandardCharsets.UTF_8.name()).mkString
      finally stream.close()
    parse(text)
  }

  /** Parses and validates the YAML text; every problem is reported at once.
    * Đọc và kiểm tra YAML; mọi lỗi được báo cùng lúc.
    */
  def parse(text: String): SourceTableCatalog = {
    val entries = readEntries(text)
    val results = entries.zipWithIndex.map { case (entry, index) => readTable(entry, index) }
    val tables = results.collect { case Right(table) => table }
    val errors = results.collect { case Left(problems) => problems }.flatten ++ catalogErrors(tables)
    if (errors.nonEmpty) {
      throw new IllegalArgumentException(s"$ResourceName is invalid:\n  - ${errors.mkString("\n  - ")}")
    }
    SourceTableCatalog(tables)
  }

  private def readEntries(text: String): Seq[Map[String, Any]] = {
    // Safe constructor, duplicate keys rejected / Constructor an toàn, từ chối khoá trùng
    val loaderOptions = new LoaderOptions()
    loaderOptions.setAllowDuplicateKeys(false)
    val yaml = new Yaml(new SafeConstructor(), new Representer(), new DumperOptions(), loaderOptions)
    yaml.load[AnyRef](text) match {
      case list: java.util.List[_] =>
        list.asScala.toSeq.map {
          case map: java.util.Map[_, _] => map.asScala.toMap.map { case (key, value) => key.toString -> value }
          case other                    => throw new IllegalArgumentException(s"$ResourceName: entry is not a map: $other")
        }
      case null  => Seq.empty
      case other => throw new IllegalArgumentException(s"$ResourceName: root must be a list, got ${other.getClass}")
    }
  }

  private def readTable(entry: Map[String, Any], index: Int): Either[Seq[String], SourceTable] = {
    val label = entry.get("table").map(name => s"table '$name'").getOrElse(s"entry #${index + 1}")
    var errors = Seq.empty[String]
    def fail(message: String): Unit = errors :+= s"$label: $message"

    def text(field: String): Option[String] = entry.get(field) match {
      case Some(value: String) => Some(value)
      case Some(other)         => fail(s"$field must be text, got '$other'"); None
      case None                => None
    }
    def required(field: String): String = text(field).getOrElse { fail(s"$field is required"); "" }

    // Unknown fields are usually typos / Trường lạ thường là gõ nhầm
    (entry.keySet -- Fields).foreach(field => fail(s"unknown field '$field'"))

    val name = required("table")
    if (name.nonEmpty && TableNamePattern.findFirstIn(name).isEmpty) fail("table must be snake_case")

    val group = required("group")
    if (group.nonEmpty && !Groups.contains(group)) fail(s"group must be one of ${Groups.mkString(", ")}")

    val (sourceSchema, sourceTable) = required("source") match {
      case SourcePattern(schema, table) => (schema, table)
      case ""                           => ("", "")
      case other                        => fail(s"source must be <schema>.<Table>, got '$other'"); ("", "")
    }
    if (sourceSchema == "datagen") fail("schema datagen holds simulation parameters and is never ingested")
    else if (sourceSchema.nonEmpty && !BusinessSchemas.contains(sourceSchema)) {
      fail(s"source schema '$sourceSchema' is not a business schema")
    }

    val load = LoadMode.parse(required("load")).fold(message => { fail(message); LoadMode.Full }, identity)

    val columns: Seq[ColumnDefinition] = entry.get("columns") match {
      case Some(map: java.util.Map[_, _]) =>
        map.asScala.toSeq.map { case (columnName, columnType) =>
          val parsedType = ColumnType
            .parse(String.valueOf(columnType))
            .fold(message => { fail(s"column $columnName: $message"); ColumnType.StringValue }, identity)
          ColumnDefinition(columnName.toString, parsedType)
        }
      case Some(other) => fail(s"columns must be a map of name: type, got '$other'"); Seq.empty
      case None        => fail("columns is required"); Seq.empty
    }
    if (columns.isEmpty) fail("columns must not be empty")
    columns.map(_.name).filter(ColumnNamePattern.findFirstIn(_).isEmpty).foreach { column =>
      fail(s"column $column must be PascalCase")
    }
    val columnTypes = columns.map(column => column.name -> column.columnType).toMap

    val key: Seq[String] = entry.get("key") match {
      case Some(list: java.util.List[_]) => list.asScala.toSeq.map(_.toString)
      case Some(other)                   => fail(s"key must be a list, got '$other'"); Seq.empty
      case None                          => fail("key is required"); Seq.empty
    }
    if (key.isEmpty) fail("key must not be empty")
    key.filterNot(columnTypes.contains).foreach(column => fail(s"key column $column is not in columns"))

    val modifiedColumn = text("modified_column")
    modifiedColumn.foreach { column =>
      columnTypes.get(column) match {
        case Some(ColumnType.TimestampValue) =>
        case Some(other)                     => fail(s"modified_column $column must be a timestamp, got ${other.sqlName}")
        case None                            => fail(s"modified_column $column is not in columns")
      }
    }
    if (load == LoadMode.Incremental && modifiedColumn.isEmpty) fail("incremental load needs modified_column")

    val partition = Partition
      .parse(text("partition").getOrElse("none"))
      .fold(message => { fail(message); Partition.Unpartitioned }, identity)
    partition.column.foreach { column =>
      columnTypes.get(column) match {
        case None => fail(s"partition column $column is not in columns")
        case Some(columnType) if partition.isInstanceOf[Partition.Months] || partition.isInstanceOf[Partition.Days] =>
          if (!columnType.isTemporal) fail(s"partition column $column must be a date or timestamp")
        case Some(_) =>
      }
    }

    if (errors.nonEmpty) Left(errors)
    else Right(SourceTable(name, group, sourceSchema, sourceTable, load, key, modifiedColumn, partition, columns))
  }

  private def catalogErrors(tables: Seq[SourceTable]): Seq[String] = {
    val duplicateNames = tables.groupBy(_.name).collect { case (name, copies) if copies.size > 1 => name }
    val duplicateSources = tables.groupBy(_.postgresName).collect { case (source, copies) if copies.size > 1 => source }
    duplicateNames.toSeq.sorted.map(name => s"table '$name' is declared more than once") ++
      duplicateSources.toSeq.sorted.map(source => s"source $source is declared more than once")
  }
}
