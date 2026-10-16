package com.kaizen.batch.config

import com.kaizen.batch.utils.iceberg.{ColumnType, Partition}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SourceTableCatalogTest extends AnyFunSuite {
  private lazy val catalog = SourceTableCatalog.load()

  private def invalid(yaml: String): String =
    intercept[IllegalArgumentException](SourceTableCatalog.parse(yaml)).getMessage

  private val validEntry =
    """- table: order
      |  group: sales
      |  source: order.Order
      |  load: incremental
      |  key: [Id]
      |  modified_column: ModifiedDate
      |  partition: months(OrderDate)
      |  columns:
      |    Id: string
      |    OrderDate: date
      |    TotalPayment: long
      |    ModifiedDate: timestamp
      |""".stripMargin

  test("tables.yaml declares 37 tables in the agreed groups") {
    assert(catalog.tables.size == 37)
    val sizes = SourceTableCatalog.Groups.map(group => group -> catalog.group(group).size).toMap
    assert(sizes == Map("master" -> 19, "sales" -> 5, "payment" -> 2, "inventory" -> 2, "supply" -> 8, "hr" -> 1))
  }

  test("rule, derivable and technical tables are not ingested") {
    val sources = catalog.tables.map(_.sourceTable).toSet
    val excluded = Set(
      "ReplenishmentPolicy",
      "SupplierProduct",
      "CarrierRate",
      "Lane",
      "DeliverySchedule",
      "DistanceMatrix",
      "InventoryBalance",
      "OrderPayment",
      "ReturnPayment",
      "Outbox",
      "PostingCutover"
    )
    assert((sources intersect excluded).isEmpty)
    assert(!catalog.tables.exists(_.sourceSchema == "datagen"))
  }

  test("personal data stays in Postgres") {
    assert((catalog.table("customer").columns.map(_.name).toSet intersect Set("FullName", "Phone", "Address")).isEmpty)
    assert((catalog.table("employee").columns.map(_.name).toSet intersect Set("Phone", "Address")).isEmpty)
  }

  test("large tables load incrementally, partitioned facts read the business date") {
    assert(catalog.table("customer").load == LoadMode.Incremental)
    assert(catalog.table("order").partition == Partition.Months("OrderDate"))
    assert(catalog.table("inventory_snapshot").key == Seq("SnapshotDate", "LocationType", "LocationId", "ProductId"))
    assert(catalog.table("shipment").columns.find(_.name == "TotalVolumeM3").map(_.columnType).contains(ColumnType.DecimalValue(8, 3)))
  }

  test("a valid entry parses") {
    val table = SourceTableCatalog.parse(validEntry).table("order")
    assert(table.postgresName == "\"order\".\"Order\"")
    assert(table.columns.map(_.name) == Seq("Id", "OrderDate", "TotalPayment", "ModifiedDate"))
  }

  test("datagen tables are rejected") {
    assert(invalid(validEntry.replace("source: order.Order", "source: datagen.GeneratorConfig")).contains("datagen"))
  }

  test("key, modified column and partition must be declared columns") {
    assert(invalid(validEntry.replace("key: [Id]", "key: [OrderId]")).contains("key column OrderId is not in columns"))
    assert(invalid(validEntry.replace("modified_column: ModifiedDate", "modified_column: UpdatedAt")).contains("modified_column UpdatedAt"))
    assert(invalid(validEntry.replace("months(OrderDate)", "months(TotalPayment)")).contains("must be a date or timestamp"))
  }

  test("names follow the conventions") {
    assert(invalid(validEntry.replace("table: order", "table: OrderTable")).contains("snake_case"))
    assert(invalid(validEntry.replace("    Id: string", "    order_id: string")).contains("PascalCase"))
  }

  test("incremental load needs a modified column") {
    assert(invalid(validEntry.replace("  modified_column: ModifiedDate\n", "")).contains("incremental load needs modified_column"))
  }

  test("unknown fields, types and duplicates are reported") {
    assert(invalid(validEntry.replace("load: incremental", "load: incremental\n  watermark: ModifiedDate")).contains("unknown field 'watermark'"))
    assert(invalid(validEntry.replace("TotalPayment: long", "TotalPayment: money")).contains("unknown column type"))
    assert(invalid(validEntry + validEntry).contains("declared more than once"))
    assert(intercept[Exception](SourceTableCatalog.parse(validEntry.replace("    OrderDate: date", "    Id: date"))) != null)
  }
}
