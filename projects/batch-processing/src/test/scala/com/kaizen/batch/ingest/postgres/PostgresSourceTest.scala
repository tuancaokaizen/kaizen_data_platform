package com.kaizen.batch.ingest.postgres

import com.kaizen.batch.config.SourceTableCatalog
import com.kaizen.batch.testing.LocalSpark
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PostgresSourceTest extends AnyFunSuite {
  private lazy val catalog = SourceTableCatalog.load()

  private val paymentTypes = Map(
    "Id" -> "text",
    "PaymentType" -> "int2",
    "Status" -> "text",
    "OrderId" -> "text",
    "ReturnId" -> "text",
    "ShopCode" -> "text",
    "TotalPayment" -> "int8",
    "Attempt" -> "int2",
    "PaymentDate" -> "date",
    "CreatedDate" -> "timestamptz",
    "ModifiedDate" -> "timestamptz",
    "Unlisted" -> "text"
  )

  test("incremental read selects declared columns, timestamps as Vietnam text, inside the load window") {
    val query = PostgresSource.selectQuery(catalog.table("payment"), paymentTypes, "2026-10-14", "2026-10-15")
    assert(query.startsWith("SELECT \"Id\", \"PaymentType\""))
    assert(query.contains("to_char(\"ModifiedDate\" AT TIME ZONE 'Asia/Ho_Chi_Minh', 'YYYY-MM-DD HH24:MI:SS.US') AS \"ModifiedDate\""))
    assert(query.contains(
      "FROM \"payment\".\"Payment\" WHERE \"ModifiedDate\" >= '2026-10-13 23:00:00+07'::timestamptz" +
        " AND \"ModifiedDate\" < '2026-10-15 00:00:00+07'::timestamptz"
    ))
    assert(!query.contains("Unlisted"))
  }

  test("full read has no filter") {
    val types = Map("ProductId" -> "text", "PrimaryFunction" -> "text", "Brand" -> "text")
    assert(PostgresSource.selectQuery(catalog.table("product_supplement"), types, "2026-10-14", "2026-10-15") ==
      "SELECT \"ProductId\", \"PrimaryFunction\", \"Brand\" FROM \"product\".\"ProductSupplement\"")
  }

  test("a declared column missing in Postgres or of another type fails before reading") {
    val missing = intercept[IllegalArgumentException](
      PostgresSource.selectQuery(catalog.table("payment"), paymentTypes - "Attempt", "2026-10-14", "2026-10-15")
    )
    assert(missing.getMessage.contains("has no column Attempt"))
    val wrongType = intercept[IllegalArgumentException](
      PostgresSource.selectQuery(catalog.table("payment"), paymentTypes.updated("TotalPayment", "text"), "2026-10-14", "2026-10-15")
    )
    assert(wrongType.getMessage.contains("TotalPayment long vs text"))
  }

  test("Vietnam timestamp text keeps its digits in the lakehouse") {
    val spark = LocalSpark.session
    import spark.implicits._
    val table = catalog.table("payment_type")
    val rows = Seq(("3", "Thẻ", "2026-10-15 08:30:00.123456", "2026-10-15 23:59:59.000000"))
      .toDF("Type", "Name", "CreatedDate", "ModifiedDate")
    val typed = PostgresSource.toDeclaredTypes(rows, table)
    assert(typed.schema.map(field => field.name -> field.dataType.simpleString) ==
      Seq("Type" -> "int", "Name" -> "string", "CreatedDate" -> "timestamp", "ModifiedDate" -> "timestamp"))
    val stored = typed.collect().head
    assert(stored.getTimestamp(2).toString == "2026-10-15 08:30:00.123456")
    assert(stored.getTimestamp(3).toString == "2026-10-15 23:59:59.0")
  }
}
