package com.kaizen.batch.processing

import com.kaizen.batch.processing.dim._
import com.kaizen.batch.processing.fact.{FactPaymentTransaction, FactReturnTransaction, FactSaleTransaction}
import com.kaizen.batch.processing.report.financial.{FinancialReport, FinancialReportDetail, FinancialReportOrder, FinancialReportProduct}
import com.kaizen.batch.processing.report.fund.{FundReport, FundReportOrder, FundReportShop}
import com.kaizen.batch.processing.silver.BronzeToSilver
import com.kaizen.batch.testing.{LocalSpark, SourceRows}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

/** Runs the jobs in Airflow order on the LOCAL catalog: stg → fnd → dw → serving, for one small day of business.
  * Chạy các job theo thứ tự Airflow trên catalog LOCAL: stg → fnd → dw → serving, cho một ngày kinh doanh nhỏ.
  *
  * Day 2026-10-14 at SHOP1 / Ngày 2026-10-14 tại SHOP1:
  *   order O1: 2 × P1 at 100 (unit cost 60) + 1 gift P2 (unit cost 30), paid 150 cash + 50 card
  *   return R1: 1 × P1 of O1, refunded 100 cash
  */
@RunWith(classOf[JUnitRunner])
class LakehousePipelineTest extends AnyFunSuite {
  private lazy val spark = LocalSpark.session
  private val FromDate = "2026-10-14"
  private val ToDate = "2026-10-15"
  private val Time = "2026-10-14 10:00:00"

  private def writeBronze(table: String, rows: Seq[Map[String, Any]]): Unit =
    SourceRows(spark, table, rows).writeTo(s"nessie.stg.$table").createOrReplace()

  private def loadBusinessDay(): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.stg")
    writeBronze("geography", Seq(Map("Id" -> "W1", "Name" -> "Phường 1", "ProvinceCode" -> "79", "ModifiedDate" -> Time)))
    writeBronze("employee", Seq(
      Map("Id" -> "E1", "Name" -> "An", "Status" -> true, "IsManager" -> true, "ShopCode" -> "SHOP1", "ModifiedDate" -> Time)
    ))
    writeBronze("shop", Seq(Map("Code" -> "SHOP1", "Name" -> "Shop One", "WardId" -> "W1", "ManagerId" -> "E1", "ModifiedDate" -> Time)))
    writeBronze("shop_capacity", Seq(Map("Id" -> "SC1", "ShopCode" -> "SHOP1", "StorageM3" -> 12.5, "EffectiveFrom" -> "2026-01-01", "ModifiedDate" -> Time)))
    writeBronze("customer", Seq(Map("Id" -> "C1", "WardId" -> "W1", "YearOfBirth" -> 1990, "ModifiedDate" -> Time)))
    writeBronze("product", Seq(
      Map("Id" -> "P1", "Name" -> "Paracetamol", "Type" -> "medicine", "RetailPrice" -> 100, "Status" -> true, "ModifiedDate" -> Time),
      Map("Id" -> "P2", "Name" -> "Vitamin C", "Type" -> "supplement", "RetailPrice" -> 30, "Status" -> true, "ModifiedDate" -> Time)
    ))
    writeBronze("product_medicine", Seq(Map("ProductId" -> "P1", "IsPrescription" -> false)))
    writeBronze("product_supplement", Seq(Map("ProductId" -> "P2", "Brand" -> "Kaizen")))
    writeBronze("promotion", Seq(Map("Code" -> "PR1", "Name" -> "Gift", "Status" -> true, "ModifiedDate" -> Time)))
    writeBronze("voucher", Seq(Map("Code" -> "V1", "Name" -> "Voucher", "PromotionCode" -> "PR1", "VoucherAmount" -> 10, "ModifiedDate" -> Time)))
    writeBronze("voucher_series", Seq(Map("SeriesCode" -> "S1", "VoucherCode" -> "V1", "Status" -> "ISSUED", "ModifiedDate" -> Time)))
    writeBronze("payment_method", Seq(
      Map("Id" -> 1, "Name" -> "Cash", "ModifiedDate" -> Time),
      Map("Id" -> 2, "Name" -> "Card", "ModifiedDate" -> Time)
    ))

    writeBronze("order", Seq(Map(
      "Id" -> "O1", "ShopCode" -> "SHOP1", "CustomerId" -> "C1", "EmployeeSaleId" -> "E1", "TransactionDate" -> Time,
      "OrderDate" -> FromDate, "Status" -> "Completed", "ModifiedDate" -> Time
    )))
    writeBronze("order_item", Seq(
      Map("Id" -> "I1", "OrderId" -> "O1", "LineNo" -> 1, "ProductId" -> "P1", "Quantity" -> 2, "UnitPrice" -> 100,
        "DiscountAmount" -> 0, "LineAmount" -> 200, "IsGift" -> false, "UnitCost" -> 60, "OrderDate" -> FromDate, "ModifiedDate" -> Time),
      Map("Id" -> "I2", "OrderId" -> "O1", "LineNo" -> 2, "ProductId" -> "P2", "Quantity" -> 1, "UnitPrice" -> 0,
        "DiscountAmount" -> 0, "LineAmount" -> 0, "IsGift" -> true, "UnitCost" -> 30, "OrderDate" -> FromDate, "ModifiedDate" -> Time)
    ))
    writeBronze("return", Seq(Map(
      "Id" -> "R1", "OrderId" -> "O1", "ShopCode" -> "SHOP1", "ReturnDate" -> FromDate, "Reason" -> "Wrong item", "ModifiedDate" -> Time
    )))
    writeBronze("return_item", Seq(Map(
      "Id" -> "RI1", "ReturnId" -> "R1", "OrderId" -> "O1", "OrderItemId" -> "I1", "ProductId" -> "P1", "Quantity" -> 1,
      "RestockQty" -> 1, "DamageQty" -> 0, "UnitPrice" -> 100, "UnitCost" -> 60, "LineAmount" -> 100, "ReturnDate" -> FromDate,
      "ModifiedDate" -> Time
    )))
    writeBronze("payment", Seq(
      Map("Id" -> "PAY1", "PaymentType" -> 1, "Status" -> "SUCCESS", "OrderId" -> "O1", "ShopCode" -> "SHOP1",
        "TotalPayment" -> 200, "Attempt" -> 1, "PaymentDate" -> FromDate, "CreatedDate" -> Time, "ModifiedDate" -> Time),
      Map("Id" -> "PAY2", "PaymentType" -> 2, "Status" -> "SUCCESS", "ReturnId" -> "R1", "ShopCode" -> "SHOP1",
        "TotalPayment" -> 100, "Attempt" -> 1, "PaymentDate" -> FromDate, "CreatedDate" -> Time, "ModifiedDate" -> Time)
    ))
    writeBronze("transaction", Seq(
      Map("Id" -> "T1", "PaymentId" -> "PAY1", "PaymentMethodId" -> 1, "Amount" -> 150, "PaymentDate" -> FromDate, "ModifiedDate" -> Time),
      Map("Id" -> "T2", "PaymentId" -> "PAY1", "PaymentMethodId" -> 2, "Amount" -> 50, "PaymentDate" -> FromDate, "ModifiedDate" -> Time),
      Map("Id" -> "T3", "PaymentId" -> "PAY2", "PaymentMethodId" -> 1, "Amount" -> 100, "PaymentDate" -> FromDate, "ModifiedDate" -> Time)
    ))

    BronzeToSilver.process(spark, "all", SeededTables.mkString(","))
  }

  private val SeededTables = Seq(
    "geography", "employee", "shop", "shop_capacity", "customer", "product", "product_medicine", "product_supplement", "promotion",
    "voucher", "voucher_series", "payment_method", "order", "order_item", "return", "return_item", "payment", "transaction"
  )

  private def runDay(): Unit = {
    Seq(DimShop.process _, DimProduct.process _, DimCustomer.process _, DimEmployee.process _,
      DimPromotion.process _, DimVoucher.process _, DimPaymentMethod.process _).foreach(job => job(spark))
    FactSaleTransaction.process(spark, FromDate, ToDate)
    FactReturnTransaction.process(spark, FromDate, ToDate)
    FactPaymentTransaction.process(spark, FromDate, ToDate)
    FinancialReportDetail.process(spark, FromDate, ToDate)
    FinancialReport.process(spark, FromDate, ToDate)
    FinancialReportProduct.process(spark, FromDate, ToDate)
    FinancialReportOrder.process(spark, FromDate, ToDate)
    FundReportOrder.process(spark, FromDate, ToDate)
    FundReport.process(spark, FromDate, ToDate)
    FundReportShop.process(spark, FromDate, ToDate)
  }

  private lazy val firstRun: Unit = {
    loadBusinessDay()
    runDay()
  }

  private def measures(table: String, keyColumn: String, columns: String*): Map[String, Seq[Any]] =
    spark.table(s"nessie.serving.$table").select((keyColumn +: columns).map(col): _*).collect()
      .map((row: Row) => row.getString(0) -> row.toSeq.tail).toMap

  test("dimensions are rebuilt with names joined") {
    firstRun
    val shop = spark.table("nessie.dw.dim_shop").collect().head
    assert(shop.getAs[String]("ManagerName") == "An" && shop.getAs[String]("WardName") == "Phường 1")
    val productLines = spark.table("nessie.dw.dim_product").select("ProductId", "ProductLineName").collect()
      .map(row => row.getString(0) -> row.getString(1)).toMap
    assert(productLines == Map("P1" -> "Thuốc", "P2" -> "Thực phẩm chức năng"))
    assert(spark.table("nessie.dw.dim_voucher").select("PromotionName").head().getString(0) == "Gift")
  }

  test("facts hold one row per item, refunds take the order of their return") {
    firstRun
    assert(spark.table("nessie.dw.fact_sale_transaction").count() == 2)
    assert(spark.table("nessie.dw.fact_return_transaction").where(col("IsGift") === false).count() == 1)
    val refundOrder = spark.table("nessie.dw.fact_payment_transaction").where(col("TransactionId") === "T3").select("OrderId").head()
    assert(refundOrder.getString(0) == "O1")
  }

  test("financial reports: gift cost is promotion cost, returns reverse revenue and cost") {
    firstRun
    val columns = Seq("SalesQuantity", "ReturnQuantity", "GrossRevenue", "ReturnAmount", "NetRevenue",
      "TotalCogs", "TotalPromotionCost", "TotalCost", "GrossProfit", "ProfitMargin")
    val expected = Seq(2L, 1L, 200.0, 100.0, 100.0, 60.0, 30.0, 90.0, 10.0, 0.1)
    assert(measures("financial_report_detail", "ProductId", columns: _*) == Map("P1" -> expected))
    assert(measures("financial_report", "ShopCode", columns: _*) == Map("SHOP1" -> expected))
    assert(measures("financial_report_product", "ProductId", columns: _*) == Map("P1" -> expected))

    val documents = measures("financial_report_order", "DocumentType", "OrderStatus", "GrossRevenue", "ReturnAmount", "TotalCogs", "TotalPromotionCost")
    assert(documents == Map(
      "SALE" -> Seq("Completed", 200.0, 0.0, 120.0, 30.0),
      "RETURN" -> Seq("Completed", 0.0, 100.0, -60.0, 0.0)
    ))
  }

  test("fund reports reconcile cash with document value per method and shop") {
    firstRun
    val columns = Seq("TotalReceipts", "TotalExpenditures", "NetCashFlow", "TotalOrderAmount", "TotalReturnAmount", "ReconciliationDiff")
    assert(measures("fund_report", "PaymentMethodName", columns: _*) == Map(
      "Cash" -> Seq(150.0, 100.0, 50.0, 150.0, 100.0, 0.0),
      "Card" -> Seq(50.0, 0.0, 50.0, 50.0, 0.0, 0.0)
    ))
    assert(measures("fund_report_shop", "ShopCode", columns: _*) == Map("SHOP1" -> Seq(200.0, 100.0, 100.0, 200.0, 100.0, 0.0)))
  }

  test("a rerun of the same day gives the same tables, and a later change updates the fact") {
    firstRun
    runDay()
    assert(spark.table("nessie.dw.fact_sale_transaction").count() == 2)
    assert(spark.table("nessie.serving.financial_report_detail").count() == 1)
    assert(spark.table("nessie.serving.fund_report_order").count() == 3)

    // The order is returned the next morning: only its modified time is in the new window
    // Đơn chuyển sang Returned sáng hôm sau: chỉ thời điểm sửa nằm trong cửa sổ mới
    writeBronze("order", Seq(Map(
      "Id" -> "O1", "ShopCode" -> "SHOP1", "CustomerId" -> "C1", "EmployeeSaleId" -> "E1", "TransactionDate" -> Time,
      "OrderDate" -> FromDate, "Status" -> "Returned", "ModifiedDate" -> "2026-10-15 08:00:00"
    )))
    BronzeToSilver.process(spark, "sales", "order")
    FactSaleTransaction.process(spark, "2026-10-15", "2026-10-16")
    val statuses = spark.table("nessie.dw.fact_sale_transaction").select("OrderStatus").distinct().collect().map(_.getString(0))
    assert(statuses.toSeq == Seq("Returned"))
    assert(spark.table("nessie.dw.fact_sale_transaction").count() == 2)
  }
}
