package com.kaizen.batch.processing

import com.kaizen.batch.processing.dim._
import com.kaizen.batch.processing.fact._
import com.kaizen.batch.processing.report.supply._
import com.kaizen.batch.processing.silver.BronzeToSilver
import com.kaizen.batch.testing.{LocalSpark, SourceRows}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

/** Runs the order, promotion, inventory, supply, customer and salary facts on their own LOCAL catalog for one day.
  * Chạy các fact đơn hàng, khuyến mãi, tồn kho, cung ứng, khách hàng và lương trên catalog LOCAL riêng cho một ngày.
  *
  * Day 2026-10-14 at SHOP1, served by DC1 / Ngày 2026-10-14 tại SHOP1, do DC1 phục vụ:
  *   O1 10:00 with voucher S1 (20 off): 3 × P1 at 100 (cost 60) + 1 gift P2 (cost 30); gift offers PR1 and PR2 both active
  *   O2 15:00: 1 × P1 at 100, returned the same day (refund 100)
  *   E1 earns base 1000 + 5 % commission; P1 sold 3 today and 1 yesterday, 12 on hand at closing
  *   trip SH1 (cost 300): stop ST1 at SHOP1, 45 min late, 8 of 10 received; stop ST2 10 min late, 20 of 20 received
  */
@RunWith(classOf[JUnitRunner])
class DimensionalModelTest extends AnyFunSuite {
  private lazy val spark = LocalSpark.isolatedSession()
  private val FromDate = "2026-10-14"
  private val ToDate = "2026-10-15"
  private val Time = "2026-10-14 10:00:00"

  private def writeBronze(table: String, rows: Seq[Map[String, Any]]): Unit =
    SourceRows(spark, table, rows).writeTo(s"nessie.stg.$table").createOrReplace()

  private def loadBusinessDay(): Unit = {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.stg")
    writeBronze("geography", Seq(Map("Id" -> "W1", "Name" -> "Phường 1", "ProvinceCode" -> "79", "SubRegion" -> "HCM", "ModifiedDate" -> Time)))
    writeBronze("employee", Seq(Map("Id" -> "E1", "Name" -> "An", "Status" -> true, "ShopCode" -> "SHOP1", "ModifiedDate" -> Time)))
    writeBronze("shop", Seq(Map("Code" -> "SHOP1", "Name" -> "Shop One", "WardId" -> "W1", "ManagerId" -> "E1",
      "ServingWarehouseId" -> "DC1", "ModifiedDate" -> Time)))
    writeBronze("shop_capacity", Seq(
      Map("Id" -> "SC0", "ShopCode" -> "SHOP1", "StorageM3" -> 8.0, "EffectiveFrom" -> "2025-01-01", "EffectiveTo" -> "2025-12-31", "ModifiedDate" -> Time),
      Map("Id" -> "SC1", "ShopCode" -> "SHOP1", "StorageM3" -> 12.5, "ShelfSlots" -> 100, "EffectiveFrom" -> "2026-01-01", "ModifiedDate" -> Time)
    ))
    writeBronze("customer", Seq(Map("Id" -> "C1", "WardId" -> "W1", "YearOfBirth" -> 1990, "ModifiedDate" -> Time)))
    writeBronze("product", Seq(
      Map("Id" -> "P1", "Name" -> "Paracetamol", "Type" -> "medicine", "StockPrice" -> 60, "RetailPrice" -> 100,
        "UnitVolumeCm3" -> 1000, "SupplierId" -> "SUP1", "Status" -> true, "ModifiedDate" -> Time),
      Map("Id" -> "P2", "Name" -> "Vitamin C", "Type" -> "supplement", "StockPrice" -> 30, "RetailPrice" -> 30,
        "SupplierId" -> "SUP1", "Status" -> true, "ModifiedDate" -> Time)
    ))
    writeBronze("product_medicine", Seq(Map("ProductId" -> "P1", "IsPrescription" -> false)))
    writeBronze("product_supplement", Seq(Map("ProductId" -> "P2", "Brand" -> "Vita")))
    writeBronze("warehouse", Seq(Map("Id" -> "DC1", "Name" -> "DC One", "WardId" -> "W1", "StorageM3" -> 1000.0, "ModifiedDate" -> Time)))
    writeBronze("supplier", Seq(Map("Id" -> "SUP1", "Name" -> "Pharma", "LeadTimeMeanDays" -> 3.0, "ModifiedDate" -> Time)))
    writeBronze("carrier", Seq(Map("Id" -> "CA1", "Name" -> "Fast", "Type" -> "3PL", "ModifiedDate" -> Time)))
    writeBronze("vehicle", Seq(Map("Id" -> "VH1", "CarrierId" -> "CA1", "CapacityKg" -> 1000, "CapacityM3" -> 10.0, "ModifiedDate" -> Time)))

    writeBronze("promotion", Seq(
      Map("Code" -> "PR1", "Name" -> "October", "StartDate" -> "2026-10-01 00:00:00", "EndDate" -> "2026-10-31 23:59:59", "ModifiedDate" -> Time),
      Map("Code" -> "PR2", "Name" -> "Mid October", "StartDate" -> "2026-10-10 00:00:00", "EndDate" -> "2026-10-20 00:00:00", "ModifiedDate" -> Time),
      Map("Code" -> "PR3", "Name" -> "September", "StartDate" -> "2026-09-01 00:00:00", "EndDate" -> "2026-09-30 23:59:59", "ModifiedDate" -> Time)
    ))
    writeBronze("gift", Seq(
      Map("Id" -> "G1", "ProductId" -> "P2", "PromotionCode" -> "PR1", "ModifiedDate" -> Time),
      Map("Id" -> "G2", "ProductId" -> "P2", "PromotionCode" -> "PR2", "ModifiedDate" -> Time),
      Map("Id" -> "G3", "ProductId" -> "P2", "PromotionCode" -> "PR3", "ModifiedDate" -> Time)
    ))
    writeBronze("voucher", Seq(Map("Code" -> "V1", "Name" -> "Voucher", "PromotionCode" -> "PR1", "VoucherAmount" -> 20, "ModifiedDate" -> Time)))
    writeBronze("voucher_series", Seq(
      Map("SeriesCode" -> "S1", "VoucherCode" -> "V1", "ExpiryDay" -> 30, "Status" -> "USED",
        "CreatedDate" -> "2026-10-10 08:00:00", "ModifiedDate" -> Time),
      Map("SeriesCode" -> "S2", "VoucherCode" -> "V1", "Status" -> "ACTIVE",
        "CreatedDate" -> "2026-10-14 09:00:00", "ModifiedDate" -> "2026-10-14 09:00:00")
    ))

    writeBronze("order", Seq(
      Map("Id" -> "O1", "ShopCode" -> "SHOP1", "CustomerId" -> "C1", "EmployeeSaleId" -> "E1", "TransactionDate" -> Time,
        "OrderDate" -> FromDate, "TotalBill" -> 300, "TotalDiscount" -> 20, "TotalPayment" -> 280, "VoucherSeriesCode" -> "S1",
        "Status" -> "Completed", "ModifiedDate" -> Time),
      Map("Id" -> "O2", "ShopCode" -> "SHOP1", "CustomerId" -> "C1", "EmployeeSaleId" -> "E1", "TransactionDate" -> "2026-10-14 15:00:00",
        "OrderDate" -> FromDate, "TotalBill" -> 100, "TotalDiscount" -> 0, "TotalPayment" -> 100, "Status" -> "Returned",
        "ModifiedDate" -> "2026-10-14 16:00:00")
    ))
    writeBronze("order_item", Seq(
      Map("Id" -> "I1", "OrderId" -> "O1", "LineNo" -> 1, "ProductId" -> "P1", "Quantity" -> 3, "UnitPrice" -> 100,
        "DiscountAmount" -> 20, "LineAmount" -> 280, "IsGift" -> false, "UnitCost" -> 60, "OrderDate" -> FromDate, "ModifiedDate" -> Time),
      Map("Id" -> "I2", "OrderId" -> "O1", "LineNo" -> 2, "ProductId" -> "P2", "Quantity" -> 1, "UnitPrice" -> 0,
        "DiscountAmount" -> 0, "LineAmount" -> 0, "IsGift" -> true, "UnitCost" -> 30, "OrderDate" -> FromDate, "ModifiedDate" -> Time),
      Map("Id" -> "I3", "OrderId" -> "O2", "LineNo" -> 1, "ProductId" -> "P1", "Quantity" -> 1, "UnitPrice" -> 100,
        "DiscountAmount" -> 0, "LineAmount" -> 100, "IsGift" -> false, "UnitCost" -> 60, "OrderDate" -> FromDate,
        "ModifiedDate" -> "2026-10-14 15:00:00")
    ))
    writeBronze("return", Seq(Map(
      "Id" -> "R1", "OrderId" -> "O2", "ShopCode" -> "SHOP1", "ReturnDate" -> FromDate, "Reason" -> "Wrong item",
      "ModifiedDate" -> "2026-10-14 16:00:00"
    )))
    writeBronze("return_item", Seq(Map(
      "Id" -> "RI1", "ReturnId" -> "R1", "OrderId" -> "O2", "OrderItemId" -> "I3", "ProductId" -> "P1", "Quantity" -> 1,
      "RestockQty" -> 1, "DamageQty" -> 0, "UnitPrice" -> 100, "UnitCost" -> 60, "LineAmount" -> 100, "ReturnDate" -> FromDate,
      "ModifiedDate" -> "2026-10-14 16:00:00"
    )))
    writeBronze("employee_salary", Seq(Map(
      "Id" -> "ES1", "EmployeeId" -> "E1", "ShopCode" -> "SHOP1", "SalaryMonth" -> "2026-10-01 00:00:00", "BaseSalary" -> 1000,
      "CommissionRate" -> 5, "ModifiedDate" -> "2026-10-01 00:00:00"
    )))
    writeBronze("lost_sale", Seq(Map(
      "Id" -> "L1", "BusinessDate" -> FromDate, "ShopCode" -> "SHOP1", "ProductId" -> "P1", "RequestedQty" -> 5,
      "SubstitutedQty" -> 2, "Reason" -> "OUT_OF_STOCK", "ModifiedDate" -> Time
    )))

    writeBronze("stock_movement", Seq(
      movement("M1", "SALE", -3, "2026-10-14"),
      movement("M2", "SALE", -1, "2026-10-13"),
      movement("M3", "TRANSFER_IN", 10, "2026-10-14"),
      movement("M4", "SALE", -50, "2026-09-01")
    ))
    writeBronze("inventory_snapshot", Seq(Map(
      "SnapshotDate" -> FromDate, "LocationType" -> "SHOP", "LocationId" -> "SHOP1", "ProductId" -> "P1", "OnHandQty" -> 12,
      "InTransitQty" -> 0, "OnOrderQty" -> 5, "ExpiredQty" -> 0, "StockoutFlag" -> false, "OverCapacityFlag" -> false,
      "ModifiedDate" -> "2026-10-14 23:30:00"
    )))

    writeBronze("purchase_order", Seq(Map(
      "Id" -> "PO1", "SupplierId" -> "SUP1", "WarehouseId" -> "DC1", "Status" -> "RECEIVED", "OrderDate" -> "2026-10-10 08:00:00",
      "ExpectedDate" -> "2026-10-13", "ReceivedDate" -> "2026-10-14 09:00:00", "ModifiedDate" -> "2026-10-14 09:00:00"
    )))
    writeBronze("purchase_order_item", Seq(Map(
      "Id" -> "POI1", "PurchaseOrderId" -> "PO1", "ProductId" -> "P1", "OrderedQty" -> 100, "ReceivedQty" -> 90, "UnitCost" -> 60,
      "ModifiedDate" -> "2026-10-14 09:00:00"
    )))
    writeBronze("transfer_order", Seq(Map(
      "Id" -> "TO1", "FromWarehouseId" -> "DC1", "ToShopCode" -> "SHOP1", "Status" -> "PARTIAL", "RequestedDate" -> "2026-10-13",
      "PlannedDeliveryDate" -> FromDate, "ModifiedDate" -> "2026-10-14 08:45:00"
    )))
    writeBronze("transfer_order_item", Seq(Map(
      "Id" -> "TOI1", "TransferOrderId" -> "TO1", "ProductId" -> "P1", "RequestedQty" -> 10, "ShippedQty" -> 10, "ReceivedQty" -> 8,
      "ModifiedDate" -> "2026-10-14 08:45:00"
    )))
    writeBronze("shipment", Seq(Map(
      "Id" -> "SH1", "WarehouseId" -> "DC1", "CarrierId" -> "CA1", "VehicleId" -> "VH1", "Status" -> "PARTIAL",
      "PlannedDepartureTime" -> "2026-10-14 06:00:00", "ActualDepartureTime" -> "2026-10-14 06:20:00", "DistanceKm" -> 20.0,
      "TotalWeightKg" -> 500.0, "TotalVolumeM3" -> 5.0, "StopCount" -> 2, "TransportCost" -> 300, "ShipmentDate" -> FromDate,
      "ModifiedDate" -> "2026-10-14 09:10:00"
    )))
    writeBronze("shipment_stop", Seq(
      Map("Id" -> "ST1", "ShipmentId" -> "SH1", "StopSeq" -> 1, "ShopCode" -> "SHOP1", "TransferOrderId" -> "TO1",
        "PlannedArrivalTime" -> "2026-10-14 08:00:00", "ActualArrivalTime" -> "2026-10-14 08:45:00", "Status" -> "PARTIAL",
        "ModifiedDate" -> "2026-10-14 08:45:00"),
      Map("Id" -> "ST2", "ShipmentId" -> "SH1", "StopSeq" -> 2, "ShopCode" -> "SHOP2",
        "PlannedArrivalTime" -> "2026-10-14 09:00:00", "ActualArrivalTime" -> "2026-10-14 09:10:00", "Status" -> "DELIVERED",
        "ModifiedDate" -> "2026-10-14 09:10:00")
    ))
    writeBronze("shipment_item", Seq(
      Map("Id" -> "SI1", "ShipmentStopId" -> "ST1", "TransferOrderItemId" -> "TOI1", "ProductId" -> "P1", "Qty" -> 10,
        "ReceivedQty" -> 8, "ModifiedDate" -> "2026-10-14 08:45:00"),
      Map("Id" -> "SI2", "ShipmentStopId" -> "ST2", "ProductId" -> "P2", "Qty" -> 20, "ReceivedQty" -> 20,
        "ModifiedDate" -> "2026-10-14 09:10:00")
    ))
    writeBronze("shipment_event", Seq(
      event("EV1", 1, null, "DEPARTED", "2026-10-14 06:20:00", null),
      event("EV2", 2, "ST1", "DELAYED", "2026-10-14 08:30:00", "TRAFFIC"),
      event("EV3", 3, "ST1", "PARTIAL", "2026-10-14 08:45:00", "SHORT_PICK"),
      event("EV4", 4, "ST2", "DELIVERED", "2026-10-14 09:10:00", null)
    ))

    BronzeToSilver.process(spark, "all", SeededTables.mkString(","))
  }

  private def movement(id: String, movementType: String, quantity: Int, date: String): Map[String, Any] = Map(
    "Id" -> id, "MovementType" -> movementType, "LocationType" -> "SHOP", "LocationId" -> "SHOP1", "ProductId" -> "P1",
    "Qty" -> quantity, "UnitCost" -> 60, "MovementTime" -> s"$date 10:00:00", "MovementDate" -> date, "ModifiedDate" -> s"$date 10:00:00"
  )

  private def event(id: String, sequence: Int, stopId: String, eventType: String, time: String, reason: String): Map[String, Any] = Map(
    "Id" -> id, "ShipmentId" -> "SH1", "EventSeq" -> sequence, "ShipmentStopId" -> stopId, "EventType" -> eventType,
    "EventTime" -> time, "ReasonCode" -> reason, "ModifiedDate" -> time
  )

  private val SeededTables = Seq(
    "geography", "employee", "shop", "shop_capacity", "customer", "product", "product_medicine", "product_supplement",
    "warehouse", "supplier", "carrier", "vehicle",
    "promotion", "gift", "voucher", "voucher_series", "order", "order_item", "return", "return_item", "employee_salary",
    "lost_sale", "stock_movement", "inventory_snapshot", "purchase_order", "purchase_order_item", "transfer_order",
    "transfer_order_item", "shipment", "shipment_stop", "shipment_item", "shipment_event"
  )

  private def runDay(): Unit = {
    Seq(DimDate.process _, DimGeography.process _, DimShop.process _, DimLocation.process _, DimWarehouse.process _,
      DimSupplier.process _, DimCarrier.process _, DimVehicle.process _, DimProduct.process _).foreach(job => job(spark))
    Seq(FactSaleTransaction.process _, FactReturnTransaction.process _, FactOrderTransaction.process _,
      FactPromotionTransaction.process _, FactVoucherSeries.process _, FactLostSale.process _, FactStockMovement.process _,
      FactInventorySnapshot.process _, FactPurchaseOrder.process _, FactTransferOrder.process _, FactShipment.process _,
      FactShipmentStop.process _, FactCustomerMonthly.process _, FactEmployeeSalary.process _
    ).foreach(job => job(spark, FromDate, ToDate))
    Seq(InventoryHealth.process _, ShopCapacityUtilization.process _, CarrierScorecard.process _, SupplierPerformance.process _)
      .foreach(job => job(spark, FromDate, ToDate))
  }

  private lazy val firstRun: Unit = {
    loadBusinessDay()
    runDay()
  }

  private def rows(table: String, keyColumn: String, columns: String*): Map[String, Seq[Any]] =
    spark.table(s"nessie.dw.$table").select((keyColumn +: columns).map(col): _*).collect()
      .map((row: Row) => row.get(0).toString -> row.toSeq.tail).toMap

  private def servingRows(table: String, keyColumn: String, columns: String*): Map[String, Seq[Any]] =
    spark.table(s"nessie.serving.$table").select((keyColumn +: columns).map(col): _*).collect()
      .map((row: Row) => row.get(0).toString -> row.toSeq.tail).toMap

  test("calendar, location and supply dimensions") {
    firstRun
    assert(spark.table("nessie.dw.dim_date").count() == 2191)
    assert(rows("dim_date", "Date", "DateKey", "DayOfWeek", "DayName", "IsWeekend").apply("2026-10-14") == Seq(20261014, 3, "Thứ Tư", false))
    assert(rows("dim_shop", "ShopCode", "StorageM3", "ShelfSlots").apply("SHOP1") == Seq(new java.math.BigDecimal("12.50"), 100))
    assert(rows("dim_location", "LocationId", "LocationType", "ProvinceCode") == Map("SHOP1" -> Seq("SHOP", "79"), "DC1" -> Seq("DC", "79")))
    assert(rows("dim_warehouse", "WarehouseId", "ServedShopCount") == Map("DC1" -> Seq(1)))
    assert(rows("dim_supplier", "SupplierId", "ProductCount") == Map("SUP1" -> Seq(2)))
    assert(rows("dim_vehicle", "VehicleId", "CarrierName") == Map("VH1" -> Seq("Fast")))
    assert(rows("dim_carrier", "CarrierId", "VehicleCount") == Map("CA1" -> Seq(1)))
  }

  test("order fact sums its items apart from gifts and carries its returns") {
    firstRun
    val columns = Seq("OrderHour", "Quantity", "GiftQuantity", "GrossAmount", "DiscountAmount", "NetAmount", "CostAmount",
      "GiftCostAmount", "HasVoucher", "HasGift", "ReturnAmount", "NetAfterReturnAmount")
    assert(rows("fact_order_transaction", "OrderId", columns: _*) == Map(
      "O1" -> Seq(10, 3, 1, 300L, 20L, 280L, 180L, 30L, true, true, 0L, 280L),
      "O2" -> Seq(15, 1, 0, 100L, 0L, 100L, 60L, 0L, false, false, 100L, 0L)
    ))
  }

  test("promotion fact: voucher lines are exact, gift lines infer the latest started active offer") {
    firstRun
    val columns = Seq("PromotionType", "PromotionCode", "VoucherCode", "IsPromotionInferred", "PromotionCandidateCount",
      "DiscountAmount", "GiftCostAmount", "PromotionCostAmount")
    assert(rows("fact_promotion_transaction", "OrderItemId", columns: _*) == Map(
      "I1" -> Seq("VOUCHER", "PR1", "V1", false, 1, 20L, 0L, 20L),
      "I2" -> Seq("GIFT", "PR2", null, true, 2, 0L, 30L, 30L)
    ))
  }

  test("voucher series fact follows each code from issue to use") {
    firstRun
    val columns = Seq("IsUsed", "OrderId", "DaysToUse", "ExpiryDate", "DiscountAmount")
    assert(rows("fact_voucher_series", "VoucherSeriesCode", columns: _*) == Map(
      "S1" -> Seq(true, "O1", 4, java.sql.Date.valueOf("2026-11-09"), 20L),
      "S2" -> Seq(false, null, null, null, null)
    ))
  }

  test("inventory facts: ledger in and out, stock value and days of supply") {
    firstRun
    assert(rows("fact_stock_movement", "MovementId", "InQuantity", "OutQuantity", "ValueAmount") == Map(
      "M1" -> Seq(0, 3, -180L),
      "M3" -> Seq(10, 0, 600L)
    ))
    val snapshot = rows("fact_inventory_snapshot", "ProductId", "InventoryPosition", "OnHandValue", "Demand28DayQuantity", "DaysOfSupply")
    assert(snapshot == Map("P1" -> Seq(17, 720L, 4L, 84.0)))
    assert(rows("fact_lost_sale", "LostSaleId", "LostQuantity", "LostRevenueEstimate") == Map("L1" -> Seq(3, 300L)))
  }

  test("supply facts: purchase lead time, transfer fill rate, trip and stop delivery") {
    firstRun
    assert(rows("fact_purchase_order", "PurchaseOrderItemId", "LeadTimeDays", "IsOnTime", "FillRate", "ReceivedAmount") ==
      Map("POI1" -> Seq(4, false, 0.9, 5400L)))
    assert(rows("fact_transfer_order", "TransferOrderItemId", "IsOnTime", "LeadTimeDays", "ShortQuantity", "FillRate") ==
      Map("TOI1" -> Seq(true, 1, 2, 0.8)))
    assert(rows("fact_shipment", "ShipmentId", "DepartureDelayMinutes", "DeliveredStopCount", "OnTimeStopCount",
      "PlannedQuantity", "ReceivedQuantity", "WeightLoadFactor", "CostPerKg") == Map("SH1" -> Seq(20, 2, 1, 30L, 28L, 0.5, 0.6)))
    assert(rows("fact_shipment_stop", "ShipmentStopId", "DelayMinutes", "IsOnTime", "IsInFull", "IsOnTimeInFull",
      "ReasonCode", "AllocatedTransportCost") == Map(
      "ST1" -> Seq(45, false, false, false, "TRAFFIC", 100L),
      "ST2" -> Seq(10, true, true, true, null, 200L)
    ))
  }

  test("customer month and salary are derived from the order and sale facts") {
    firstRun
    assert(rows("fact_customer_monthly", "CustomerId", "OrderCount", "ReturnedOrderCount", "VoucherOrderCount", "NetAmount",
      "NetAfterReturnAmount", "AverageOrderValue", "IsNewCustomer") == Map("C1" -> Seq(2, 1, 1, 380L, 280L, 190.0, true)))
    assert(rows("fact_employee_salary", "EmployeeSalaryId", "SalesOrderCount", "SalesAmount", "ReturnAmount", "NetSalesAmount",
      "CommissionAmount", "TotalSalary") == Map("ES1" -> Seq(2, 380L, 100L, 280L, 14L, 1014L)))
  }

  test("a rerun of the same day keeps one row per key") {
    firstRun
    runDay()
    Seq("fact_order_transaction" -> 2L, "fact_promotion_transaction" -> 2L, "fact_voucher_series" -> 2L,
      "fact_shipment_stop" -> 2L, "fact_customer_monthly" -> 1L, "fact_employee_salary" -> 1L).foreach { case (table, expected) =>
      assert(spark.table(s"nessie.dw.$table").count() == expected, table)
    }
  }

  test("supply serving reports: inventory health, capacity, carrier week, supplier month") {
    firstRun
    assert(servingRows("inventory_health", "ProductLineCode", "OnHandQuantity", "OnHandValue", "LostQuantity", "FillRate", "StockoutRate") ==
      Map("medicine" -> Seq(12L, 720L, 3L, 0.4, 0.0)))
    val capacity = servingRows("shop_capacity_utilization", "ShopCode", "OnHandVolumeM3", "StorageUtilization", "ReceivingCount")
    assert(capacity("SHOP1").head == new java.math.BigDecimal("0.0120"))
    assert(math.abs(capacity("SHOP1")(1).asInstanceOf[Double] - 0.012 / 12.5) < 1e-9)
    assert(capacity("SHOP1")(2) == 0L)
    assert(servingRows("carrier_scorecard", "CarrierId", "StopCount", "OnTimeInFullStopCount", "OnTimeInFullRate", "CostPerKg", "CostPerStop") ==
      Map("CA1" -> Seq(2L, 1L, 0.5, 0.6, 150.0)))
    assert(servingRows("supplier_performance", "SupplierId", "ReceivedLineCount", "OnTimeRate", "AverageLeadTimeDays", "AverageFillRate", "ReceivedAmount") ==
      Map("SUP1" -> Seq(1L, 0.0, 4.0, 0.9, 5400L)))
  }
}
