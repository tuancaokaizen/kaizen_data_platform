-- ClickHouse Native Report Schema DDL
-- Database: report
--
-- Views (hot/cold, tên cột tiếng Việt): schema_clickhouse_views.sql

CREATE DATABASE IF NOT EXISTS report;

-- ============================================================================
-- Financial reports — 3 grains / Báo cáo tài chính 3 grain
-- ============================================================================

-- Shop aggregate (theo cửa hàng)
CREATE TABLE IF NOT EXISTS report.financial_report (
    ReportDate Date,
    ShopCode String,
    ShopName String,
    SalesQuantity Int64,
    ReturnQuantity Int64,
    GrossRevenue Float64,
    ReturnAmount Float64,
    NetRevenue Float64,
    TotalCogs Float64,
    TotalPromotionCost Float64,
    TotalCost Float64,
    GrossProfit Float64,
    ProfitMargin Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (ReportDate, ShopCode);

-- Detail: shop + product + line (chi tiết)
CREATE TABLE IF NOT EXISTS report.financial_report_detail (
    ReportDate Date,
    ShopCode String,
    ShopName String,
    ProductId String,
    ProductName String,
    ProductLineCode String,
    ProductLineName String,
    SalesQuantity Int64,
    ReturnQuantity Int64,
    GrossRevenue Float64,
    ReturnAmount Float64,
    NetRevenue Float64,
    TotalCogs Float64,
    TotalPromotionCost Float64,
    TotalCost Float64,
    GrossProfit Float64,
    ProfitMargin Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (ReportDate, ShopCode, ProductId);

-- Product aggregate across shops (theo sản phẩm)
CREATE TABLE IF NOT EXISTS report.financial_report_product (
    ReportDate Date,
    ProductId String,
    ProductName String,
    ProductLineCode String,
    ProductLineName String,
    SalesQuantity Int64,
    ReturnQuantity Int64,
    GrossRevenue Float64,
    ReturnAmount Float64,
    NetRevenue Float64,
    TotalCogs Float64,
    TotalPromotionCost Float64,
    TotalCost Float64,
    GrossProfit Float64,
    ProfitMargin Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (ReportDate, ProductId);

-- Order grain: one row per sale order or return document / Theo từng đơn hoặc phiếu trả
CREATE TABLE IF NOT EXISTS report.financial_report_order (
    ReportDate Date,
    ShopCode String,
    ShopName String,
    DocumentType String,
    OrderId String,
    ReturnId String,
    EmployeeSaleId String,
    OrderStatus String,
    SalesQuantity Int64,
    ReturnQuantity Int64,
    GrossRevenue Float64,
    ReturnAmount Float64,
    NetRevenue Float64,
    TotalCogs Float64,
    TotalPromotionCost Float64,
    TotalCost Float64,
    GrossProfit Float64,
    ProfitMargin Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (DocumentType, OrderId, ReturnId);

-- ============================================================================
-- Fund reports / Báo cáo quỹ
-- ============================================================================

-- Shop + payment method (đối soát PTTT — giữ tương thích)
CREATE TABLE IF NOT EXISTS report.fund_report (
    ReportDate Date,
    ShopCode String,
    ShopName String,
    PaymentMethodName String,
    TotalReceipts Float64,
    TotalExpenditures Float64,
    NetCashFlow Float64,
    TotalOrderAmount Float64,
    TotalReturnAmount Float64,
    ReconciliationDiff Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (ReportDate, ShopCode, PaymentMethodName);

-- Shop aggregate (theo cửa hàng)
CREATE TABLE IF NOT EXISTS report.fund_report_shop (
    ReportDate Date,
    ShopCode String,
    ShopName String,
    TotalReceipts Float64,
    TotalExpenditures Float64,
    NetCashFlow Float64,
    TotalOrderAmount Float64,
    TotalReturnAmount Float64,
    ReconciliationDiff Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (ReportDate, ShopCode);

-- Order grain: OrderId + PaymentMethod (+ ReturnId for refunds) / Theo từng đơn × PTTT
CREATE TABLE IF NOT EXISTS report.fund_report_order (
    ReportDate Date,
    OrderId String,
    ReturnId String,
    ShopCode String,
    ShopName String,
    PaymentMethodName String,
    TotalReceipts Float64,
    TotalExpenditures Float64,
    NetCashFlow Float64,
    TotalOrderAmount Float64,
    TotalReturnAmount Float64,
    ReconciliationDiff Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (OrderId, ReturnId, PaymentMethodName);

-- ============================================================================
-- Realtime business-by-order (Flink BusinessRealtimeJob v2) / Tình hình KD theo đơn realtime v2
-- Hot only — cold lives in serving.realtime_business_by_order (Iceberg) via MigrateRealtimeBusinessToColdJob
-- Grain (OrderId, ReturnId); ReturnId '' = SALE row. Money in VND (Int64).
-- Version = newest meta.version among the order and its payments; a grain's ReportDate never changes,
-- so dedup inside one partition is enough
-- Grain (OrderId, ReturnId); ReturnId '' = dòng bán. Tiền VND (Int64).
-- Version = meta.version mới nhất của đơn và các phiếu; ReportDate của một grain không đổi nên dedup trong partition là đủ
-- ============================================================================
CREATE TABLE IF NOT EXISTS report.realtime_business_by_order (
    ReportDate Date,
    OrderId String,
    ReturnId String,
    DocumentType LowCardinality(String),
    ShopCode LowCardinality(String),
    ShopName String,
    OrderStatus LowCardinality(String),
    SalesAmount Int64,
    ReturnAmount Int64,
    NetRevenue Int64,
    TotalReceipts Int64,
    TotalExpenditures Int64,
    NetCashFlow Int64,
    PaymentMethodSummary String,
    TotalOrderAmount Int64,
    TotalReturnAmount Int64,
    ReconciliationDiff Int64,
    PendingOrder UInt8,
    Version UInt64,
    UpdatedAt DateTime64(3)
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMMDD(ReportDate)
ORDER BY (OrderId, ReturnId)
-- View + migrate use a 14d hot window, TTL 16d = 14 + 2d buffer, equal to the Flink state TTL ([business_realtime] state_days)
-- View + migrate dùng cửa sổ hot 14 ngày, TTL 16 ngày = 14 + 2 ngày buffer, bằng TTL state của Flink ([business_realtime] state_days)
TTL ReportDate + INTERVAL 16 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

-- Cold stub, same columns (replaced by Iceberg engine after MigrateRealtimeBusinessToColdJob)
-- Stub cold cùng cột (đổi sang Iceberg engine sau job migrate)
CREATE TABLE IF NOT EXISTS serving.realtime_business_by_order (
    ReportDate Date,
    OrderId String,
    ReturnId String,
    DocumentType LowCardinality(String),
    ShopCode LowCardinality(String),
    ShopName String,
    OrderStatus LowCardinality(String),
    SalesAmount Int64,
    ReturnAmount Int64,
    NetRevenue Int64,
    TotalReceipts Int64,
    TotalExpenditures Int64,
    NetCashFlow Int64,
    PaymentMethodSummary String,
    TotalOrderAmount Int64,
    TotalReturnAmount Int64,
    ReconciliationDiff Int64,
    PendingOrder UInt8,
    Version UInt64,
    UpdatedAt DateTime64(3)
) ENGINE = ReplacingMergeTree(Version)
ORDER BY (OrderId, ReturnId);

-- ============================================================================
-- Realtime stock per position (Flink SupplyChainRealtimeJob ← inventory-movements)
-- Tồn kho realtime theo vị trí (Flink SupplyChainRealtimeJob ← inventory-movements)
-- Grain (LocationType, LocationCode, ProductId), current state only; daily history is serving.inventory_health.
-- The flow counters belong to BusinessDate (ICT) and restart at the first movement of a new day.
-- Version = PG movement Seq, so a replayed movement never moves a row backwards.
-- Grain (LocationType, LocationCode, ProductId), chỉ trạng thái hiện tại; lịch sử ngày ở serving.inventory_health.
-- Các bộ đếm thuộc về BusinessDate (giờ VN) và về 0 ở biến động đầu tiên của ngày mới.
-- Version = Seq biến động trong PG nên biến động phát lại không kéo dòng lùi.
-- ============================================================================
CREATE TABLE IF NOT EXISTS report.realtime_inventory_position (
    LocationType LowCardinality(String),
    LocationCode LowCardinality(String),
    ProductId LowCardinality(String),
    OnHandQty Int64,
    BusinessDate Date,
    SoldQty Int64,
    ReturnedQty Int64,
    ReceivedQty Int64,
    TransferInQty Int64,
    TransferOutQty Int64,
    ExpiredQty Int64,
    DamagedQty Int64,
    AdjustedQty Int64,
    OversellQty Int64,
    MovementCount UInt32,
    IsStockout UInt8,
    StockoutSince Nullable(DateTime64(3)),
    LastMovementType LowCardinality(String),
    LastMovementAt DateTime64(3),
    Version UInt64,
    UpdatedAt DateTime64(3)
) ENGINE = ReplacingMergeTree(Version)
ORDER BY (LocationType, LocationCode, ProductId);

-- ============================================================================
-- Realtime trip status (Flink SupplyChainRealtimeJob ← logistics-events)
-- Trạng thái chuyến realtime (Flink SupplyChainRealtimeJob ← logistics-events)
-- Grain ShipmentId; ShipmentDate = ICT date of the planned departure and never changes, so dedup inside one
-- partition is enough. Version = event seq within the shipment. History is serving.carrier_scorecard.
-- Grain ShipmentId; ShipmentDate = ngày VN của giờ xuất phát dự kiến, không đổi nên dedup trong partition là đủ.
-- Version = số thứ tự sự kiện trong chuyến. Lịch sử ở serving.carrier_scorecard.
-- ============================================================================
CREATE TABLE IF NOT EXISTS report.realtime_shipment_status (
    ShipmentDate Date,
    ShipmentId String,
    OriginCode LowCardinality(String),
    CarrierCode LowCardinality(String),
    VehicleId String,
    Status LowCardinality(String),
    PlannedDeparture Nullable(DateTime64(3)),
    ActualDeparture Nullable(DateTime64(3)),
    StopCount UInt16,
    StopsDelivered UInt16,
    StopsPartial UInt16,
    StopsFailed UInt16,
    StopsDelayed UInt16,
    LastStopSeq UInt16,
    LastStopShop LowCardinality(String),
    MaxDelayMinutes Int32,
    LoadKg Float64,
    LoadM3 Float64,
    CostVnd Int64,
    LastEventType LowCardinality(String),
    LastReasonCode LowCardinality(String),
    LastEventAt DateTime64(3),
    Version UInt64,
    UpdatedAt DateTime64(3)
) ENGINE = ReplacingMergeTree(Version)
PARTITION BY toYYYYMM(ShipmentDate)
ORDER BY ShipmentId
-- Trips older than 30 days are only needed in the lakehouse / Chuyến quá 30 ngày chỉ cần trong lakehouse
TTL ShipmentDate + INTERVAL 30 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;
