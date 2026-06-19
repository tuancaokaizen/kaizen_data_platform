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
