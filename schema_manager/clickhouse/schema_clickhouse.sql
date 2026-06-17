-- ClickHouse Native Report Schema DDL
-- Database: report

CREATE DATABASE IF NOT EXISTS report;

-- 1. Financial Report Table
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

-- 2. Fund Report Table
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
