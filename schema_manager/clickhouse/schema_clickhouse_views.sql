-- ClickHouse Report Views — Hot/Cold với tên cột tiếng Việt
-- Hot layer  : report.* (ReplacingMergeTree, dữ liệu 30 ngày gần nhất)
-- Cold layer : serving.* (Iceberg/Nessie, lịch sử đầy đủ trên MinIO)
--
-- Quy ước đặt tên:
--   report.v_bao_cao_*     → hot (native)
--   serving.v_bao_cao_*    → cold (Nessie/Iceberg)
--   report.v_bao_cao_*_tong_hop → hợp nhất hot + cold

CREATE DATABASE IF NOT EXISTS report;
CREATE DATABASE IF NOT EXISTS serving;

-- Dọn view cũ (tên legacy) / Clean up legacy view names
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_nong;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_lanh;
DROP VIEW IF EXISTS report.v_bao_cao_quy_nong;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_lanh;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh;
DROP VIEW IF EXISTS report.v_bao_cao_quy;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh;
DROP VIEW IF EXISTS serving.v_bao_cao_quy;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_quy_tong_hop;

-- ============================================================================
-- 1. Báo cáo tài chính — HOT (report.financial_report)
-- ============================================================================
CREATE VIEW report.v_bao_cao_tai_chinh AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    SalesQuantity AS `SoLuongBan`,
    ReturnQuantity AS `SoLuongTra`,
    GrossRevenue AS `DoanhThuGop`,
    ReturnAmount AS `GiaTriTraHang`,
    NetRevenue AS `DoanhThuThuan`,
    TotalCogs AS `TongGiaVon`,
    TotalPromotionCost AS `TongChiPhiKhuyenMai`,
    TotalCost AS `TongChiPhi`,
    GrossProfit AS `LoiNhuanGop`,
    ProfitMargin AS `BienLoiNhuan`
FROM report.financial_report;

-- ============================================================================
-- 2. Báo cáo tài chính — COLD (serving.financial_report / Nessie Iceberg)
-- ============================================================================
CREATE VIEW serving.v_bao_cao_tai_chinh AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    SalesQuantity AS `SoLuongBan`,
    ReturnQuantity AS `SoLuongTra`,
    GrossRevenue AS `DoanhThuGop`,
    ReturnAmount AS `GiaTriTraHang`,
    NetRevenue AS `DoanhThuThuan`,
    TotalCogs AS `TongGiaVon`,
    TotalPromotionCost AS `TongChiPhiKhuyenMai`,
    TotalCost AS `TongChiPhi`,
    GrossProfit AS `LoiNhuanGop`,
    ProfitMargin AS `BienLoiNhuan`
FROM serving.financial_report;

-- ============================================================================
-- 3. Báo cáo tài chính — HỢP NHẤT hot + cold
--    Cold: ngày < today() - 30 ngày | Hot: toàn bộ bảng report (30 ngày gần nhất)
-- ============================================================================
CREATE VIEW report.v_bao_cao_tai_chinh_tong_hop AS
SELECT *
FROM serving.v_bao_cao_tai_chinh
WHERE `NgayBaoCao` < today() - toIntervalDay(30)

UNION ALL

SELECT *
FROM report.v_bao_cao_tai_chinh;

-- ============================================================================
-- 4. Báo cáo quỹ / dòng tiền — HOT (report.fund_report)
-- ============================================================================
CREATE VIEW report.v_bao_cao_quy AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    PaymentMethodName AS `TenPhuongThucThanhToan`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM report.fund_report;

-- ============================================================================
-- 5. Báo cáo quỹ — COLD (serving.fund_report / Nessie Iceberg)
-- ============================================================================
CREATE VIEW serving.v_bao_cao_quy AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    PaymentMethodName AS `TenPhuongThucThanhToan`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM serving.fund_report;

-- ============================================================================
-- 6. Báo cáo quỹ — HỢP NHẤT hot + cold
-- ============================================================================
CREATE VIEW report.v_bao_cao_quy_tong_hop AS
SELECT *
FROM serving.v_bao_cao_quy
WHERE `NgayBaoCao` < today() - toIntervalDay(30)

UNION ALL

SELECT *
FROM report.v_bao_cao_quy;
