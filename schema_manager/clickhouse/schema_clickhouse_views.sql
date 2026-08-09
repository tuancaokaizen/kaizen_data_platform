-- ClickHouse Report Views — Hot/Cold với tên cột tiếng Việt
-- Hot layer  : report.* (ReplacingMergeTree, dữ liệu 30 ngày gần nhất)
-- Cold layer : serving.* (Iceberg/Nessie, lịch sử đầy đủ trên MinIO)

CREATE DATABASE IF NOT EXISTS report;
CREATE DATABASE IF NOT EXISTS serving;

-- Clean legacy view names / Dọn view cũ
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_nong;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_lanh;
DROP VIEW IF EXISTS report.v_bao_cao_quy_nong;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_lanh;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_chi_tiet_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_san_pham_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_chi_tiet;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_san_pham;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_chi_tiet;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_theo_san_pham;
DROP VIEW IF EXISTS report.v_bao_cao_quy_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_shop_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_don_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_don;
DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_theo_don;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_don_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_don;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_don;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_san_pham_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_quy_chi_tiet_tong_hop;
DROP VIEW IF EXISTS report.v_bao_cao_quy;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_shop;
DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_san_pham;
DROP VIEW IF EXISTS report.v_bao_cao_quy_chi_tiet;
DROP VIEW IF EXISTS serving.v_bao_cao_quy;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_shop;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_san_pham;
DROP VIEW IF EXISTS serving.v_bao_cao_quy_chi_tiet;

-- ============================================================================
-- FINANCIAL — shop grain (backward compatible / tương thích backend hiện tại)
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

CREATE VIEW report.v_bao_cao_tai_chinh_tong_hop AS
SELECT * FROM serving.v_bao_cao_tai_chinh
WHERE `NgayBaoCao` < today() - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh;

-- ============================================================================
-- FINANCIAL — detail grain (shop + product + line)
-- ============================================================================
CREATE VIEW report.v_bao_cao_tai_chinh_chi_tiet AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    ProductId AS `MaSanPham`,
    ProductName AS `TenSanPham`,
    ProductLineCode AS `MaNganhHang`,
    ProductLineName AS `TenNganhHang`,
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
FROM report.financial_report_detail;

CREATE VIEW serving.v_bao_cao_tai_chinh_chi_tiet AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    ProductId AS `MaSanPham`,
    ProductName AS `TenSanPham`,
    ProductLineCode AS `MaNganhHang`,
    ProductLineName AS `TenNganhHang`,
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
FROM serving.financial_report_detail;

CREATE VIEW report.v_bao_cao_tai_chinh_chi_tiet_tong_hop AS
SELECT * FROM serving.v_bao_cao_tai_chinh_chi_tiet
WHERE `NgayBaoCao` < today() - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_chi_tiet;

-- ============================================================================
-- FINANCIAL — product grain
-- ============================================================================
CREATE VIEW report.v_bao_cao_tai_chinh_theo_san_pham AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ProductId AS `MaSanPham`,
    ProductName AS `TenSanPham`,
    ProductLineCode AS `MaNganhHang`,
    ProductLineName AS `TenNganhHang`,
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
FROM report.financial_report_product;

CREATE VIEW serving.v_bao_cao_tai_chinh_theo_san_pham AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ProductId AS `MaSanPham`,
    ProductName AS `TenSanPham`,
    ProductLineCode AS `MaNganhHang`,
    ProductLineName AS `TenNganhHang`,
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
FROM serving.financial_report_product;

CREATE VIEW report.v_bao_cao_tai_chinh_theo_san_pham_tong_hop AS
SELECT * FROM serving.v_bao_cao_tai_chinh_theo_san_pham
WHERE `NgayBaoCao` < today() - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_theo_san_pham;

-- ============================================================================
-- FUND — payment grain (shop + PTTT, backward compatible)
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

CREATE VIEW report.v_bao_cao_quy_tong_hop AS
SELECT * FROM serving.v_bao_cao_quy
WHERE `NgayBaoCao` < today() - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_quy;

-- ============================================================================
-- FUND — shop grain
-- ============================================================================
CREATE VIEW report.v_bao_cao_quy_theo_shop AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM report.fund_report_shop;

CREATE VIEW serving.v_bao_cao_quy_theo_shop AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM serving.fund_report_shop;

CREATE VIEW report.v_bao_cao_quy_theo_shop_tong_hop AS
SELECT * FROM serving.v_bao_cao_quy_theo_shop
WHERE `NgayBaoCao` < today() - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_quy_theo_shop;

-- ============================================================================
-- FINANCIAL — order grain (hot 7 days in push job) / Theo từng đơn hàng
-- ============================================================================
CREATE VIEW report.v_bao_cao_tai_chinh_theo_don AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    DocumentType AS `LoaiChungTu`,
    OrderId AS `MaDonHang`,
    ReturnId AS `MaPhieuTra`,
    EmployeeSaleId AS `MaNhanVienBan`,
    OrderStatus AS `TrangThaiDon`,
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
FROM report.financial_report_order;

CREATE VIEW serving.v_bao_cao_tai_chinh_theo_don AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    DocumentType AS `LoaiChungTu`,
    OrderId AS `MaDonHang`,
    ReturnId AS `MaPhieuTra`,
    EmployeeSaleId AS `MaNhanVienBan`,
    OrderStatus AS `TrangThaiDon`,
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
FROM serving.financial_report_order;

CREATE VIEW report.v_bao_cao_tai_chinh_theo_don_tong_hop AS
SELECT * FROM serving.v_bao_cao_tai_chinh_theo_don
WHERE `NgayBaoCao` < today() - toIntervalDay(7)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_theo_don;

-- ============================================================================
-- FUND — order grain (hot 7 days in push job) / Quỹ theo từng đơn × PTTT
-- ============================================================================
CREATE VIEW report.v_bao_cao_quy_theo_don AS
SELECT
    ReportDate AS `NgayBaoCao`,
    OrderId AS `MaDonHang`,
    ReturnId AS `MaPhieuTra`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    PaymentMethodName AS `TenPhuongThucThanhToan`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM report.fund_report_order;

CREATE VIEW serving.v_bao_cao_quy_theo_don AS
SELECT
    ReportDate AS `NgayBaoCao`,
    OrderId AS `MaDonHang`,
    ReturnId AS `MaPhieuTra`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    PaymentMethodName AS `TenPhuongThucThanhToan`,
    TotalReceipts AS `TongThu`,
    TotalExpenditures AS `TongChi`,
    NetCashFlow AS `DongTienRong`,
    TotalOrderAmount AS `TongTienDonHang`,
    TotalReturnAmount AS `TongTienHoanTra`,
    ReconciliationDiff AS `ChenhLechDoiSoat`
FROM serving.fund_report_order;

CREATE VIEW report.v_bao_cao_quy_theo_don_tong_hop AS
SELECT * FROM serving.v_bao_cao_quy_theo_don
WHERE `NgayBaoCao` < today() - toIntervalDay(7)
UNION ALL
SELECT * FROM report.v_bao_cao_quy_theo_don;

-- ============================================================================
-- REALTIME business-by-order (Flink) — view hot 14d / cold Iceberg (CH table TTL 16d buffer)
-- Tình hình KD theo đơn realtime — view hot 14 ngày / cold Iceberg (bảng hot TTL 16 ngày buffer)
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_kd_theo_don_rt;
DROP VIEW IF EXISTS serving.v_bao_cao_kd_theo_don_rt;
DROP VIEW IF EXISTS report.v_bao_cao_kd_theo_don_rt_tong_hop;

-- Cold stub so views deploy before first Iceberg migrate / Stub cold để deploy view trước migrate
CREATE TABLE IF NOT EXISTS serving.biz_rt_order (
    ReportDate Date,
    OrderId String,
    ReturnId String,
    DocumentType LowCardinality(String),
    ShopCode String,
    ShopName String,
    OrderStatus String,
    SalesAmount Float64,
    ReturnAmount Float64,
    NetRevenue Float64,
    TotalReceipts Float64,
    TotalExpenditures Float64,
    NetCashFlow Float64,
    PaymentMethodSummary String,
    TotalOrderAmount Float64,
    TotalReturnAmount Float64,
    ReconciliationDiff Float64,
    PendingOrder UInt8,
    UpdatedAt DateTime64(3)
) ENGINE = ReplacingMergeTree(UpdatedAt)
ORDER BY (OrderId, ReturnId);

CREATE VIEW report.v_bao_cao_kd_theo_don_rt AS
SELECT
    r.ReportDate AS `NgayBaoCao`,
    r.OrderId AS `MaDonHang`,
    r.ReturnId AS `MaPhieuTra`,
    r.DocumentType AS `LoaiChungTu`,
    r.ShopCode AS `MaCuaHang`,
    coalesce(nullIf(ds.ShopName, ''), nullIf(r.ShopName, r.ShopCode), r.ShopCode) AS `TenCuaHang`,
    r.OrderStatus AS `TrangThaiDon`,
    r.SalesAmount AS `DoanhThuBan`,
    r.ReturnAmount AS `GiaTriTraHang`,
    r.NetRevenue AS `DoanhThuThuan`,
    r.TotalReceipts AS `TongThu`,
    r.TotalExpenditures AS `TongChi`,
    r.NetCashFlow AS `DongTienRong`,
    r.PaymentMethodSummary AS `TomTatPTTT`,
    r.TotalOrderAmount AS `TongTienDonHang`,
    r.TotalReturnAmount AS `TongTienHoanTra`,
    r.ReconciliationDiff AS `ChenhLechDoiSoat`,
    r.PendingOrder AS `ChoDonHang`
FROM report.biz_rt_order AS r FINAL
LEFT JOIN (
    SELECT ShopCode, any(ShopName) AS ShopName
    FROM dw.dim_shop
    GROUP BY ShopCode
) AS ds ON r.ShopCode = ds.ShopCode;

CREATE VIEW serving.v_bao_cao_kd_theo_don_rt AS
SELECT
    r.ReportDate AS `NgayBaoCao`,
    r.OrderId AS `MaDonHang`,
    r.ReturnId AS `MaPhieuTra`,
    r.DocumentType AS `LoaiChungTu`,
    r.ShopCode AS `MaCuaHang`,
    coalesce(nullIf(ds.ShopName, ''), nullIf(r.ShopName, r.ShopCode), r.ShopCode) AS `TenCuaHang`,
    r.OrderStatus AS `TrangThaiDon`,
    r.SalesAmount AS `DoanhThuBan`,
    r.ReturnAmount AS `GiaTriTraHang`,
    r.NetRevenue AS `DoanhThuThuan`,
    r.TotalReceipts AS `TongThu`,
    r.TotalExpenditures AS `TongChi`,
    r.NetCashFlow AS `DongTienRong`,
    r.PaymentMethodSummary AS `TomTatPTTT`,
    r.TotalOrderAmount AS `TongTienDonHang`,
    r.TotalReturnAmount AS `TongTienHoanTra`,
    r.ReconciliationDiff AS `ChenhLechDoiSoat`,
    r.PendingOrder AS `ChoDonHang`
FROM serving.biz_rt_order AS r
LEFT JOIN (
    SELECT ShopCode, any(ShopName) AS ShopName
    FROM dw.dim_shop
    GROUP BY ShopCode
) AS ds ON r.ShopCode = ds.ShopCode;

CREATE VIEW report.v_bao_cao_kd_theo_don_rt_tong_hop AS
SELECT * FROM serving.v_bao_cao_kd_theo_don_rt
WHERE `NgayBaoCao` < today() - toIntervalDay(14)
UNION ALL
SELECT * FROM report.v_bao_cao_kd_theo_don_rt
WHERE `NgayBaoCao` >= today() - toIntervalDay(14);
