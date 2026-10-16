-- ClickHouse Report Views — Hot/Cold với tên cột tiếng Việt
-- Hot layer  : report.* (ReplacingMergeTree, dữ liệu 30 ngày gần nhất)
-- Cold layer : serving.* (Iceberg/Nessie, lịch sử đầy đủ trên MinIO)
-- Each *_tong_hop view filters BOTH sides on the same ICT cutoff: hot is refilled once a day, so between
-- midnight and the next push it still holds the day that cold now covers. The server runs in UTC.
-- Mỗi view *_tong_hop lọc CẢ HAI phía theo cùng mốc giờ VN: hot chỉ nạp lại mỗi ngày một lần nên từ nửa đêm
-- tới lần push kế tiếp vẫn còn ngày mà cold đã phủ. Server chạy múi giờ UTC.

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_chi_tiet
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_theo_san_pham
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_quy
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_quy_theo_shop
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
UNION ALL
SELECT * FROM report.v_bao_cao_tai_chinh_theo_don
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7);

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
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
UNION ALL
SELECT * FROM report.v_bao_cao_quy_theo_don
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7);

-- ============================================================================
-- REALTIME business-by-order (Flink) — view hot 14d / cold Iceberg (CH table TTL 16d buffer)
-- Tình hình KD theo đơn realtime — view hot 14 ngày / cold Iceberg (bảng hot TTL 16 ngày buffer)
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_kinh_doanh_theo_don_realtime;
DROP VIEW IF EXISTS serving.v_bao_cao_kinh_doanh_theo_don_realtime;
DROP VIEW IF EXISTS report.v_bao_cao_kinh_doanh_theo_don_realtime_tong_hop;

-- Cold stub so views deploy before first Iceberg migrate / Stub cold để deploy view trước migrate
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

-- Shop names come with the rows (Flink reads shop."Shop"), so the realtime views do not depend on dw.
-- Money is exposed as Float64 like the other report views (backend scans float64).
-- Tên cửa hàng đi kèm dòng (Flink đọc shop."Shop") nên view realtime không phụ thuộc dw.
-- Tiền xuất ra Float64 như các view báo cáo khác (backend scan float64).
CREATE VIEW report.v_bao_cao_kinh_doanh_theo_don_realtime AS
SELECT
    r.ReportDate AS `NgayBaoCao`,
    r.OrderId AS `MaDonHang`,
    r.ReturnId AS `MaPhieuTra`,
    r.DocumentType AS `LoaiChungTu`,
    r.ShopCode AS `MaCuaHang`,
    if(r.ShopName = '', r.ShopCode, r.ShopName) AS `TenCuaHang`,
    r.OrderStatus AS `TrangThaiDon`,
    toFloat64(r.SalesAmount) AS `DoanhThuBan`,
    toFloat64(r.ReturnAmount) AS `GiaTriTraHang`,
    toFloat64(r.NetRevenue) AS `DoanhThuThuan`,
    toFloat64(r.TotalReceipts) AS `TongThu`,
    toFloat64(r.TotalExpenditures) AS `TongChi`,
    toFloat64(r.NetCashFlow) AS `DongTienRong`,
    r.PaymentMethodSummary AS `TomTatPTTT`,
    toFloat64(r.TotalOrderAmount) AS `TongTienDonHang`,
    toFloat64(r.TotalReturnAmount) AS `TongTienHoanTra`,
    toFloat64(r.ReconciliationDiff) AS `ChenhLechDoiSoat`,
    r.PendingOrder AS `ChoDonHang`
FROM report.realtime_business_by_order AS r FINAL;

CREATE VIEW serving.v_bao_cao_kinh_doanh_theo_don_realtime AS
SELECT
    r.ReportDate AS `NgayBaoCao`,
    r.OrderId AS `MaDonHang`,
    r.ReturnId AS `MaPhieuTra`,
    r.DocumentType AS `LoaiChungTu`,
    r.ShopCode AS `MaCuaHang`,
    if(r.ShopName = '', r.ShopCode, r.ShopName) AS `TenCuaHang`,
    r.OrderStatus AS `TrangThaiDon`,
    toFloat64(r.SalesAmount) AS `DoanhThuBan`,
    toFloat64(r.ReturnAmount) AS `GiaTriTraHang`,
    toFloat64(r.NetRevenue) AS `DoanhThuThuan`,
    toFloat64(r.TotalReceipts) AS `TongThu`,
    toFloat64(r.TotalExpenditures) AS `TongChi`,
    toFloat64(r.NetCashFlow) AS `DongTienRong`,
    r.PaymentMethodSummary AS `TomTatPTTT`,
    toFloat64(r.TotalOrderAmount) AS `TongTienDonHang`,
    toFloat64(r.TotalReturnAmount) AS `TongTienHoanTra`,
    toFloat64(r.ReconciliationDiff) AS `ChenhLechDoiSoat`,
    r.PendingOrder AS `ChoDonHang`
FROM serving.realtime_business_by_order AS r;

CREATE VIEW report.v_bao_cao_kinh_doanh_theo_don_realtime_tong_hop AS
SELECT * FROM serving.v_bao_cao_kinh_doanh_theo_don_realtime
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(14)
UNION ALL
SELECT * FROM report.v_bao_cao_kinh_doanh_theo_don_realtime
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(14);
