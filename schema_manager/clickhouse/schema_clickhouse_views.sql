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
-- Cold from serving / Dữ liệu lạnh từ serving
SELECT * FROM serving.v_bao_cao_tai_chinh
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
-- Hot from report / Dữ liệu nóng từ report (Flink/hot store)
SELECT * FROM report.v_bao_cao_tai_chinh
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
-- Hot fallback from serving when report has no rows for that date / Fallback serving khi report trống ngày đó
SELECT * FROM serving.v_bao_cao_tai_chinh AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_tai_chinh AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_tai_chinh_chi_tiet AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_tai_chinh_chi_tiet AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_tai_chinh_theo_san_pham AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_tai_chinh_theo_san_pham AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_quy AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_quy AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_quy_theo_shop AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_quy_theo_shop AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
UNION ALL
SELECT * FROM serving.v_bao_cao_tai_chinh_theo_don AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_tai_chinh_theo_don AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
UNION ALL
SELECT * FROM serving.v_bao_cao_quy_theo_don AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_quy_theo_don AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(7)
      AND r.`NgayBaoCao` IS NOT NULL
  );

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

-- ============================================================================
-- SUPPLY — inventory health (hot 30d) / Sức khỏe tồn kho
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_suc_khoe_ton_kho;
DROP VIEW IF EXISTS serving.v_bao_cao_suc_khoe_ton_kho;
DROP VIEW IF EXISTS report.v_bao_cao_suc_khoe_ton_kho_tong_hop;

CREATE VIEW report.v_bao_cao_suc_khoe_ton_kho AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    ProductLineCode AS `MaNganhHang`,
    ProductLineName AS `TenNganhHang`,
    OnHandQuantity AS `SoLuongTon`,
    OnHandValue AS `GiaTriTon`,
    OnHandVolumeM3 AS `TheTichTonM3`,
    ExpiredQuantity AS `SoLuongHetHan`,
    SkuCount AS `SoMatHang`,
    StockoutSkuCount AS `SoMatHangHetHang`,
    StockoutRate AS `TyLeHetHang`,
    AverageDaysOfSupply AS `SoNgayDuHangTrungBinh`,
    RequestedQuantity AS `SoLuongYeuCau`,
    LostQuantity AS `SoLuongMatBan`,
    LostRevenueEstimate AS `DoanhThuMatUocTinh`,
    FillRate AS `TyLeDapUng`
FROM report.inventory_health;

-- Cast Iceberg Decimal/Nullable → Float64/Int64 to match hot report.* for UNION
-- Ép kiểu Iceberg Decimal/Nullable → Float64/Int64 khớp hot report.* khi UNION
CREATE VIEW serving.v_bao_cao_suc_khoe_ton_kho AS
SELECT
    toDate(ReportDate) AS `NgayBaoCao`,
    CAST(ShopCode AS String) AS `MaCuaHang`,
    CAST(ShopName AS String) AS `TenCuaHang`,
    CAST(ProductLineCode AS String) AS `MaNganhHang`,
    CAST(ProductLineName AS String) AS `TenNganhHang`,
    toInt64(OnHandQuantity) AS `SoLuongTon`,
    toInt64(OnHandValue) AS `GiaTriTon`,
    toFloat64(OnHandVolumeM3) AS `TheTichTonM3`,
    toInt64(ExpiredQuantity) AS `SoLuongHetHan`,
    toInt64(SkuCount) AS `SoMatHang`,
    toInt64(StockoutSkuCount) AS `SoMatHangHetHang`,
    toFloat64(StockoutRate) AS `TyLeHetHang`,
    toFloat64(AverageDaysOfSupply) AS `SoNgayDuHangTrungBinh`,
    toInt64(RequestedQuantity) AS `SoLuongYeuCau`,
    toInt64(LostQuantity) AS `SoLuongMatBan`,
    toInt64(LostRevenueEstimate) AS `DoanhThuMatUocTinh`,
    toFloat64(FillRate) AS `TyLeDapUng`
FROM serving.inventory_health;

CREATE VIEW report.v_bao_cao_suc_khoe_ton_kho_tong_hop AS
SELECT * FROM serving.v_bao_cao_suc_khoe_ton_kho
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_suc_khoe_ton_kho
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_suc_khoe_ton_kho AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_suc_khoe_ton_kho AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

-- ============================================================================
-- SUPPLY — shop capacity utilization (hot 30d) / Công suất cửa hàng
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_cong_suat_cua_hang;
DROP VIEW IF EXISTS serving.v_bao_cao_cong_suat_cua_hang;
DROP VIEW IF EXISTS report.v_bao_cao_cong_suat_cua_hang_tong_hop;

CREATE VIEW report.v_bao_cao_cong_suat_cua_hang AS
SELECT
    ReportDate AS `NgayBaoCao`,
    ShopCode AS `MaCuaHang`,
    ShopName AS `TenCuaHang`,
    OnHandVolumeM3 AS `TheTichTonM3`,
    OnHandValue AS `GiaTriTon`,
    StorageM3 AS `SucChuaKhoM3`,
    ColdStorageL AS `SucChuaKhoLanhLit`,
    ShelfSlots AS `SoKeHang`,
    ReceivingSlotsPerDay AS `SlotNhanHangNgay`,
    ReceivingCount AS `SoLuotNhanHang`,
    OverCapacitySkuCount AS `SoMatHangVuotSucChua`,
    StorageUtilization AS `TyLeSuDungKho`,
    ReceivingUtilization AS `TyLeSuDungSlotNhan`
FROM report.shop_capacity_utilization;

-- Cast Iceberg Decimal/Nullable → match hot report.* / Ép kiểu Iceberg khớp hot report.*
CREATE VIEW serving.v_bao_cao_cong_suat_cua_hang AS
SELECT
    toDate(ReportDate) AS `NgayBaoCao`,
    CAST(ShopCode AS String) AS `MaCuaHang`,
    CAST(ShopName AS String) AS `TenCuaHang`,
    toFloat64(OnHandVolumeM3) AS `TheTichTonM3`,
    toInt64(OnHandValue) AS `GiaTriTon`,
    toFloat64(StorageM3) AS `SucChuaKhoM3`,
    toInt32(ColdStorageL) AS `SucChuaKhoLanhLit`,
    toInt32(ShelfSlots) AS `SoKeHang`,
    toInt32(ReceivingSlotsPerDay) AS `SlotNhanHangNgay`,
    toInt64(ReceivingCount) AS `SoLuotNhanHang`,
    toInt64(OverCapacitySkuCount) AS `SoMatHangVuotSucChua`,
    toFloat64(StorageUtilization) AS `TyLeSuDungKho`,
    toFloat64(ReceivingUtilization) AS `TyLeSuDungSlotNhan`
FROM serving.shop_capacity_utilization;

CREATE VIEW report.v_bao_cao_cong_suat_cua_hang_tong_hop AS
SELECT * FROM serving.v_bao_cao_cong_suat_cua_hang
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM report.v_bao_cao_cong_suat_cua_hang
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
UNION ALL
SELECT * FROM serving.v_bao_cao_cong_suat_cua_hang AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_cong_suat_cua_hang AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(30)
      AND r.`NgayBaoCao` IS NOT NULL
  );

-- ============================================================================
-- SUPPLY — carrier scorecard (hot 90d) / Thẻ điểm nhà vận chuyển
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_the_diem_nha_van_chuyen;
DROP VIEW IF EXISTS serving.v_bao_cao_the_diem_nha_van_chuyen;
DROP VIEW IF EXISTS report.v_bao_cao_the_diem_nha_van_chuyen_tong_hop;

CREATE VIEW report.v_bao_cao_the_diem_nha_van_chuyen AS
SELECT
    ReportDate AS `NgayBaoCao`,
    CarrierId AS `MaNhaVanChuyen`,
    CarrierName AS `TenNhaVanChuyen`,
    ShipmentCount AS `SoChuyenHang`,
    StopCount AS `SoDiemGiao`,
    DeliveredStopCount AS `SoDiemDaGiao`,
    OnTimeStopCount AS `SoDiemDungHan`,
    OnTimeInFullStopCount AS `SoDiemOTIF`,
    OnTimeInFullRate AS `TyLeOTIF`,
    AverageDelayMinutes AS `ThoiGianTreTrungBinhPhut`,
    TransportCost AS `ChiPhiVanChuyen`,
    TotalWeightKg AS `TongTrongLuongKg`,
    TotalVolumeM3 AS `TongTheTichM3`,
    CostPerKg AS `ChiPhiTrenKg`,
    CostPerStop AS `ChiPhiTrenDiemGiao`,
    AverageWeightLoadFactor AS `HeSoChatTaiTrongLuong`,
    AverageVolumeLoadFactor AS `HeSoChatTaiTheTich`
FROM report.carrier_scorecard;

-- Cast Iceberg Nullable → match hot report.* / Ép kiểu Iceberg khớp hot report.*
CREATE VIEW serving.v_bao_cao_the_diem_nha_van_chuyen AS
SELECT
    toDate(ReportDate) AS `NgayBaoCao`,
    CAST(CarrierId AS String) AS `MaNhaVanChuyen`,
    CAST(CarrierName AS String) AS `TenNhaVanChuyen`,
    toInt64(ShipmentCount) AS `SoChuyenHang`,
    toInt64(StopCount) AS `SoDiemGiao`,
    toInt64(DeliveredStopCount) AS `SoDiemDaGiao`,
    toInt64(OnTimeStopCount) AS `SoDiemDungHan`,
    toInt64(OnTimeInFullStopCount) AS `SoDiemOTIF`,
    toFloat64(OnTimeInFullRate) AS `TyLeOTIF`,
    toFloat64(AverageDelayMinutes) AS `ThoiGianTreTrungBinhPhut`,
    toInt64(TransportCost) AS `ChiPhiVanChuyen`,
    toFloat64(TotalWeightKg) AS `TongTrongLuongKg`,
    toFloat64(TotalVolumeM3) AS `TongTheTichM3`,
    toFloat64(CostPerKg) AS `ChiPhiTrenKg`,
    toFloat64(CostPerStop) AS `ChiPhiTrenDiemGiao`,
    toFloat64(AverageWeightLoadFactor) AS `HeSoChatTaiTrongLuong`,
    toFloat64(AverageVolumeLoadFactor) AS `HeSoChatTaiTheTich`
FROM serving.carrier_scorecard;

CREATE VIEW report.v_bao_cao_the_diem_nha_van_chuyen_tong_hop AS
SELECT * FROM serving.v_bao_cao_the_diem_nha_van_chuyen
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(90)
UNION ALL
SELECT * FROM report.v_bao_cao_the_diem_nha_van_chuyen
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(90)
UNION ALL
SELECT * FROM serving.v_bao_cao_the_diem_nha_van_chuyen AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(90)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_the_diem_nha_van_chuyen AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(90)
      AND r.`NgayBaoCao` IS NOT NULL
  );

-- ============================================================================
-- SUPPLY — supplier performance (hot 365d) / Hiệu suất nhà cung cấp
-- ============================================================================
DROP VIEW IF EXISTS report.v_bao_cao_hieu_suat_nha_cung_cap;
DROP VIEW IF EXISTS serving.v_bao_cao_hieu_suat_nha_cung_cap;
DROP VIEW IF EXISTS report.v_bao_cao_hieu_suat_nha_cung_cap_tong_hop;

CREATE VIEW report.v_bao_cao_hieu_suat_nha_cung_cap AS
SELECT
    ReportDate AS `NgayBaoCao`,
    SupplierId AS `MaNhaCungCap`,
    SupplierName AS `TenNhaCungCap`,
    PurchaseLineCount AS `SoDongDonMua`,
    ReceivedLineCount AS `SoDongDaNhan`,
    OnTimeLineCount AS `SoDongDungHan`,
    OnTimeRate AS `TyLeDungHan`,
    AverageLeadTimeDays AS `LeadTimeThucTeNgay`,
    AveragePromisedLeadTimeDays AS `LeadTimeCamKetNgay`,
    LeadTimeVarianceDays AS `ChenhLechLeadTimeNgay`,
    AverageFillRate AS `TyLeDapUngTrungBinh`,
    OrderedQuantity AS `SoLuongDat`,
    ReceivedQuantity AS `SoLuongNhan`,
    OrderedAmount AS `GiaTriDat`,
    ReceivedAmount AS `GiaTriNhan`
FROM report.supplier_performance;

-- Cast Iceberg Nullable → match hot report.* / Ép kiểu Iceberg khớp hot report.*
CREATE VIEW serving.v_bao_cao_hieu_suat_nha_cung_cap AS
SELECT
    toDate(ReportDate) AS `NgayBaoCao`,
    CAST(SupplierId AS String) AS `MaNhaCungCap`,
    CAST(SupplierName AS String) AS `TenNhaCungCap`,
    toInt64(PurchaseLineCount) AS `SoDongDonMua`,
    toInt64(ReceivedLineCount) AS `SoDongDaNhan`,
    toInt64(OnTimeLineCount) AS `SoDongDungHan`,
    toFloat64(OnTimeRate) AS `TyLeDungHan`,
    toFloat64(AverageLeadTimeDays) AS `LeadTimeThucTeNgay`,
    toFloat64(AveragePromisedLeadTimeDays) AS `LeadTimeCamKetNgay`,
    toFloat64(LeadTimeVarianceDays) AS `ChenhLechLeadTimeNgay`,
    toFloat64(AverageFillRate) AS `TyLeDapUngTrungBinh`,
    toInt64(OrderedQuantity) AS `SoLuongDat`,
    toInt64(ReceivedQuantity) AS `SoLuongNhan`,
    toInt64(OrderedAmount) AS `GiaTriDat`,
    toInt64(ReceivedAmount) AS `GiaTriNhan`
FROM serving.supplier_performance;

CREATE VIEW report.v_bao_cao_hieu_suat_nha_cung_cap_tong_hop AS
SELECT * FROM serving.v_bao_cao_hieu_suat_nha_cung_cap
WHERE `NgayBaoCao` < toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(365)
UNION ALL
SELECT * FROM report.v_bao_cao_hieu_suat_nha_cung_cap
WHERE `NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(365)
UNION ALL
SELECT * FROM serving.v_bao_cao_hieu_suat_nha_cung_cap AS s
WHERE s.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(365)
  AND s.`NgayBaoCao` NOT IN (
    SELECT DISTINCT r.`NgayBaoCao`
    FROM report.v_bao_cao_hieu_suat_nha_cung_cap AS r
    WHERE r.`NgayBaoCao` >= toDate(now('Asia/Ho_Chi_Minh')) - toIntervalDay(365)
      AND r.`NgayBaoCao` IS NOT NULL
  );
