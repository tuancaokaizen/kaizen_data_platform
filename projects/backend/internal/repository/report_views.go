package repository

// Financial view keys map to ClickHouse _tong_hop views / View tài chính
const (
	FinancialViewShop    = "shop"
	FinancialViewDetail  = "detail"
	FinancialViewProduct = "product"
	FinancialViewOrder   = "order"
)

// Fund view keys map to ClickHouse _tong_hop views / View quỹ
const (
	FundViewPayment = "payment"
	FundViewShop    = "shop"
	FundViewOrder   = "order"
)

// KdViewOrder — KD realtime by order (Flink hot + Iceberg cold union) / KD realtime theo đơn
const (
	KdViewOrder = "order"
)

type FinancialFilter struct {
	ShopCode        string
	ProductId       string
	ProductLineCode string
	OrderId         string
	DocumentType    string
	OrderStatus     string
}

type FundFilter struct {
	ShopCode          string
	PaymentMethodName string
	OrderId           string
}

type KdFilter struct {
	ShopCode     string
	OrderId      string
	DocumentType string
	OrderStatus  string
}

type financialViewSpec struct {
	table   string
	columns string
	orderBy string
}

func financialViewSpecFor(view string) financialViewSpec {
	switch view {
	case FinancialViewDetail:
		return financialViewSpec{
			table: "report.v_bao_cao_tai_chinh_chi_tiet_tong_hop",
			columns: "`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `MaSanPham`, `TenSanPham`, `MaNganhHang`, `TenNganhHang`, " +
				"`SoLuongBan`, `SoLuongTra`, `DoanhThuGop`, `GiaTriTraHang`, `DoanhThuThuan`, `TongGiaVon`, " +
				"`TongChiPhiKhuyenMai`, `TongChiPhi`, `LoiNhuanGop`, `BienLoiNhuan`",
			orderBy: "`NgayBaoCao` DESC, `MaCuaHang`, `MaSanPham`",
		}
	case FinancialViewProduct:
		return financialViewSpec{
			table: "report.v_bao_cao_tai_chinh_theo_san_pham_tong_hop",
			columns: "`NgayBaoCao`, `MaSanPham`, `TenSanPham`, `MaNganhHang`, `TenNganhHang`, " +
				"`SoLuongBan`, `SoLuongTra`, `DoanhThuGop`, `GiaTriTraHang`, `DoanhThuThuan`, `TongGiaVon`, " +
				"`TongChiPhiKhuyenMai`, `TongChiPhi`, `LoiNhuanGop`, `BienLoiNhuan`",
			orderBy: "`NgayBaoCao` DESC, `MaSanPham`",
		}
	case FinancialViewOrder:
		return financialViewSpec{
			table: "report.v_bao_cao_tai_chinh_theo_don_tong_hop",
			columns: "`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `LoaiChungTu`, `MaDonHang`, `MaPhieuTra`, " +
				"`MaNhanVienBan`, `TrangThaiDon`, `SoLuongBan`, `SoLuongTra`, `DoanhThuGop`, `GiaTriTraHang`, " +
				"`DoanhThuThuan`, `TongGiaVon`, `TongChiPhiKhuyenMai`, `TongChiPhi`, `LoiNhuanGop`, `BienLoiNhuan`",
			orderBy: "`NgayBaoCao` DESC, `MaCuaHang`, `MaDonHang`",
		}
	default:
		return financialViewSpec{
			table: "report.v_bao_cao_tai_chinh_tong_hop",
			columns: "`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `SoLuongBan`, `SoLuongTra`, `DoanhThuGop`, " +
				"`GiaTriTraHang`, `DoanhThuThuan`, `TongGiaVon`, `TongChiPhiKhuyenMai`, `TongChiPhi`, `LoiNhuanGop`, `BienLoiNhuan`",
			orderBy: "`NgayBaoCao` DESC, `MaCuaHang`",
		}
	}
}

func fundViewSpecFor(view string) financialViewSpec {
	switch view {
	case FundViewShop:
		return financialViewSpec{
			table: "report.v_bao_cao_quy_theo_shop_tong_hop",
			columns: "`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `TongThu`, `TongChi`, `DongTienRong`, " +
				"`TongTienDonHang`, `TongTienHoanTra`, `ChenhLechDoiSoat`",
			orderBy: "`NgayBaoCao` DESC, `MaCuaHang`",
		}
	case FundViewOrder:
		return financialViewSpec{
			table: "report.v_bao_cao_quy_theo_don_tong_hop",
			columns: "`NgayBaoCao`, `MaDonHang`, `MaPhieuTra`, `MaCuaHang`, `TenCuaHang`, `TenPhuongThucThanhToan`, " +
				"`TongThu`, `TongChi`, `DongTienRong`, `TongTienDonHang`, `TongTienHoanTra`, `ChenhLechDoiSoat`",
			orderBy: "`NgayBaoCao` DESC, `MaDonHang`, `TenPhuongThucThanhToan`",
		}
	default:
		return financialViewSpec{
			table: "report.v_bao_cao_quy_tong_hop",
			columns: "`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `TenPhuongThucThanhToan`, `TongThu`, `TongChi`, " +
				"`DongTienRong`, `TongTienDonHang`, `TongTienHoanTra`, `ChenhLechDoiSoat`",
			orderBy: "`NgayBaoCao` DESC, `MaCuaHang`, `TenPhuongThucThanhToan`",
		}
	}
}

func appendFinancialFilters(view string, filter FinancialFilter, where string, args []any) (string, []any) {
	if filter.ShopCode != "" && view != FinancialViewProduct {
		where += " AND `MaCuaHang` = ?"
		args = append(args, filter.ShopCode)
	}
	if filter.ProductId != "" && (view == FinancialViewDetail || view == FinancialViewProduct) {
		where += " AND `MaSanPham` = ?"
		args = append(args, filter.ProductId)
	}
	if filter.ProductLineCode != "" && (view == FinancialViewDetail || view == FinancialViewProduct) {
		where += " AND `MaNganhHang` = ?"
		args = append(args, filter.ProductLineCode)
	}
	if filter.OrderId != "" && view == FinancialViewOrder {
		where += " AND `MaDonHang` = ?"
		args = append(args, filter.OrderId)
	}
	if filter.DocumentType != "" && view == FinancialViewOrder {
		where += " AND `LoaiChungTu` = ?"
		args = append(args, filter.DocumentType)
	}
	if filter.OrderStatus != "" && view == FinancialViewOrder {
		where += " AND `TrangThaiDon` = ?"
		args = append(args, filter.OrderStatus)
	}
	return where, args
}

func kdViewSpecFor(view string) financialViewSpec {
	switch view {
	default:
		return financialViewSpec{
			table: "report.v_bao_cao_kinh_doanh_theo_don_realtime_tong_hop",
			columns: "`NgayBaoCao`, `MaDonHang`, `MaPhieuTra`, `LoaiChungTu`, `MaCuaHang`, `TenCuaHang`, `TrangThaiDon`, " +
				"`DoanhThuBan`, `GiaTriTraHang`, `DoanhThuThuan`, `TongThu`, `TongChi`, `DongTienRong`, `TomTatPTTT`, " +
				"`TongTienDonHang`, `TongTienHoanTra`, `ChenhLechDoiSoat`, `ChoDonHang`",
			orderBy: "`NgayBaoCao` DESC, `MaDonHang`, `MaPhieuTra`",
		}
	}
}

func appendKdFilters(filter KdFilter, where string, args []any) (string, []any) {
	if filter.ShopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, filter.ShopCode)
	}
	if filter.OrderId != "" {
		where += " AND `MaDonHang` = ?"
		args = append(args, filter.OrderId)
	}
	if filter.DocumentType != "" {
		where += " AND `LoaiChungTu` = ?"
		args = append(args, filter.DocumentType)
	}
	if filter.OrderStatus != "" {
		where += " AND `TrangThaiDon` = ?"
		args = append(args, filter.OrderStatus)
	}
	return where, args
}

func appendFundFilters(view string, filter FundFilter, where string, args []any) (string, []any) {
	if filter.ShopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, filter.ShopCode)
	}
	if filter.PaymentMethodName != "" && view != FundViewShop {
		where += " AND `TenPhuongThucThanhToan` = ?"
		args = append(args, filter.PaymentMethodName)
	}
	if filter.OrderId != "" && view == FundViewOrder {
		where += " AND `MaDonHang` = ?"
		args = append(args, filter.OrderId)
	}
	return where, args
}
