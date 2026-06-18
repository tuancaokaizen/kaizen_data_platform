package repository

import (
	"context"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ReportRepository struct {
	conn         clickhouse.Conn
	queryTimeout time.Duration
}

func NewReportRepository(conn clickhouse.Conn, queryTimeout time.Duration) *ReportRepository {
	return &ReportRepository{conn: conn, queryTimeout: queryTimeout}
}

type FinancialRow struct {
	ReportDate         time.Time `json:"reportDate"`
	ShopCode           string    `json:"shopCode"`
	ShopName           string    `json:"shopName"`
	SalesQuantity      int64     `json:"salesQuantity"`
	ReturnQuantity     int64     `json:"returnQuantity"`
	GrossRevenue       float64   `json:"grossRevenue"`
	ReturnAmount       float64   `json:"returnAmount"`
	NetRevenue         float64   `json:"netRevenue"`
	TotalCogs          float64   `json:"totalCogs"`
	TotalPromotionCost float64   `json:"totalPromotionCost"`
	TotalCost          float64   `json:"totalCost"`
	GrossProfit        float64   `json:"grossProfit"`
	ProfitMargin       float64   `json:"profitMargin"`
}

type FinancialSummary struct {
	TotalSalesQuantity  int64   `json:"totalSalesQuantity"`
	TotalReturnQuantity int64   `json:"totalReturnQuantity"`
	TotalGrossRevenue   float64 `json:"totalGrossRevenue"`
	TotalReturnAmount   float64 `json:"totalReturnAmount"`
	TotalNetRevenue     float64 `json:"totalNetRevenue"`
	TotalCost           float64 `json:"totalCost"`
	TotalGrossProfit    float64 `json:"totalGrossProfit"`
}

type FundRow struct {
	ReportDate         time.Time `json:"reportDate"`
	ShopCode           string    `json:"shopCode"`
	ShopName           string    `json:"shopName"`
	PaymentMethodName  string    `json:"paymentMethodName"`
	TotalReceipts      float64   `json:"totalReceipts"`
	TotalExpenditures  float64   `json:"totalExpenditures"`
	NetCashFlow        float64   `json:"netCashFlow"`
	TotalOrderAmount   float64   `json:"totalOrderAmount"`
	TotalReturnAmount  float64   `json:"totalReturnAmount"`
	ReconciliationDiff float64   `json:"reconciliationDiff"`
}

type FundSummary struct {
	TotalReceipts      float64 `json:"totalReceipts"`
	TotalExpenditures  float64 `json:"totalExpenditures"`
	TotalNetCashFlow   float64 `json:"totalNetCashFlow"`
	TotalOrderAmount   float64 `json:"totalOrderAmount"`
	TotalReturnAmount  float64 `json:"totalReturnAmount"`
	ReconciliationDiff float64 `json:"reconciliationDiff"`
}

func (r *ReportRepository) GetFinancialReport(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	shopCode string,
	page int,
	pageSize int,
) ([]FinancialRow, int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	// Build dynamic where clause against Vietnamese view columns / Tạo where clause theo cột tiếng Việt của view
	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	if shopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, shopCode)
	}

	countSQL := fmt.Sprintf("SELECT count() FROM report.v_bao_cao_tai_chinh_tong_hop WHERE %s", where)
	var total uint64
	if err := r.conn.QueryRow(queryCtx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	dataSQL := fmt.Sprintf(`
		SELECT
			`+"`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `SoLuongBan`, `SoLuongTra`,"+`
			`+"`DoanhThuGop`, `GiaTriTraHang`, `DoanhThuThuan`, `TongGiaVon`,"+`
			`+"`TongChiPhiKhuyenMai`, `TongChiPhi`, `LoiNhuanGop`, `BienLoiNhuan`"+`
		FROM report.v_bao_cao_tai_chinh_tong_hop
		WHERE %s
		ORDER BY `+"`NgayBaoCao` DESC, `MaCuaHang`"+`
		LIMIT ? OFFSET ?`, where)
	args = append(args, pageSize, offset)

	rows, err := r.conn.Query(queryCtx, dataSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	result := make([]FinancialRow, 0, pageSize)
	for rows.Next() {
		var row FinancialRow
		if err := rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.SalesQuantity, &row.ReturnQuantity,
			&row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue, &row.TotalCogs,
			&row.TotalPromotionCost, &row.TotalCost, &row.GrossProfit, &row.ProfitMargin,
		); err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}
	return result, int64(total), nil
}

func (r *ReportRepository) GetFinancialSummary(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	shopCode string,
) (FinancialSummary, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	if shopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, shopCode)
	}

	query := fmt.Sprintf(`
		SELECT
			sum(`+"`SoLuongBan`"+`), sum(`+"`SoLuongTra`"+`), sum(`+"`DoanhThuGop`"+`),
			sum(`+"`GiaTriTraHang`"+`), sum(`+"`DoanhThuThuan`"+`), sum(`+"`TongChiPhi`"+`), sum(`+"`LoiNhuanGop`"+`)
		FROM report.v_bao_cao_tai_chinh_tong_hop
		WHERE %s`, where)

	var summary FinancialSummary
	err := r.conn.QueryRow(queryCtx, query, args...).Scan(
		&summary.TotalSalesQuantity,
		&summary.TotalReturnQuantity,
		&summary.TotalGrossRevenue,
		&summary.TotalReturnAmount,
		&summary.TotalNetRevenue,
		&summary.TotalCost,
		&summary.TotalGrossProfit,
	)
	return summary, err
}

func (r *ReportRepository) GetFundReport(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	shopCode string,
	paymentMethodName string,
	page int,
	pageSize int,
) ([]FundRow, int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	if shopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, shopCode)
	}
	if paymentMethodName != "" {
		where += " AND `TenPhuongThucThanhToan` = ?"
		args = append(args, paymentMethodName)
	}

	countSQL := fmt.Sprintf("SELECT count() FROM report.v_bao_cao_quy_tong_hop WHERE %s", where)
	var total uint64
	if err := r.conn.QueryRow(queryCtx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	dataSQL := fmt.Sprintf(`
		SELECT
			`+"`NgayBaoCao`, `MaCuaHang`, `TenCuaHang`, `TenPhuongThucThanhToan`,"+`
			`+"`TongThu`, `TongChi`, `DongTienRong`, `TongTienDonHang`, `TongTienHoanTra`, `ChenhLechDoiSoat`"+`
		FROM report.v_bao_cao_quy_tong_hop
		WHERE %s
		ORDER BY `+"`NgayBaoCao` DESC, `MaCuaHang`, `TenPhuongThucThanhToan`"+`
		LIMIT ? OFFSET ?`, where)
	args = append(args, pageSize, offset)

	rows, err := r.conn.Query(queryCtx, dataSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	result := make([]FundRow, 0, pageSize)
	for rows.Next() {
		var row FundRow
		if err := rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.PaymentMethodName,
			&row.TotalReceipts, &row.TotalExpenditures, &row.NetCashFlow,
			&row.TotalOrderAmount, &row.TotalReturnAmount, &row.ReconciliationDiff,
		); err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}
	return result, int64(total), nil
}

func (r *ReportRepository) GetFundSummary(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	shopCode string,
	paymentMethodName string,
) (FundSummary, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	if shopCode != "" {
		where += " AND `MaCuaHang` = ?"
		args = append(args, shopCode)
	}
	if paymentMethodName != "" {
		where += " AND `TenPhuongThucThanhToan` = ?"
		args = append(args, paymentMethodName)
	}

	query := fmt.Sprintf(`
		SELECT
			sum(`+"`TongThu`"+`), sum(`+"`TongChi`"+`), sum(`+"`DongTienRong`"+`),
			sum(`+"`TongTienDonHang`"+`), sum(`+"`TongTienHoanTra`"+`), sum(`+"`ChenhLechDoiSoat`"+`)
		FROM report.v_bao_cao_quy_tong_hop
		WHERE %s`, where)

	var summary FundSummary
	err := r.conn.QueryRow(queryCtx, query, args...).Scan(
		&summary.TotalReceipts,
		&summary.TotalExpenditures,
		&summary.TotalNetCashFlow,
		&summary.TotalOrderAmount,
		&summary.TotalReturnAmount,
		&summary.ReconciliationDiff,
	)
	return summary, err
}
