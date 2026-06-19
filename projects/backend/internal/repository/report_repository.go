package repository

import (
	"context"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ReportRepository struct {
	conn         clickhouse.Conn
	queryTimeout time.Duration
}

func NewReportRepository(conn clickhouse.Conn, queryTimeout time.Duration) *ReportRepository {
	return &ReportRepository{conn: conn, queryTimeout: queryTimeout}
}

// ReportRow is a unified row shape for all report views / Dòng báo cáo thống nhất cho mọi view
type ReportRow struct {
	ReportDate         time.Time `json:"reportDate"`
	ShopCode           string    `json:"shopCode,omitempty"`
	ShopName           string    `json:"shopName,omitempty"`
	ProductId          string    `json:"productId,omitempty"`
	ProductName        string    `json:"productName,omitempty"`
	ProductLineCode    string    `json:"productLineCode,omitempty"`
	ProductLineName    string    `json:"productLineName,omitempty"`
	DocumentType       string    `json:"documentType,omitempty"`
	OrderId            string    `json:"orderId,omitempty"`
	ReturnId           string    `json:"returnId,omitempty"`
	EmployeeSaleId     string    `json:"employeeSaleId,omitempty"`
	OrderStatus        string    `json:"orderStatus,omitempty"`
	PaymentMethodName  string    `json:"paymentMethodName,omitempty"`
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
	TotalReceipts      float64   `json:"totalReceipts"`
	TotalExpenditures  float64   `json:"totalExpenditures"`
	NetCashFlow        float64   `json:"netCashFlow"`
	TotalOrderAmount   float64   `json:"totalOrderAmount"`
	TotalReturnAmount  float64   `json:"totalReturnAmount"`
	ReconciliationDiff float64   `json:"reconciliationDiff"`
}

type ReportSummary struct {
	TotalSalesQuantity  int64   `json:"totalSalesQuantity"`
	TotalReturnQuantity int64   `json:"totalReturnQuantity"`
	TotalGrossRevenue   float64 `json:"totalGrossRevenue"`
	TotalReturnAmount   float64 `json:"totalReturnAmount"`
	TotalNetRevenue     float64 `json:"totalNetRevenue"`
	TotalCost           float64 `json:"totalCost"`
	TotalGrossProfit    float64 `json:"totalGrossProfit"`
	TotalReceipts       float64 `json:"totalReceipts"`
	TotalExpenditures   float64 `json:"totalExpenditures"`
	TotalNetCashFlow    float64 `json:"totalNetCashFlow"`
	TotalOrderAmount    float64 `json:"totalOrderAmount"`
	TotalReturnAmtFund  float64 `json:"totalReturnAmountFund"`
	ReconciliationDiff  float64 `json:"reconciliationDiff"`
}

func normalizeFinancialView(view string) string {
	switch view {
	case FinancialViewDetail, FinancialViewProduct, FinancialViewOrder:
		return view
	default:
		return FinancialViewShop
	}
}

func normalizeFundView(view string) string {
	switch view {
	case FundViewShop, FundViewOrder:
		return view
	default:
		return FundViewPayment
	}
}

func (r *ReportRepository) GetFinancialReport(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
	filter FinancialFilter,
	page int,
	pageSize int,
) ([]ReportRow, int64, error) {
	view = normalizeFinancialView(view)
	spec := financialViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendFinancialFilters(view, filter, where, args)

	countSQL := fmt.Sprintf("SELECT count() FROM %s WHERE %s", spec.table, where)
	var total uint64
	if err := r.conn.QueryRow(queryCtx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	dataSQL := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s",
		spec.columns, spec.table, where, spec.orderBy,
	)
	if pageSize > 0 {
		offset := (page - 1) * pageSize
		dataSQL += " LIMIT ? OFFSET ?"
		args = append(args, pageSize, offset)
	}

	rows, err := r.conn.Query(queryCtx, dataSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	capacity := pageSize
	if capacity <= 0 {
		capacity = int(total)
		if capacity == 0 {
			capacity = 64
		}
	}
	result := make([]ReportRow, 0, capacity)
	for rows.Next() {
		row, err := scanFinancialRow(view, rows)
		if err != nil {
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
	view string,
	filter FinancialFilter,
) (ReportSummary, error) {
	view = normalizeFinancialView(view)
	spec := financialViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendFinancialFilters(view, filter, where, args)

	query := fmt.Sprintf(`
		SELECT
			sum(`+"`SoLuongBan`"+`), sum(`+"`SoLuongTra`"+`), sum(`+"`DoanhThuGop`"+`),
			sum(`+"`GiaTriTraHang`"+`), sum(`+"`DoanhThuThuan`"+`), sum(`+"`TongChiPhi`"+`), sum(`+"`LoiNhuanGop`"+`)
		FROM %s
		WHERE %s`, spec.table, where)

	var summary ReportSummary
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
	view string,
	filter FundFilter,
	page int,
	pageSize int,
) ([]ReportRow, int64, error) {
	view = normalizeFundView(view)
	spec := fundViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendFundFilters(view, filter, where, args)

	countSQL := fmt.Sprintf("SELECT count() FROM %s WHERE %s", spec.table, where)
	var total uint64
	if err := r.conn.QueryRow(queryCtx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	dataSQL := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s",
		spec.columns, spec.table, where, spec.orderBy,
	)
	if pageSize > 0 {
		offset := (page - 1) * pageSize
		dataSQL += " LIMIT ? OFFSET ?"
		args = append(args, pageSize, offset)
	}

	rows, err := r.conn.Query(queryCtx, dataSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	capacity := pageSize
	if capacity <= 0 {
		capacity = int(total)
		if capacity == 0 {
			capacity = 64
		}
	}
	result := make([]ReportRow, 0, capacity)
	for rows.Next() {
		row, err := scanFundRow(view, rows)
		if err != nil {
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
	view string,
	filter FundFilter,
) (ReportSummary, error) {
	view = normalizeFundView(view)
	spec := fundViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendFundFilters(view, filter, where, args)

	query := fmt.Sprintf(`
		SELECT
			sum(`+"`TongThu`"+`), sum(`+"`TongChi`"+`), sum(`+"`DongTienRong`"+`),
			sum(`+"`TongTienDonHang`"+`), sum(`+"`TongTienHoanTra`"+`), sum(`+"`ChenhLechDoiSoat`"+`)
		FROM %s
		WHERE %s`, spec.table, where)

	var summary ReportSummary
	err := r.conn.QueryRow(queryCtx, query, args...).Scan(
		&summary.TotalReceipts,
		&summary.TotalExpenditures,
		&summary.TotalNetCashFlow,
		&summary.TotalOrderAmount,
		&summary.TotalReturnAmtFund,
		&summary.ReconciliationDiff,
	)
	return summary, err
}

func scanFinancialRow(view string, rows driver.Rows) (ReportRow, error) {
	var row ReportRow
	var err error

	switch view {
	case FinancialViewDetail:
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.ProductId, &row.ProductName,
			&row.ProductLineCode, &row.ProductLineName, &row.SalesQuantity, &row.ReturnQuantity,
			&row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue, &row.TotalCogs,
			&row.TotalPromotionCost, &row.TotalCost, &row.GrossProfit, &row.ProfitMargin,
		)
	case FinancialViewProduct:
		err = rows.Scan(
			&row.ReportDate, &row.ProductId, &row.ProductName, &row.ProductLineCode, &row.ProductLineName,
			&row.SalesQuantity, &row.ReturnQuantity, &row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue,
			&row.TotalCogs, &row.TotalPromotionCost, &row.TotalCost, &row.GrossProfit, &row.ProfitMargin,
		)
	case FinancialViewOrder:
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.DocumentType, &row.OrderId, &row.ReturnId,
			&row.EmployeeSaleId, &row.OrderStatus, &row.SalesQuantity, &row.ReturnQuantity,
			&row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue, &row.TotalCogs,
			&row.TotalPromotionCost, &row.TotalCost, &row.GrossProfit, &row.ProfitMargin,
		)
	default:
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.SalesQuantity, &row.ReturnQuantity,
			&row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue, &row.TotalCogs,
			&row.TotalPromotionCost, &row.TotalCost, &row.GrossProfit, &row.ProfitMargin,
		)
	}
	return row, err
}

func scanFundRow(view string, rows driver.Rows) (ReportRow, error) {
	var row ReportRow
	var err error

	switch view {
	case FundViewShop:
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.TotalReceipts, &row.TotalExpenditures,
			&row.NetCashFlow, &row.TotalOrderAmount, &row.TotalReturnAmount, &row.ReconciliationDiff,
		)
	case FundViewOrder:
		err = rows.Scan(
			&row.ReportDate, &row.OrderId, &row.ReturnId, &row.ShopCode, &row.ShopName, &row.PaymentMethodName,
			&row.TotalReceipts, &row.TotalExpenditures, &row.NetCashFlow, &row.TotalOrderAmount,
			&row.TotalReturnAmount, &row.ReconciliationDiff,
		)
	default:
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.PaymentMethodName,
			&row.TotalReceipts, &row.TotalExpenditures, &row.NetCashFlow, &row.TotalOrderAmount,
			&row.TotalReturnAmount, &row.ReconciliationDiff,
		)
	}
	return row, err
}
