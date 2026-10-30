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
	PaymentMethodSummary string  `json:"paymentMethodSummary,omitempty"`
	PendingOrder         int32     `json:"pendingOrder"`
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
	// Supply — inventory / Tồn kho
	CarrierId              string  `json:"carrierId,omitempty"`
	CarrierName            string  `json:"carrierName,omitempty"`
	SupplierId             string  `json:"supplierId,omitempty"`
	SupplierName           string  `json:"supplierName,omitempty"`
	OnHandQuantity         int64   `json:"onHandQuantity"`
	OnHandValue            float64 `json:"onHandValue"`
	OnHandVolumeM3         float64 `json:"onHandVolumeM3"`
	ExpiredQuantity        int64   `json:"expiredQuantity"`
	SkuCount               int64   `json:"skuCount"`
	StockoutSkuCount       int64   `json:"stockoutSkuCount"`
	StockoutRate           float64 `json:"stockoutRate"`
	AverageDaysOfSupply    float64 `json:"averageDaysOfSupply"`
	RequestedQuantity      int64   `json:"requestedQuantity"`
	LostQuantity           int64   `json:"lostQuantity"`
	LostRevenueEstimate    float64 `json:"lostRevenueEstimate"`
	FillRate               float64 `json:"fillRate"`
	// Supply — capacity / Công suất
	StorageM3              float64 `json:"storageM3"`
	ColdStorageL           int32   `json:"coldStorageL"`
	ShelfSlots             int32   `json:"shelfSlots"`
	ReceivingSlotsPerDay   int32   `json:"receivingSlotsPerDay"`
	ReceivingCount         int64   `json:"receivingCount"`
	OverCapacitySkuCount   int64   `json:"overCapacitySkuCount"`
	StorageUtilization     float64 `json:"storageUtilization"`
	ReceivingUtilization   float64 `json:"receivingUtilization"`
	// Supply — carrier / Nhà vận chuyển
	ShipmentCount            int64   `json:"shipmentCount"`
	StopCount                int64   `json:"stopCount"`
	DeliveredStopCount       int64   `json:"deliveredStopCount"`
	OnTimeStopCount          int64   `json:"onTimeStopCount"`
	OnTimeInFullStopCount    int64   `json:"onTimeInFullStopCount"`
	OnTimeInFullRate         float64 `json:"onTimeInFullRate"`
	AverageDelayMinutes      float64 `json:"averageDelayMinutes"`
	TransportCost            float64 `json:"transportCost"`
	TotalWeightKg            float64 `json:"totalWeightKg"`
	TotalVolumeM3            float64 `json:"totalVolumeM3"`
	CostPerKg                float64 `json:"costPerKg"`
	CostPerStop              float64 `json:"costPerStop"`
	AverageWeightLoadFactor  float64 `json:"averageWeightLoadFactor"`
	AverageVolumeLoadFactor  float64 `json:"averageVolumeLoadFactor"`
	// Supply — supplier / Nhà cung cấp
	PurchaseLineCount            int64   `json:"purchaseLineCount"`
	ReceivedLineCount            int64   `json:"receivedLineCount"`
	OnTimeLineCount              int64   `json:"onTimeLineCount"`
	OnTimeRate                   float64 `json:"onTimeRate"`
	AverageLeadTimeDays          float64 `json:"averageLeadTimeDays"`
	AveragePromisedLeadTimeDays  float64 `json:"averagePromisedLeadTimeDays"`
	LeadTimeVarianceDays         float64 `json:"leadTimeVarianceDays"`
	AverageFillRate              float64 `json:"averageFillRate"`
	OrderedQuantity              int64   `json:"orderedQuantity"`
	ReceivedQuantity             int64   `json:"receivedQuantity"`
	OrderedAmount                float64 `json:"orderedAmount"`
	ReceivedAmount               float64 `json:"receivedAmount"`
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

func normalizeKdView(view string) string {
	switch view {
	default:
		return KdViewOrder
	}
}

func normalizeSupplyView(view string) string {
	switch view {
	case SupplyViewCapacity, SupplyViewCarrier, SupplyViewSupplier:
		return view
	default:
		return SupplyViewInventory
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

func (r *ReportRepository) GetKdReport(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
	filter KdFilter,
	page int,
	pageSize int,
) ([]ReportRow, int64, error) {
	view = normalizeKdView(view)
	spec := kdViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendKdFilters(filter, where, args)

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
		row, err := scanKdRow(rows)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}
	return result, int64(total), nil
}

func (r *ReportRepository) GetKdSummary(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
	filter KdFilter,
) (ReportSummary, error) {
	view = normalizeKdView(view)
	spec := kdViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendKdFilters(filter, where, args)

	query := fmt.Sprintf(`
		SELECT
			sum(`+"`DoanhThuThuan`"+`), sum(`+"`TongThu`"+`), sum(`+"`TongChi`"+`), sum(`+"`DongTienRong`"+`),
			sum(`+"`ChenhLechDoiSoat`"+`), countIf(`+"`ChoDonHang`"+` = 1)
		FROM %s
		WHERE %s`, spec.table, where)

	var summary ReportSummary
	var pendingCount uint64
	err := r.conn.QueryRow(queryCtx, query, args...).Scan(
		&summary.TotalNetRevenue,
		&summary.TotalReceipts,
		&summary.TotalExpenditures,
		&summary.TotalNetCashFlow,
		&summary.ReconciliationDiff,
		&pendingCount,
	)
	summary.TotalSalesQuantity = int64(pendingCount)
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

func scanKdRow(rows driver.Rows) (ReportRow, error) {
	var row ReportRow
	// ClickHouse UInt8 for PendingOrder / ChoDonHang là UInt8 trên ClickHouse
	var pending uint8
	err := rows.Scan(
		&row.ReportDate, &row.OrderId, &row.ReturnId, &row.DocumentType,
		&row.ShopCode, &row.ShopName, &row.OrderStatus,
		&row.GrossRevenue, &row.ReturnAmount, &row.NetRevenue,
		&row.TotalReceipts, &row.TotalExpenditures, &row.NetCashFlow, &row.PaymentMethodSummary,
		&row.TotalOrderAmount, &row.TotalReturnAmount, &row.ReconciliationDiff, &pending,
	)
	row.PendingOrder = int32(pending)
	return row, err
}

func (r *ReportRepository) GetSupplyReport(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
	filter SupplyFilter,
	page int,
	pageSize int,
) ([]ReportRow, int64, error) {
	view = normalizeSupplyView(view)
	spec := supplyViewSpecFor(view)

	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}
	where, args = appendSupplyFilters(view, filter, where, args)

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
		row, err := scanSupplyRow(view, rows)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}
	return result, int64(total), nil
}

func scanSupplyRow(view string, rows driver.Rows) (ReportRow, error) {
	var row ReportRow
	var err error

	switch view {
	case SupplyViewCapacity:
		var onHandValue int64
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.OnHandVolumeM3, &onHandValue,
			&row.StorageM3, &row.ColdStorageL, &row.ShelfSlots, &row.ReceivingSlotsPerDay,
			&row.ReceivingCount, &row.OverCapacitySkuCount, &row.StorageUtilization, &row.ReceivingUtilization,
		)
		row.OnHandValue = float64(onHandValue)
	case SupplyViewCarrier:
		var transportCost int64
		err = rows.Scan(
			&row.ReportDate, &row.CarrierId, &row.CarrierName, &row.ShipmentCount, &row.StopCount,
			&row.DeliveredStopCount, &row.OnTimeStopCount, &row.OnTimeInFullStopCount, &row.OnTimeInFullRate,
			&row.AverageDelayMinutes, &transportCost, &row.TotalWeightKg, &row.TotalVolumeM3,
			&row.CostPerKg, &row.CostPerStop, &row.AverageWeightLoadFactor, &row.AverageVolumeLoadFactor,
		)
		row.TransportCost = float64(transportCost)
	case SupplyViewSupplier:
		var orderedAmount, receivedAmount int64
		err = rows.Scan(
			&row.ReportDate, &row.SupplierId, &row.SupplierName, &row.PurchaseLineCount, &row.ReceivedLineCount,
			&row.OnTimeLineCount, &row.OnTimeRate, &row.AverageLeadTimeDays, &row.AveragePromisedLeadTimeDays,
			&row.LeadTimeVarianceDays, &row.AverageFillRate, &row.OrderedQuantity, &row.ReceivedQuantity,
			&orderedAmount, &receivedAmount,
		)
		row.OrderedAmount = float64(orderedAmount)
		row.ReceivedAmount = float64(receivedAmount)
	default:
		var onHandValue, lostRevenue int64
		err = rows.Scan(
			&row.ReportDate, &row.ShopCode, &row.ShopName, &row.ProductLineCode, &row.ProductLineName,
			&row.OnHandQuantity, &onHandValue, &row.OnHandVolumeM3, &row.ExpiredQuantity,
			&row.SkuCount, &row.StockoutSkuCount, &row.StockoutRate, &row.AverageDaysOfSupply,
			&row.RequestedQuantity, &row.LostQuantity, &lostRevenue, &row.FillRate,
		)
		row.OnHandValue = float64(onHandValue)
		row.LostRevenueEstimate = float64(lostRevenue)
	}
	return row, err
}
