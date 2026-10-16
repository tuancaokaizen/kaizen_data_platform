package repository

import (
	"context"
	"fmt"
	"time"
)

// ShopOption is a distinct shop for filter dropdowns / Cửa hàng cho dropdown lọc
type ShopOption struct {
	ShopCode string `json:"shopCode"`
	ShopName string `json:"shopName"`
}

// ProductOption is a distinct product for filter dropdowns / Sản phẩm cho dropdown lọc
type ProductOption struct {
	ProductId   string `json:"productId"`
	ProductName string `json:"productName"`
}

// FinancialDetailFilterOptions holds distinct values for the detail view / Giá trị distinct cho view chi tiết
type FinancialDetailFilterOptions struct {
	Shops    []ShopOption    `json:"shops"`
	Products []ProductOption `json:"products"`
}

// GetFinancialDetailFilterOptions lists distinct shops and products in date range
// Lấy danh sách cửa hàng và sản phẩm distinct trong khoảng ngày (view chi tiết SP×CH)
func (r *ReportRepository) GetFinancialDetailFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
) (FinancialDetailFilterOptions, error) {
	spec := financialViewSpecFor(FinancialViewDetail)
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}

	shopSQL := fmt.Sprintf(
		`SELECT DISTINCT %s, %s FROM %s WHERE %s ORDER BY %s`,
		"`MaCuaHang`", "`TenCuaHang`", spec.table, where, "`MaCuaHang`",
	)
	shopRows, err := r.conn.Query(queryCtx, shopSQL, args...)
	if err != nil {
		return FinancialDetailFilterOptions{}, err
	}
	defer shopRows.Close()

	shops := make([]ShopOption, 0, 32)
	for shopRows.Next() {
		var opt ShopOption
		if err := shopRows.Scan(&opt.ShopCode, &opt.ShopName); err != nil {
			return FinancialDetailFilterOptions{}, err
		}
		if opt.ShopCode != "" {
			shops = append(shops, opt)
		}
	}
	if err := shopRows.Err(); err != nil {
		return FinancialDetailFilterOptions{}, err
	}

	productSQL := fmt.Sprintf(
		`SELECT DISTINCT %s, %s FROM %s WHERE %s ORDER BY %s`,
		"`MaSanPham`", "`TenSanPham`", spec.table, where, "`MaSanPham`",
	)
	productRows, err := r.conn.Query(queryCtx, productSQL, args...)
	if err != nil {
		return FinancialDetailFilterOptions{}, err
	}
	defer productRows.Close()

	products := make([]ProductOption, 0, 64)
	for productRows.Next() {
		var opt ProductOption
		if err := productRows.Scan(&opt.ProductId, &opt.ProductName); err != nil {
			return FinancialDetailFilterOptions{}, err
		}
		if opt.ProductId != "" {
			products = append(products, opt)
		}
	}
	if err := productRows.Err(); err != nil {
		return FinancialDetailFilterOptions{}, err
	}

	return FinancialDetailFilterOptions{Shops: shops, Products: products}, nil
}

// KdFilterOptions holds distinct values for KD realtime filters / Giá trị distinct cho lọc KD realtime
type KdFilterOptions struct {
	Shops         []ShopOption `json:"shops"`
	OrderStatuses []string     `json:"orderStatuses"`
}

// GetKdFilterOptions lists distinct shops and order statuses in date range for KD order view
// Lấy cửa hàng và trạng thái đơn distinct trong khoảng ngày (view KD theo đơn)
func (r *ReportRepository) GetKdFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
) (KdFilterOptions, error) {
	spec := kdViewSpecFor(KdViewOrder)
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	where := "`NgayBaoCao` >= ? AND `NgayBaoCao` <= ?"
	args := []any{fromDate, toDate}

	// Group by shop code — avoids duplicate rows when TenCuaHang varies / Gom theo mã CH
	shopSQL := fmt.Sprintf(
		`SELECT %s, any(%s) FROM %s WHERE %s GROUP BY %s ORDER BY %s`,
		"`MaCuaHang`", "`TenCuaHang`", spec.table, where, "`MaCuaHang`", "`MaCuaHang`",
	)
	shopRows, err := r.conn.Query(queryCtx, shopSQL, args...)
	if err != nil {
		return KdFilterOptions{}, err
	}
	defer shopRows.Close()

	shops := make([]ShopOption, 0, 32)
	for shopRows.Next() {
		var opt ShopOption
		if err := shopRows.Scan(&opt.ShopCode, &opt.ShopName); err != nil {
			return KdFilterOptions{}, err
		}
		if opt.ShopCode != "" {
			shops = append(shops, opt)
		}
	}
	if err := shopRows.Err(); err != nil {
		return KdFilterOptions{}, err
	}

	statusSQL := fmt.Sprintf(
		`SELECT DISTINCT %s FROM %s WHERE %s AND %s != '' ORDER BY %s`,
		"`TrangThaiDon`", spec.table, where, "`TrangThaiDon`", "`TrangThaiDon`",
	)
	statusRows, err := r.conn.Query(queryCtx, statusSQL, args...)
	if err != nil {
		return KdFilterOptions{}, err
	}
	defer statusRows.Close()

	statuses := make([]string, 0, 16)
	for statusRows.Next() {
		var status string
		if err := statusRows.Scan(&status); err != nil {
			return KdFilterOptions{}, err
		}
		if status != "" {
			statuses = append(statuses, status)
		}
	}

	return KdFilterOptions{Shops: shops, OrderStatuses: statuses}, statusRows.Err()
}
