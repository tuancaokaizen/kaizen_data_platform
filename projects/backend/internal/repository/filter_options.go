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

// SegmentOption — RFM segment dropdown / Dropdown phân khúc RFM
type SegmentOption struct {
	SegmentCode string `json:"segmentCode"`
	SegmentName string `json:"segmentName"`
}

// ClusterOption — KMeans cluster dropdown / Dropdown nhóm (cụm) khách hàng
type ClusterOption struct {
	ClusterCode string `json:"clusterCode"`
	ClusterName string `json:"clusterName"`
}

// AnalyticsFilterOptions — distinct segments/clusters for analytics views
// Giá trị distinct phân khúc / nhóm cho báo cáo analytics
type AnalyticsFilterOptions struct {
	Segments []SegmentOption `json:"segments"`
	Clusters []ClusterOption `json:"clusters"`
}

// SupplyFilterOptions — distinct shops/products for supply views
// Giá trị distinct cửa hàng / sản phẩm cho báo cáo chuỗi cung ứng
type SupplyFilterOptions struct {
	Shops    []ShopOption    `json:"shops"`
	Products []ProductOption `json:"products"`
}

// GetAnalyticsFilterOptions lists distinct segments (and clusters for customer view)
// Lấy phân khúc distinct (và nhóm cụm khi view=customer)
func (r *ReportRepository) GetAnalyticsFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
) (AnalyticsFilterOptions, error) {
	view = normalizeAnalyticsView(view)
	spec := analyticsViewSpecFor(view)
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	dateCol := dateColumnForSpec(view, "analytics")
	where := fmt.Sprintf("%s >= ? AND %s <= ?", dateCol, dateCol)
	args := []any{fromDate, toDate}

	segmentSQL := fmt.Sprintf(
		`SELECT %s, any(%s) FROM %s WHERE %s AND %s != '' GROUP BY %s ORDER BY %s`,
		"`MaPhanKhuc`", "`TenPhanKhuc`", spec.table, where, "`MaPhanKhuc`", "`MaPhanKhuc`", "`MaPhanKhuc`",
	)
	segmentRows, err := r.conn.Query(queryCtx, segmentSQL, args...)
	if err != nil {
		return AnalyticsFilterOptions{}, err
	}
	defer segmentRows.Close()

	segments := make([]SegmentOption, 0, 16)
	for segmentRows.Next() {
		var opt SegmentOption
		if err := segmentRows.Scan(&opt.SegmentCode, &opt.SegmentName); err != nil {
			return AnalyticsFilterOptions{}, err
		}
		if opt.SegmentCode != "" {
			segments = append(segments, opt)
		}
	}
	if err := segmentRows.Err(); err != nil {
		return AnalyticsFilterOptions{}, err
	}

	clusters := make([]ClusterOption, 0)
	if view == AnalyticsViewCustomer {
		clusterSQL := fmt.Sprintf(
			`SELECT %s, any(%s) FROM %s WHERE %s AND %s != '' GROUP BY %s ORDER BY %s`,
			"`MaCumMoHinh`", "`TenCum`", spec.table, where, "`MaCumMoHinh`", "`MaCumMoHinh`", "`MaCumMoHinh`",
		)
		clusterRows, err := r.conn.Query(queryCtx, clusterSQL, args...)
		if err != nil {
			return AnalyticsFilterOptions{}, err
		}
		defer clusterRows.Close()
		for clusterRows.Next() {
			var opt ClusterOption
			if err := clusterRows.Scan(&opt.ClusterCode, &opt.ClusterName); err != nil {
				return AnalyticsFilterOptions{}, err
			}
			if opt.ClusterCode != "" {
				clusters = append(clusters, opt)
			}
		}
		if err := clusterRows.Err(); err != nil {
			return AnalyticsFilterOptions{}, err
		}
	}

	return AnalyticsFilterOptions{Segments: segments, Clusters: clusters}, nil
}

// GetSupplyFilterOptions lists distinct shops/products for a supply view in date range
// Lấy cửa hàng / sản phẩm distinct theo view chuỗi cung ứng trong khoảng ngày
func (r *ReportRepository) GetSupplyFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
	view string,
) (SupplyFilterOptions, error) {
	view = normalizeSupplyView(view)
	spec := supplyViewSpecFor(view)
	queryCtx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()

	dateCol := dateColumnForSpec(view, "supply")
	where := fmt.Sprintf("%s >= ? AND %s <= ?", dateCol, dateCol)
	args := []any{fromDate, toDate}

	shops := make([]ShopOption, 0)
	if view == SupplyViewInventory || view == SupplyViewCapacity || view == SupplyViewSolver {
		shopSQL := fmt.Sprintf(
			`SELECT %s, any(%s) FROM %s WHERE %s AND %s != '' GROUP BY %s ORDER BY %s`,
			"`MaCuaHang`", "`TenCuaHang`", spec.table, where, "`MaCuaHang`", "`MaCuaHang`", "`MaCuaHang`",
		)
		shopRows, err := r.conn.Query(queryCtx, shopSQL, args...)
		if err != nil {
			return SupplyFilterOptions{}, err
		}
		defer shopRows.Close()
		for shopRows.Next() {
			var opt ShopOption
			if err := shopRows.Scan(&opt.ShopCode, &opt.ShopName); err != nil {
				return SupplyFilterOptions{}, err
			}
			if opt.ShopCode != "" {
				shops = append(shops, opt)
			}
		}
		if err := shopRows.Err(); err != nil {
			return SupplyFilterOptions{}, err
		}
	}

	products := make([]ProductOption, 0)
	if view == SupplyViewSolver {
		productSQL := fmt.Sprintf(
			`SELECT %s, any(%s) FROM %s WHERE %s AND %s != '' GROUP BY %s ORDER BY %s`,
			"`MaSanPham`", "`TenSanPham`", spec.table, where, "`MaSanPham`", "`MaSanPham`", "`MaSanPham`",
		)
		productRows, err := r.conn.Query(queryCtx, productSQL, args...)
		if err != nil {
			return SupplyFilterOptions{}, err
		}
		defer productRows.Close()
		for productRows.Next() {
			var opt ProductOption
			if err := productRows.Scan(&opt.ProductId, &opt.ProductName); err != nil {
				return SupplyFilterOptions{}, err
			}
			if opt.ProductId != "" {
				products = append(products, opt)
			}
		}
		if err := productRows.Err(); err != nil {
			return SupplyFilterOptions{}, err
		}
	}

	return SupplyFilterOptions{Shops: shops, Products: products}, nil
}
