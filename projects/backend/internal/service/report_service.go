package service

import (
	"context"
	"time"

	"github.com/kaizen/backend-report-api/internal/repository"
)

type ReportService struct {
	repo *repository.ReportRepository
}

func NewReportService(repo *repository.ReportRepository) *ReportService {
	return &ReportService{repo: repo}
}

type ReportQuery struct {
	FromDate          time.Time
	ToDate            time.Time
	View              string
	ShopCode          string
	ProductId         string
	ProductLineCode   string
	OrderId           string
	DocumentType      string
	OrderStatus       string
	PaymentMethodName string
	Page              int
	PageSize          int
}

func (s *ReportService) GetFinancialReport(
	ctx context.Context,
	query ReportQuery,
) ([]repository.ReportRow, int64, error) {
	filter := repository.FinancialFilter{
		ShopCode:        query.ShopCode,
		ProductId:       query.ProductId,
		ProductLineCode: query.ProductLineCode,
		OrderId:         query.OrderId,
		DocumentType:    query.DocumentType,
		OrderStatus:     query.OrderStatus,
	}
	return s.repo.GetFinancialReport(
		ctx, query.FromDate, query.ToDate, query.View, filter, query.Page, query.PageSize,
	)
}

func (s *ReportService) GetFinancialSummary(
	ctx context.Context,
	query ReportQuery,
) (repository.ReportSummary, error) {
	filter := repository.FinancialFilter{
		ShopCode:        query.ShopCode,
		ProductId:       query.ProductId,
		ProductLineCode: query.ProductLineCode,
		OrderId:         query.OrderId,
		DocumentType:    query.DocumentType,
		OrderStatus:     query.OrderStatus,
	}
	return s.repo.GetFinancialSummary(ctx, query.FromDate, query.ToDate, query.View, filter)
}

func (s *ReportService) GetFundReport(
	ctx context.Context,
	query ReportQuery,
) ([]repository.ReportRow, int64, error) {
	filter := repository.FundFilter{
		ShopCode:          query.ShopCode,
		PaymentMethodName: query.PaymentMethodName,
		OrderId:           query.OrderId,
	}
	return s.repo.GetFundReport(
		ctx, query.FromDate, query.ToDate, query.View, filter, query.Page, query.PageSize,
	)
}

func (s *ReportService) GetFundSummary(
	ctx context.Context,
	query ReportQuery,
) (repository.ReportSummary, error) {
	filter := repository.FundFilter{
		ShopCode:          query.ShopCode,
		PaymentMethodName: query.PaymentMethodName,
		OrderId:           query.OrderId,
	}
	return s.repo.GetFundSummary(ctx, query.FromDate, query.ToDate, query.View, filter)
}

func (s *ReportService) GetKdReport(
	ctx context.Context,
	query ReportQuery,
) ([]repository.ReportRow, int64, error) {
	filter := repository.KdFilter{
		ShopCode:     query.ShopCode,
		OrderId:      query.OrderId,
		DocumentType: query.DocumentType,
		OrderStatus:  query.OrderStatus,
	}
	return s.repo.GetKdReport(
		ctx, query.FromDate, query.ToDate, query.View, filter, query.Page, query.PageSize,
	)
}

func (s *ReportService) GetKdSummary(
	ctx context.Context,
	query ReportQuery,
) (repository.ReportSummary, error) {
	filter := repository.KdFilter{
		ShopCode:     query.ShopCode,
		OrderId:      query.OrderId,
		DocumentType: query.DocumentType,
		OrderStatus:  query.OrderStatus,
	}
	return s.repo.GetKdSummary(ctx, query.FromDate, query.ToDate, query.View, filter)
}

// GetFinancialDetailFilterOptions returns distinct shop/product values for detail view filters
// Trả về giá trị distinct cửa hàng/sản phẩm cho bộ lọc view chi tiết
func (s *ReportService) GetFinancialDetailFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
) (repository.FinancialDetailFilterOptions, error) {
	return s.repo.GetFinancialDetailFilterOptions(ctx, fromDate, toDate)
}

func (s *ReportService) GetKdFilterOptions(
	ctx context.Context,
	fromDate time.Time,
	toDate time.Time,
) (repository.KdFilterOptions, error) {
	return s.repo.GetKdFilterOptions(ctx, fromDate, toDate)
}
