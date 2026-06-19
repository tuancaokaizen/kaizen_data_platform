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
