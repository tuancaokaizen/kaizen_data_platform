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
	ShopCode          string
	PaymentMethodName string
	Page              int
	PageSize          int
}

func (s *ReportService) GetFinancialReport(
	ctx context.Context,
	query ReportQuery,
) ([]repository.FinancialRow, int64, error) {
	return s.repo.GetFinancialReport(ctx, query.FromDate, query.ToDate, query.ShopCode, query.Page, query.PageSize)
}

func (s *ReportService) GetFinancialSummary(
	ctx context.Context,
	query ReportQuery,
) (repository.FinancialSummary, error) {
	return s.repo.GetFinancialSummary(ctx, query.FromDate, query.ToDate, query.ShopCode)
}

func (s *ReportService) GetFundReport(
	ctx context.Context,
	query ReportQuery,
) ([]repository.FundRow, int64, error) {
	return s.repo.GetFundReport(ctx, query.FromDate, query.ToDate, query.ShopCode, query.PaymentMethodName, query.Page, query.PageSize)
}

func (s *ReportService) GetFundSummary(
	ctx context.Context,
	query ReportQuery,
) (repository.FundSummary, error) {
	return s.repo.GetFundSummary(ctx, query.FromDate, query.ToDate, query.ShopCode, query.PaymentMethodName)
}
