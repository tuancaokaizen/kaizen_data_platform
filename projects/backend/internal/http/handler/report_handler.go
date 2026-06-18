package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/kaizen/backend-report-api/internal/http/response"
	"github.com/kaizen/backend-report-api/internal/service"
)

const dateLayout = "2006-01-02"

type ReportHandler struct {
	reportService *service.ReportService
}

func NewReportHandler(reportService *service.ReportService) *ReportHandler {
	return &ReportHandler{reportService: reportService}
}

func (h *ReportHandler) Healthz(w http.ResponseWriter, _ *http.Request) {
	response.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *ReportHandler) GetFinancialReport(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r, false)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	data, total, err := h.reportService.GetFinancialReport(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"data": data,
		"pagination": map[string]any{
			"page":     query.Page,
			"pageSize": query.PageSize,
			"total":    total,
		},
	})
}

func (h *ReportHandler) GetFinancialSummary(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r, false)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, err := h.reportService.GetFinancialSummary(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{"data": summary})
}

func (h *ReportHandler) GetFundReport(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r, true)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	data, total, err := h.reportService.GetFundReport(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{
		"data": data,
		"pagination": map[string]any{
			"page":     query.Page,
			"pageSize": query.PageSize,
			"total":    total,
		},
	})
}

func (h *ReportHandler) GetFundSummary(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r, true)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, err := h.reportService.GetFundSummary(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{"data": summary})
}

func buildReportQuery(r *http.Request, includePaymentMethod bool) (service.ReportQuery, error) {
	// Parse and validate required date range / Parse và validate khoảng ngày bắt buộc
	fromDate, err := time.Parse(dateLayout, r.URL.Query().Get("fromDate"))
	if err != nil {
		return service.ReportQuery{}, err
	}
	toDate, err := time.Parse(dateLayout, r.URL.Query().Get("toDate"))
	if err != nil {
		return service.ReportQuery{}, err
	}

	// Apply safe defaults for pagination / Áp dụng mặc định an toàn cho phân trang
	page := parseIntOrDefault(r.URL.Query().Get("page"), 1)
	pageSize := parseIntOrDefault(r.URL.Query().Get("pageSize"), 50)
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 50
	}

	query := service.ReportQuery{
		FromDate: fromDate,
		ToDate:   toDate,
		ShopCode: r.URL.Query().Get("shopCode"),
		Page:     page,
		PageSize: pageSize,
	}
	if includePaymentMethod {
		query.PaymentMethodName = r.URL.Query().Get("paymentMethodName")
	}
	return query, nil
}

func parseIntOrDefault(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}
