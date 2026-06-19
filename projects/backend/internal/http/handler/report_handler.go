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
	query, err := buildReportQuery(r)
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
		"view": query.View,
		"pagination": map[string]any{
			"page":     query.Page,
			"pageSize": query.PageSize,
			"total":    total,
		},
	})
}

func (h *ReportHandler) GetFinancialSummary(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, err := h.reportService.GetFinancialSummary(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{"data": summary, "view": query.View})
}

func (h *ReportHandler) GetFundReport(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r)
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
		"view": query.View,
		"pagination": map[string]any{
			"page":     query.Page,
			"pageSize": query.PageSize,
			"total":    total,
		},
	})
}

func (h *ReportHandler) GetFundSummary(w http.ResponseWriter, r *http.Request) {
	query, err := buildReportQuery(r)
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	summary, err := h.reportService.GetFundSummary(r.Context(), query)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, map[string]any{"data": summary, "view": query.View})
}

func buildReportQuery(r *http.Request) (service.ReportQuery, error) {
	// Parse and validate required date range / Parse và validate khoảng ngày bắt buộc
	fromDate, err := time.Parse(dateLayout, r.URL.Query().Get("fromDate"))
	if err != nil {
		return service.ReportQuery{}, err
	}
	toDate, err := time.Parse(dateLayout, r.URL.Query().Get("toDate"))
	if err != nil {
		return service.ReportQuery{}, err
	}

	page := parseIntOrDefault(r.URL.Query().Get("page"), 1)
	pageSize := parseIntOrDefault(r.URL.Query().Get("pageSize"), 50)
	if page <= 0 {
		page = 1
	}
	// pageSize=0 means fetch all rows / pageSize=0 nghĩa là lấy toàn bộ dòng
	if pageSize != 0 && (pageSize < 0 || pageSize > 500) {
		pageSize = 50
	}

	productId := r.URL.Query().Get("productId")
	if productId == "" {
		// Backward compatible alias / Alias tương thích cũ
		productId = r.URL.Query().Get("productCode")
	}

	view := r.URL.Query().Get("view")
	if view == "" {
		view = defaultViewForPath(r.URL.Path)
	}

	return service.ReportQuery{
		FromDate:          fromDate,
		ToDate:            toDate,
		View:              view,
		ShopCode:          r.URL.Query().Get("shopCode"),
		ProductId:         productId,
		ProductLineCode:   r.URL.Query().Get("productLineCode"),
		OrderId:           r.URL.Query().Get("orderId"),
		DocumentType:      r.URL.Query().Get("documentType"),
		OrderStatus:       r.URL.Query().Get("orderStatus"),
		PaymentMethodName: r.URL.Query().Get("paymentMethodName"),
		Page:              page,
		PageSize:          pageSize,
	}, nil
}

func defaultViewForPath(path string) string {
	if path == "/api/v1/reports/fund" || path == "/api/v1/reports/fund/summary" {
		return "payment"
	}
	return "shop"
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
