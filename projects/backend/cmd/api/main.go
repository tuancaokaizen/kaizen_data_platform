package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kaizen/backend-report-api/internal/config"
	"github.com/kaizen/backend-report-api/internal/db"
	reportHandler "github.com/kaizen/backend-report-api/internal/http/handler"
	"github.com/kaizen/backend-report-api/internal/http/middleware"
	"github.com/kaizen/backend-report-api/internal/repository"
	"github.com/kaizen/backend-report-api/internal/service"
	supersetClient "github.com/kaizen/backend-report-api/internal/superset"
)

func main() {
	// Load runtime settings from environment / Tải cấu hình runtime từ biến môi trường
	cfg := config.Load()

	// Initialize ClickHouse connection / Khởi tạo kết nối ClickHouse
	conn, err := db.NewClickHouseConn(cfg)
	if err != nil {
		log.Fatalf("failed to connect clickhouse: %v", err)
	}
	defer conn.Close()

	// Wire repository -> service -> handler / Gắn chuỗi repository -> service -> handler
	reportRepo := repository.NewReportRepository(conn, cfg.QueryTimeout)
	reportSvc := service.NewReportService(reportRepo)
	reportHTTP := reportHandler.NewReportHandler(reportSvc)

	superset := supersetClient.NewClient(cfg.SupersetHost, cfg.SupersetUser, cfg.SupersetPass)
	biSvc := service.NewBIService(superset, cfg.SupersetEmbedDomain)
	biHTTP := reportHandler.NewBIHandler(biSvc)

	// Register API routes / Đăng ký route API
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", reportHTTP.Healthz)
	mux.HandleFunc("/api/v1/reports/financial", reportHTTP.GetFinancialReport)
	mux.HandleFunc("/api/v1/reports/financial/summary", reportHTTP.GetFinancialSummary)
	mux.HandleFunc("/api/v1/reports/fund", reportHTTP.GetFundReport)
	mux.HandleFunc("/api/v1/reports/fund/summary", reportHTTP.GetFundSummary)
	mux.HandleFunc("/api/v1/bi/embed-config", biHTTP.GetEmbedConfig)
	mux.HandleFunc("/api/v1/bi/guest-token", biHTTP.PostGuestToken)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      middleware.WithCORS(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in background to support graceful shutdown / Chạy server nền để hỗ trợ tắt an toàn
	go func() {
		log.Printf("backend API is running on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// Wait for termination signal / Chờ tín hiệu dừng tiến trình
	stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-stopCtx.Done()

	// Stop accepting new requests and finish in-flight calls / Ngừng nhận request mới và chờ request đang chạy
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
