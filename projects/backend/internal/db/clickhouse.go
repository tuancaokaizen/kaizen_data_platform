package db

import (
	"context"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/kaizen/backend-report-api/internal/config"
)

func NewClickHouseConn(cfg config.Config) (clickhouse.Conn, error) {
	// Port 8123 = HTTP interface; 9000 = native TCP / 8123=HTTP, 9000=native
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.CHHost, cfg.CHPort)},
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: cfg.CHUser,
			Password: cfg.CHPassword,
		},
	}
	if cfg.CHPort == 8123 {
		opts.Protocol = clickhouse.HTTP
	} else {
		opts.Protocol = clickhouse.Native
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	// Fail fast on bad protocol/host / Báo lỗi sớm nếu sai protocol/host
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping failed: %w", err)
	}
	return conn, nil
}
