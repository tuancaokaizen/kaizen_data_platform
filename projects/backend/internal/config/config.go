package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port         string
	CHHost       string
	CHPort       int
	CHDatabase   string
	CHUser       string
	CHPassword   string
	QueryTimeout time.Duration
}

func Load() Config {
	timeoutSeconds := getEnvInt("QUERY_TIMEOUT_SECONDS", 15)
	return Config{
		Port:         getEnv("API_PORT", "8090"),
		CHHost:       getEnv("CLICKHOUSE_HOST", "clickhouse-service.databases.svc.cluster.local"),
		CHPort:       getEnvInt("CLICKHOUSE_PORT", 8123),
		CHDatabase:   getEnv("CLICKHOUSE_DATABASE", "report"),
		CHUser:       getEnv("CLICKHOUSE_USER", "admin"),
		CHPassword:   getEnv("CLICKHOUSE_PASSWORD", "kaizen123"),
		QueryTimeout: time.Duration(timeoutSeconds) * time.Second,
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}
