# Backend Report API (Go)

Đọc báo cáo từ ClickHouse `report.*` (view hot/cold tiếng Việt).

**Chạy full stack:** `cd .. && ./run_local.sh`

## Local only

```bash
cp .env.example .env
./deploy_backend.sh build
./deploy_backend.sh run    # :8090
```

Cần ClickHouse K3s reachable qua `kubectl port-forward -n databases svc/clickhouse-service 9000:9000`.

## API

- `GET /healthz`
- `GET /api/v1/reports/financial?fromDate=&toDate=&page=&pageSize=&shopCode=`
- `GET /api/v1/reports/fund?fromDate=&toDate=&page=&pageSize=&shopCode=`

## Env (`.env.example`)

| Var | Mặc định local |
|-----|----------------|
| `API_PORT` | 8090 |
| `CLICKHOUSE_HOST` | localhost |
| `CLICKHOUSE_PORT` | 9000 |
