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

### BI embed (Superset guest token)

- `GET /api/v1/bi/embed-config`
- `POST /api/v1/bi/guest-token` — body `{ "slug": "tc-tong-hop-cua-hang" }`

Chi tiết: [`../docs/superset-and-embed.md`](../docs/superset-and-embed.md)

## Env (`.env.example`)

| Var | Mặc định local |
|-----|----------------|
| `API_PORT` | 8090 |
| `CLICKHOUSE_HOST` | localhost |
| `CLICKHOUSE_PORT` | 9000 |
| `SUPERSET_HOST` | localhost:32288 (khi dùng BI embed) |
| `SUPERSET_EMBED_DOMAIN` | http://localhost:8092 |
