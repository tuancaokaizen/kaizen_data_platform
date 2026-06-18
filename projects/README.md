# Report Dashboard — local dev only (data from K3s ClickHouse)

## Quick start

```bash
export K3S_CONF="$HOME/.kube/config.k3s-new"   # optional if default kubeconfig works

cd projects
./run_local.sh
```

Open **http://localhost:8092** — báo cáo tài chính + báo cáo quỹ.

`run_local.sh` tự làm:
1. Port-forward ClickHouse K3s → `localhost:9000` / `8123`
2. Build + chạy Go API → `:8090`
3. Chạy Vite dev server → `:8092`

Ctrl+C dừng toàn bộ.

## Prerequisites

- `kubectl` + kubeconfig trỏ cluster K3s (data pipeline đã chạy, ClickHouse có view `report.*`)
- Go 1.22+
- Node.js 18+ (`brew install node`)

## Layout

```
projects/
├── run_local.sh          # ← entry point
├── backend/              # Go API → ClickHouse
└── frontend/             # React + Vite + Ant Design
```

## Chạy riêng từng phần

```bash
# Backend only (cần port-forward ClickHouse trước)
kubectl port-forward -n databases svc/clickhouse-service 9000:9000
cd backend && cp .env.example .env && ./deploy_backend.sh run

# Frontend only (cần backend :8090)
cd frontend && cp .env.example .env && ./deploy_frontend.sh run
```

## Ports

| Service    | Port |
|-------------|------|
| Frontend UI | 8092 |
| Backend API | 8090 |
| ClickHouse  | 9000 (native), 8123 (HTTP) |
