# Frontend Report Dashboard (React + Vite)

**Chạy full stack:** `cd .. && ./run_local.sh`

## Local only

```bash
cp .env.example .env
./deploy_frontend.sh run    # :8092, proxy /api → backend :8090
```

`VITE_API_BASE_URL` để trống → dùng Vite proxy (không lỗi CORS).

## Scripts

| Lệnh | Mô tả |
|------|--------|
| `./deploy_frontend.sh run` | Dev server |
| `./deploy_frontend.sh build` | Production build → `dist/` |
| `./deploy_frontend.sh clean` | Xóa `node_modules`, `dist`, `.vite` |
