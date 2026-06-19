# Frontend Report Dashboard (React + Vite)

**Chạy full stack:** `cd .. && ./run_local.sh`

## Local only

```bash
cp .env.example .env
./deploy_frontend.sh run    # :8092, proxy /api → backend :8090
```

`VITE_API_BASE_URL` để trống → dùng Vite proxy (không lỗi CORS).

### Tab Phân tích BI (Superset embed)

- Tab **Bảng chi tiết** | **Phân tích BI** trong `App.tsx`
- Slug dashboard: `src/config/biDashboardRegistry.ts` (gen từ `superset/config/dashboard_registry.yaml`)
- Hook embed: `src/hooks/useSupersetEmbed.ts`
- Vite proxy `/embedded`, `/static` → Superset — xem `vite.config.ts`

Hướng dẫn deploy + nhúng: [`../docs/superset-and-embed.md`](../docs/superset-and-embed.md)

## Scripts

| Lệnh | Mô tả |
|------|--------|
| `./deploy_frontend.sh run` | Dev server |
| `./deploy_frontend.sh build` | Production build → `dist/` |
| `./deploy_frontend.sh clean` | Xóa `node_modules`, `dist`, `.vite` |
