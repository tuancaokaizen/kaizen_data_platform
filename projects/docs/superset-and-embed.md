# Superset deploy & nhúng BI (FE + BE)

Hướng dẫn ngắn cho data platform: deploy dashboard Superset từ YAML và nhúng tab **Phân tích BI** trong React.

---

## 1. Kiến trúc (local dev)

```
Browser :8092  (React — tab Phân tích BI)
    │
    ├─ GET  /api/v1/reports/*     → Go backend :8090 → ClickHouse
    ├─ GET  /api/v1/bi/embed-config
    ├─ POST /api/v1/bi/guest-token
    │
    └─ iframe /embedded/{uuid}    → Vite proxy → Superset :32288 (K3s)
```

- **Parent frame origin** (vd. `http://localhost:8092`, `http://100.x.x.x:8092`) phải nằm trong `allowed_domains` của Superset.
- **Guest token** do Go backend mint qua Superset API — frontend không giữ password admin.

---

## 2. Superset — deploy dashboard

### Setup một lần

```bash
cd projects/superset
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.example config/.env   # hoặc .env.tailscale.example
```

### Lệnh thường dùng

| Việc | Lệnh |
|------|------|
| **⭐ Deploy + nhúng BI (một lệnh)** | `./publish_superset.sh --tailscale --force --embed` |
| Kiểm tra YAML | `.venv/bin/python3 -m superset_deploy validate` |
| Đăng ký dataset ClickHouse | `.venv/bin/python3 -m superset_deploy bootstrap` |
| Tạo/cập nhật dashboard | `./deploy_superset.sh --force` hoặc publish ở trên |
| Một dashboard | `... deploy --force --dashboard tc-tong-hop-cua-hang` |
| Backup + MinIO | `./publish_superset.sh --force --embed --minio` |
| Import MinIO → cluster khác | `./publish_superset.sh --import-minio --embed` |

Chi tiết: **[publish-superset.md](publish-superset.md)**

Các lệnh CLI riêng lẻ (khi không dùng publish):

### File quan trọng

| File | Vai trò |
|------|---------|
| `config/dashboard_registry.yaml` | **SSOT** — slug, title, `report_key`, `chart_label` |
| `config/dashboards/*.yaml` | Layout chart + filter từng dashboard |
| `config/datasets.yaml` | View ClickHouse (`report.v_bao_cao_*`) |
| `src/superset_deploy/naming.py` | Tên chart UI (tiếng Việt) vs key nội bộ |

**Tên chart trên UI** = field `title:` trong YAML (vd. `Doanh thu thuần`). Key kỹ thuật `slug/chart_key` chỉ dùng để upsert, không hiển thị.

### 7 dashboard ↔ menu báo cáo

| Slug | Menu React (`report_key`) |
|------|---------------------------|
| `tc-tong-hop-cua-hang` | `financial:shop` |
| `tc-chi-tiet-san-pham-cua-hang` | `financial:detail` |
| `tc-tong-hop-san-pham` | `financial:product` |
| `tc-chi-tiet-don-hang` | `financial:order` |
| `quy-pttt-cua-hang` | `fund:payment` |
| `quy-tong-hop-cua-hang` | `fund:shop` |
| `quy-chi-tiet-don-hang` | `fund:order` |

### Thêm dashboard mới

1. Thêm entry `config/dashboard_registry.yaml`
2. Tạo `config/dashboards/<slug>.yaml`
3. `export-registry` → `deploy --force` → `enable-embed` → `save-query-context`
4. Rebuild Go backend (đọc `dashboards.embed.json`)

---

## 3. Superset — bật embed (cluster)

### Một lần trên K3s

```bash
kubectl apply -f k8s/superset-embed-configmap.yaml
./scripts/enable_superset_embed_k8s.sh    # mount superset_config.py, EMBEDDED_SUPERSET=True
./scripts/apply_superset_pvc_k8s.sh       # PVC giữ SQLite — tránh mất ID khi restart pod
```

### Sau mỗi deploy dashboard

```bash
./scripts/grant_guest_embed_permissions.sh   # Gamma: dataset + dashboard access
.venv/bin/python3 -m superset_deploy enable-embed \
  --domain http://localhost:8092 \
  --domain http://100.x.x.x:8092              # Tailscale / LAN IP mở UI
.venv/bin/python3 -m superset_deploy save-query-context
```

`enable-embed` ghi file:

`projects/backend/internal/superset/dashboards.embed.json`

→ **Rebuild & restart** Go backend sau khi file đổi.

---

## 4. Backend (Go) — guest token

### Env (`backend/.env`)

```env
SUPERSET_HOST=http://localhost:32288          # Superset API (port-forward K3s)
SUPERSET_EMBED_DOMAIN=http://localhost:8092     # Gợi ý origin UI (enable-embed cũng dùng)
SUPERSET_USERNAME=admin
SUPERSET_PASSWORD=admin
# SUPERSET_EMBED_EXTRA_DOMAINS=http://100.x.x.x:8092
```

### API

| Endpoint | Mô tả |
|----------|--------|
| `GET /api/v1/bi/embed-config` | `enabled`, `supersetDomain`, danh sách `{ slug, embeddedUuid }` |
| `POST /api/v1/bi/guest-token` | Body `{ "slug": "tc-tong-hop-cua-hang" }` → `{ token, embeddedUuid }` |

### Code chính

| File | Vai trò |
|------|---------|
| `internal/superset/client.go` | Login Superset + mint guest token (cookie jar CSRF) |
| `internal/superset/dashboards.go` | Load `dashboards.embed.json` (embed UUID + dashboardId) |
| `internal/service/bi_service.go` | `GetEmbedConfig`, `GuestToken` |
| `internal/http/handler/bi_handler.go` | HTTP handlers |

---

## 5. Frontend (React) — tab Phân tích BI

### Luồng

1. User chọn báo cáo → tab **Phân tích BI**
2. `supersetSlugForReportKey()` lấy slug từ `biDashboardRegistry.ts` (auto-gen từ registry YAML)
3. `useSupersetEmbed` → `embed-config` → `guest-token` → `@superset-ui/embedded-sdk`
4. iframe: `{origin}/embedded/{uuid}?guest_token=...` — Vite proxy sang Superset

### Code chính

| File | Vai trò |
|------|---------|
| `src/config/biDashboardRegistry.ts` | Map `report_key` → slug (generated) |
| `src/config/reportViews.ts` | Menu báo cáo + `supersetSlug` |
| `src/hooks/useSupersetEmbed.ts` | Logic embed SDK |
| `src/components/SupersetEmbedTab.tsx` | UI tab BI |
| `vite.config.ts` | Proxy `/embedded`, `/static`, `/api/v1` → Superset :32288 |

### Vite proxy (bắt buộc local)

Frontend và iframe **cùng origin** (`:8092`). Vite forward:

- `/api/v1/reports`, `/api/v1/bi` → Go `:8090`
- `/embedded`, `/static`, `/api/v1` (Superset) → `:32288`

---

## 6. Chạy local — một lệnh

```bash
cd projects && ./run_local.sh
```

Tự động (có kubectl):

1. Port-forward ClickHouse + Superset
2. PVC Superset (nếu chưa có)
3. Grant guest permissions
4. `enable-embed` + detect IP (LAN, Tailscale, ifconfig)
5. `save-query-context`
6. Backend + Frontend

UI: http://localhost:8092 — hoặc IP mạng/Tailscale in trong log Vite.

> Dùng `.venv/bin/python3` cho `superset_deploy` (không dùng system `python3` 3.14).

---

## 7. Checklist sau restart pod Superset

| Bước | Cần khi |
|------|---------|
| PVC đã mount | Luôn có — giữ DB, tránh reset ID |
| `grant_guest_embed_permissions.sh` | Sau `sync_role_definitions` / pod mới |
| `enable-embed --domain <origin-ui>` | Origin UI đổi (IP Tailscale mới) |
| `save-query-context` | Pod mới / deploy chart mới |
| Rebuild backend | `dashboards.embed.json` đổi (DB wipe không PVC) |

**Có PVC:** restart pod → ID/UUID giữ nguyên, thường chỉ cần grant lại.

**Không PVC / DB mới:** chạy full `deploy --force` + `enable-embed` + rebuild backend.

---

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Fix |
|-------------|-------------|-----|
| **403** qua IP Tailscale/LAN | `allowed_domains` thiếu origin | `enable-embed --domain http://<ip>:8092` |
| **403** localhost OK | Gamma thiếu quyền | `grant_guest_embed_permissions.sh` |
| **403** guest token OK | UUID backend ≠ Superset | `enable-embed` → rebuild backend |
| Blank / broken iframe | Thiếu proxy `/static`, `/embedded` | Kiểm tra `vite.config.ts` |
| Filter "guest cannot modify payload" | Chart thiếu `query_context` | `save-query-context` |
| Tên chart dạng `slug/key` | Chưa deploy naming mới | `deploy --force` |
| `No module named superset_deploy` | Sai Python | Dùng `superset/.venv/bin/python3` |

---

## 9. Tham chiếu nhanh

```bash
# Khuyến nghị — một lệnh sau sửa YAML dashboard
cd projects/superset
./publish_superset.sh --tailscale --force --embed
cd .. && ./run_local.sh
```

```bash
# + backup MinIO (GitOps)
./publish_superset.sh --tailscale --force --embed --minio

# Cluster khác — import từ MinIO
./publish_superset.sh --tailscale --import-minio --embed --domain https://reports.example.com
```

Chi tiết: [publish-superset.md](publish-superset.md)

```bash
# Chỉ sửa mapping slug ↔ menu
.venv/bin/python3 -m superset_deploy export-registry
```
