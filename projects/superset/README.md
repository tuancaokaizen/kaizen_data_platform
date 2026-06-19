# Superset — Deploy dashboard từ YAML

Ghi định nghĩa dashboard trong file YAML → script đẩy lên Superset.  
Data lấy từ **ClickHouse view** `report.v_bao_cao_*` (cùng bộ với Go backend / React).

**Superset live:** http://100.104.250.97:32288 (NodePort 32288)

---

## Chạy lệnh nào?

| Bạn muốn… | Lệnh |
|-----------|------|
| **⭐ Sửa YAML + nhúng tab BI (một lệnh)** | `./publish_superset.sh --tailscale --force --embed` |
| **Cài lần đầu** | `./bootstrap.sh --tailscale` rồi `./publish_superset.sh --tailscale --force --embed` |
| **Sửa chart / filter (chỉ Superset)** | `./deploy_superset.sh --force` |
| **Deploy + backup MinIO** | `./publish_superset.sh --tailscale --force --embed --minio` |
| **Import từ MinIO (cluster khác)** | `./publish_superset.sh --tailscale --import-minio --embed` |
| **Backup lên MinIO** | `./sync_minio.sh` |
| **Xem trước, không gọi API** | `./deploy_superset.sh --dry-run` |

Chi tiết `publish_superset.sh`: **[../docs/publish-superset.md](../docs/publish-superset.md)**

> Pipeline chạy thêm data mới → **không cần deploy lại**. Dashboard tự đọc view mới.

---

## Setup (một lần)

```bash
cd projects/superset
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp config/.env.tailscale.example config/.env
# Sửa SUPERSET_HOST nếu IP Tailscale đổi
```

---

## Lần đầu trên cluster

```bash
./bootstrap.sh --tailscale      # đăng ký ClickHouse + 7 dataset trên Superset
./deploy_superset.sh --tailscale   # tạo 7 dashboard + chart
```

---

## Hàng ngày (sau khi sửa YAML)

```bash
# Khuyến nghị — deploy + nhúng BI một lệnh
./publish_superset.sh --tailscale --force --embed

# Chỉ cập nhật dashboard trên Superset (không embed)
./deploy_superset.sh --force
```

Deploy một dashboard:

```bash
python -m superset_deploy deploy --force --dashboard tc-tong-hop-cua-hang
```

---

## File cần biết

```
config/
  datasets.yaml          ← danh sách view ClickHouse (sửa → chạy bootstrap)
  defaults.yaml          ← chiều cao chart, filter mặc định
  dashboards/*.yaml      ← ★ mỗi file = 1 dashboard (sửa thường xuyên nhất)
  filters/common.yaml    ← filter dùng chung (ngay, cua_hang, …)
  .env                   ← URL Superset, MinIO, ClickHouse

src/superset_deploy/     ← code deploy (CLI)
```

**Sửa dashboard:** mở `config/dashboards/<slug>.yaml` → `./deploy_superset.sh --force`

**Thêm view mới:** thêm vào `datasets.yaml` → `./bootstrap.sh` → tạo file YAML mới trong `dashboards/` → deploy `--force`

---

## 7 dashboard

| Slug | Báo cáo |
|------|---------|
| `tc-tong-hop-cua-hang` | Tổng hợp theo cửa hàng |
| `tc-chi-tiet-san-pham-cua-hang` | Chi tiết SP × cửa hàng |
| `tc-tong-hop-san-pham` | Tổng hợp sản phẩm |
| `tc-chi-tiet-don-hang` | Chi tiết đơn |
| `quy-pttt-cua-hang` | Quỹ — PTTT × cửa hàng |
| `quy-tong-hop-cua-hang` | Tổng hợp quỹ cửa hàng |
| `quy-chi-tiet-don-hang` | Chi tiết quỹ đơn |

URL: `http://100.104.250.97:32288/superset/dashboard/<slug>/`

---

## CLI (tùy chọn)

Shell script ở trên gọi CLI này — dùng trực tiếp cũng được:

```bash
python -m superset_deploy validate          # kiểm tra YAML
python -m superset_deploy bootstrap         # dataset
python -m superset_deploy deploy --force    # dashboard
python -m superset_deploy enable-embed      # bật nhúng + ghi dashboards.embed.json
python -m superset_deploy save-query-context
python -m superset_deploy export-registry   # → frontend biDashboardRegistry.ts
python -m superset_deploy export            # export ZIP
```

---

## Nhúng dashboard vào React (Embedded SDK)

**Hướng dẫn đầy đủ:** [`../docs/superset-and-embed.md`](../docs/superset-and-embed.md) (Superset + Go + React).

Tóm tắt:

```bash
./scripts/grant_guest_embed_permissions.sh
.venv/bin/python3 -m superset_deploy enable-embed --domain http://localhost:8092
.venv/bin/python3 -m superset_deploy enable-embed --domain http://<tailscale-ip>:8092
.venv/bin/python3 -m superset_deploy save-query-context
# → cập nhật backend/internal/superset/dashboards.embed.json → rebuild Go backend
```

React tab **Phân tích BI**:

- `GET /api/v1/bi/embed-config`
- `POST /api/v1/bi/guest-token` — `{ "slug": "tc-tong-hop-cua-hang" }`

Registry: `config/dashboard_registry.yaml` → `export-registry` → `frontend/src/config/biDashboardRegistry.ts`

Backend `.env`:

```
SUPERSET_HOST=http://localhost:32288
SUPERSET_EMBED_DOMAIN=http://localhost:8092
SUPERSET_USERNAME=admin
SUPERSET_PASSWORD=...
```

**Cluster:** `kubectl apply -f k8s/superset-embed-configmap.yaml` + `scripts/apply_superset_pvc_k8s.sh` (giữ SQLite qua restart).

`run_local.sh` tự chạy grant + enable-embed + save-query-context khi có kubectl.

---

## MinIO

`./sync_minio.sh` export dashboard → upload bucket `superset/assets/`.

Cần `kubectl` + `MINIO_ENDPOINT=http://localhost:31900` trong `.env` — script tự port-forward MinIO (giống batch/airflow).

---

## Local (không Tailscale)

```bash
cp config/.env.example config/.env
./scripts/port_forward.sh    # terminal 1
./bootstrap.sh && ./deploy_superset.sh   # terminal 2
```

---

## Lỗi hay gặp

| Lỗi | Fix |
|-----|-----|
| Không vào được Superset | Check `SUPERSET_HOST` trong `.env` |
| `superset_deploy` not found | `pip install -r requirements.txt` |
| MinIO refused | Dùng `localhost:31900`, không `:9000` trên Tailscale |
| Dashboard trống | Chạy pipeline batch + tạo view ClickHouse |
| Tab BI: **403 Forbidden** | Chạy `./scripts/grant_guest_embed_permissions.sh` sau deploy (Gamma thiếu quyền dataset) |
| Tab BI: **Page not found** (404 Superset) | Bật `EMBEDDED_SUPERSET` trong `superset_config.py` → restart Superset → `enable-embed` |
| Tab BI: Firefox chặn iframe | Mở UI cùng origin (`localhost:8092`); `enable-embed --domain http://<ip>:8092` |
| Filter không có "Chọn tất cả" | Trong YAML: `select_all: true` |

---

## Ghi chú kỹ thuật (đọc khi cần)

- Superset **không** query MinIO trực tiếp — chỉ query ClickHouse view.
- URI ClickHouse trong Superset dùng DNS nội bộ K3s (`clickhouse-service.databases.svc.cluster.local`).
- Laptop chỉ gọi **Superset API**; MinIO chỉ để backup/export dashboard (GitOps).
- Code deploy: `src/superset_deploy/` — thêm loại chart mới trong `builders/chart_params.py`.
