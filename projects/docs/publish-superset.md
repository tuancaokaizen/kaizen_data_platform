# `publish_superset.sh` — Một lệnh deploy + nhúng BI

Script gom toàn bộ pipeline sau khi sửa YAML dashboard: **deploy API → (tùy chọn MinIO) → grant guest → enable-embed → sync FE/BE**.

**Vị trí:** `projects/superset/publish_superset.sh`

---

## Khi nào dùng?

| Tình huống | Lệnh |
|------------|------|
| Sửa `config/dashboards/*.yaml`, cần tab **Phân tích BI** | `./publish_superset.sh --tailscale --force --embed` |
| Thêm backup artifact lên MinIO (GitOps) | thêm `--minio` |
| Cluster UAT/PROD import từ MinIO | `./publish_superset.sh --tailscale --import-minio --embed --domain <origin-ui>` |
| Lần đầu (chưa bootstrap) | `./bootstrap.sh --tailscale` rồi `publish ...` |

> **MinIO không tự sync vào Superset.** `--minio` chỉ **upload backup**; cluster đích cần `--import-minio`.

---

## Ba workflow

### A — Dev (khuyến nghị)

YAML là nguồn sự thật → đẩy thẳng Superset API → bật embed cho UI local.

```bash
cd projects/superset
./publish_superset.sh --tailscale --force --embed
cd .. && ./run_local.sh
```

**Script tự làm:**

1. Port-forward Superset `:32288` (nếu `SUPERSET_HOST` là localhost)
2. `deploy --force` — chart + dashboard + **save-query-context** (tự động trong orchestrator)
3. `grant_guest_embed_permissions.sh`
4. `enable-embed` — domain localhost + LAN + Tailscale `:8092`
5. `export-registry` → `frontend/src/config/biDashboardRegistry.ts`
6. `go build` backend — đọc `dashboards.embed.json`

### B — Dev + backup MinIO

```bash
./publish_superset.sh --tailscale --force --embed --minio
```

Thêm bước export ZIP + upload `s3://superset/assets/dashboards/*.zip` + `manifest.json`.

### C — Import cluster khác (UAT/PROD)

Trên cluster **đích** (sửa `config/.env` → `SUPERSET_HOST` PROD):

```bash
./publish_superset.sh --tailscale --import-minio --embed \
  --domain https://reports.example.com
```

**Script làm:**

1. Import ZIP từ MinIO (`import_from_minio.py --all`)
2. `save-query-context` (import ZIP thường thiếu field này)
3. Grant + enable-embed + export-registry + rebuild backend

Không chạy `deploy --force` trừ khi thêm flag `--force` (deploy YAML rồi mới import — hiếm khi cần).

---

## Sơ đồ

```text
                    publish_superset.sh
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
    [--force]          [--minio]         [--import-minio]
         │                  │                  │
         ▼                  ▼                  ▼
   deploy YAML        export ZIP         import ZIP
   → Superset API     → MinIO S3         ← MinIO S3
         │                  │                  │
         └──────────────────┴──────────────────┘
                            │
                      [--embed]
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
           grant      enable-embed   export-registry
           guest      + embed.json   → biDashboardRegistry.ts
              │             │
              └──────┬──────┘
                     ▼
              go build backend
                     │
                     ▼
              ./run_local.sh → tab Phân tích BI
```

---

## Tất cả options

```bash
./publish_superset.sh --help
```

| Flag | Mô tả |
|------|--------|
| `--tailscale` | Dùng `config/.env.tailscale.example` |
| `--force` | Cập nhật chart/dashboard đã tồn tại (bắt buộc sau sửa YAML) |
| `--embed` | Grant + enable-embed + export-registry + rebuild BE |
| `--minio` | Sau deploy: export + upload MinIO |
| `--import-minio` | Import từ MinIO (bỏ qua deploy YAML) |
| `--domain URL` | Thêm origin UI (lặp được) |
| `--dashboard SLUG` | Chỉ một dashboard |
| `--rebuild-backend` | `go build` sau embed (mặc định khi `--embed`) |
| `--no-rebuild-backend` | Bỏ qua go build |
| `--skip-venv` | Dùng system `python3` |
| `--dry-run` | Preview deploy, không ghi API |

---

## So với script cũ

| Trước (nhiều lệnh) | Sau (`publish_superset.sh`) |
|--------------------|----------------------------|
| `deploy_superset.sh --force` | Gộp trong publish |
| `grant_guest_embed_permissions.sh` | `--embed` |
| `enable-embed --domain ...` | `--embed` (auto-detect IP) |
| `save-query-context` | Tự động trong `deploy --force`; thêm sau `--import-minio` |
| `export-registry` | `--embed` |
| `go build` backend | `--embed` (mặc định) |
| `sync_minio.sh` | `--minio` |

`deploy_superset.sh`, `sync_minio.sh` vẫn dùng được riêng lẻ.

---

## Lỗi hay gặp

| Lỗi | Fix |
|-----|-----|
| Preflight Superset fail | Kiểm tra kubectl, pod `apache-superset`, hoặc set `SUPERSET_HOST` Tailscale |
| enable-embed 404 | `enable_superset_embed_k8s.sh` + restart pod |
| Tab BI 403 | Chạy lại với `--embed`; thêm `--domain http://<ip>:8092` |
| Backend embed UUID cũ | `--embed` rebuild BE; restart `run_local.sh` |
| MinIO fail | Port-forward `:31900`; kiểm tra `MINIO_*` trong `config/.env` |
| import-minio empty | Chạy workflow B trên dev trước (`--minio`) |

---

## Liên kết

- [superset-and-embed.md](superset-and-embed.md) — kiến trúc nhúng FE/BE
- [../superset/README.md](../superset/README.md) — YAML dashboard, bootstrap
- [../../docs/DEPLOY.md](../../docs/DEPLOY.md) — deploy toàn platform
