# Hướng dẫn Deploy — Kaizen Data Platform

Tài liệu tổng hợp cách deploy **tất cả thành phần** trong repo. Mỗi feature có script riêng; doc này mô tả thứ tự, lệnh và khi nào cần chạy lại.

> Kiến trúc tổng thể: [README.md](../README.md)  
> Superset + nhúng BI chi tiết: [projects/docs/superset-and-embed.md](../projects/docs/superset-and-embed.md)

---

## Mục lục

1. [Sơ đồ deploy](#1-sơ-đồ-deploy)
2. [Yêu cầu môi trường](#2-yêu-cầu-môi-trường)
3. [Chuẩn bị lần đầu](#3-chuẩn-bị-lần-đầu)
4. [Deploy đầy đủ (greenfield)](#4-deploy-đầy-đủ-greenfield)
5. [Từng feature](#5-từng-feature)
6. [Airflow — DAG & thứ tự chạy](#6-airflow--dag--thứ-tự-chạy)
7. [Ports & dịch vụ K3s](#7-ports--dịch-vụ-k3s)
8. [Deploy hàng ngày — chạy lại khi nào?](#8-deploy-hàng-ngày--chạy-lại-khi-nào)
9. [Script vận hành (ops)](#9-script-vận-hành-ops)
10. [Lỗi hay gặp](#10-lỗi-hay-gặp)

---

## 1. Sơ đồ deploy

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  INFRA K3s (đã có sẵn): PG, Kafka, ES, Nessie, MinIO, Spark, Flink,    │
│  ClickHouse, Airflow, Superset                                          │
└─────────────────────────────────────────────────────────────────────────┘
         ▲              ▲              ▲              ▲
         │              │              │              │
  schema_manager   stream-processing  batch-processing   dags/
  (DDL manual/     deploy_stream.sh   deploy_batch.sh   deploy_airflow.sh
   DAG sync)              │                  │                  │
         │              │                  │                  │
         └──────────────┴──────────────────┴──────────────────┘
                                    │
                         Airflow UI — trigger DAG
                                    │
                         ClickHouse report.* có data
                                    │
              ┌─────────────────────┴─────────────────────┐
              │                                           │
     projects/run_local.sh                    projects/superset/
     (Go API + React UI)                      bootstrap + deploy_superset
              │                                           │
              └──────── tab Phân tích BI (embed) ─────────┘
```

**Luồng dữ liệu sau deploy:** Flink/Kafka → PG/ES → Spark ingest → Nessie DW → Serving → Push ClickHouse → UI/Superset đọc `report.*`.

---

## 2. Yêu cầu môi trường

### Dev deploy (build + đẩy lên K3s)

| Tool | Dùng cho |
|------|----------|
| **kubectl** + **kubeconfig** | Port-forward, Flink submit, Superset PVC |
| **Gradle** + **JDK 17+** | Build JAR batch/stream (không có `gradlew` trong repo) |
| **Go 1.21+** | Report API |
| **Node.js 18+** / **npm** | Report UI |
| **Python 3.10–3.12** + venv | Superset deploy CLI |
| **mc** hoặc **aws cli** | Upload JAR/DAGs lên MinIO |

### Chỉ xem báo cáo (không deploy)

| Tool | Dùng cho |
|------|----------|
| **Tailscale** (cùng tailnet) | ClickHouse NodePort `30900` |
| **Go** + **Node.js** | `./run_local.sh` |
| Không cần kubectl | `backend/.env.tailscale.example` |

### File không có trong git — tự tạo

| File | Cách lấy |
|------|----------|
| `dags/config.k3s-new` | Admin cung cấp kubeconfig |
| `**/.env` | Copy từ `.env.example` / `.env.tailscale.example` |
| `**/build/`, `*.jar` | Build script |
| `node_modules/` | `npm install` |

---

## 3. Chuẩn bị lần đầu

```bash
git clone <repo-url> kaizen_data_platform
cd kaizen_data_platform

# Kubeconfig — bắt buộc cho dev deploy
cp /path/to/kubeconfig dags/config.k3s-new
# hoặc: export K3S_CONF=~/.kube/config.k3s-new

# MinIO endpoint — dùng chung batch/stream/airflow
# Sửa projects/batch-processing/config.ini và projects/stream-processing/config.ini
# [minio] endpoint = http://localhost:31900  (port-forward hoặc NodePort)
```

**Kiểm tra kết nối cluster:**

```bash
export KUBECONFIG=dags/config.k3s-new   # hoặc ~/.kube/config.k3s-new
kubectl get nodes
kubectl get svc -A | grep -E 'minio|clickhouse|flink|superset'
```

---

## 4. Deploy đầy đủ (greenfield)

Thực hiện **tuần tự** trên máy dev (đã có K3s + infra).

### Bước 0 — Schema nguồn (tùy chọn reset)

```bash
# Áp DDL PostgreSQL + ES mapping (xem schema_manager/README.md)
# Reset data dev (script local, không track git):
./clean_data.sh
```

### Bước 1 — Flink streaming

```bash
cd projects/stream-processing
./deploy_stream.sh stream    # build + upload MinIO + submit jobs
```

Jobs: `OrderProducerJob`, `PaymentProducerJob`, `KaizenConsumerJob` → Kafka → PG + ES.

### Bước 2 — Spark batch JAR

```bash
cd projects/batch-processing
./deploy_batch.sh deploy     # build + upload → MinIO bucket spark
```

### Bước 3 — Airflow DAGs

```bash
cd dags
./deploy_airflow.sh upload     # sync DAGs + copy ClickHouse DDL vào dags/
```

Airflow scheduler tự pick DAG mới từ MinIO bucket `airflow`.

### Bước 4 — Chạy pipeline trên Airflow UI

Xem [mục 6](#6-airflow--dag--thứ-tự-chạy). Tóm tắt:

1. `master_data_pipeline` — sinh master PG  
2. `spark_master_data_ingest_pipeline`  
3. `spark_transaction_data_ingest_pipeline` (hoặc Flink + incremental)  
4. `spark_dw_transform_pipeline` — DW + serving + **push ClickHouse** + tạo views  

Hoặc để `sil_daily_pipeline` chạy tự động 00:30.

### Bước 5 — Superset dashboard (BI)

```bash
cd projects/superset
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.tailscale.example config/.env

# Cluster: bật embed + PVC (một lần)
kubectl apply -f k8s/superset-embed-configmap.yaml
./scripts/enable_superset_embed_k8s.sh
./scripts/apply_superset_pvc_k8s.sh

# Dataset + dashboard + nhúng BI (một lệnh)
./bootstrap.sh --tailscale
./publish_superset.sh --tailscale --force --embed
```

Chi tiết: [projects/docs/publish-superset.md](../projects/docs/publish-superset.md)

### Bước 6 — Report UI (local)

```bash
cd projects
cp backend/.env.example backend/.env
./run_local.sh
```

→ http://localhost:8092 — tab **Bảng chi tiết** + **Phân tích BI**

---

## 5. Từng feature

### 5.1 Schema Manager

**Đường dẫn:** `schema_manager/`  
**Vai trò:** DDL PostgreSQL, ES mappings, ClickHouse `report.*` + views tiếng Việt.

| Thành phần | Deploy |
|------------|--------|
| PostgreSQL | Chạy SQL trong `postgres_source/` (hoặc `schema_postgres.sql`) trên DB nguồn |
| Elasticsearch | Apply JSON trong `elasticsearch/` lên cluster ES |
| ClickHouse | DDL trong `clickhouse/*.sql` — **tự sync** khi `./deploy_airflow.sh upload`; task `create_clickhouse_report_views` trong DAG transform |

Chi tiết: [schema_manager/README.md](../schema_manager/README.md)

---

### 5.2 Flink Streaming

**Đường dẫn:** `projects/stream-processing/`  
**Script:** `deploy_stream.sh`  
**Config:** `config.ini` (MinIO, Kafka, …)

| Lệnh | Mô tả |
|------|--------|
| `./deploy_stream.sh build` | Build shadow JAR |
| `./deploy_stream.sh upload` | Upload → MinIO `flink/artifact/...` |
| `./deploy_stream.sh start` | Submit jobs qua Flink JobManager pod |
| `./deploy_stream.sh stop` | Stop + savepoint |
| `./deploy_stream.sh status` | `flink list` |
| `./deploy_stream.sh clean` | Cancel jobs + xóa checkpoint MinIO |
| `./deploy_stream.sh stream` | **build + upload + start** (thường dùng) |
| `./deploy_stream.sh restart` | stop → build → upload → start |

**Khi deploy lại:** Sửa code Java → `stream` hoặc `restart`.

---

### 5.3 Spark Batch

**Đường dẫn:** `projects/batch-processing/`  
**Script:** `deploy_batch.sh`  
**Artifact:** `build/libs/kaizen-batch-processing-1.0.0-batch.jar` → MinIO `spark/jobs/kaizen-batch-processing.jar`

| Lệnh | Mô tả |
|------|--------|
| `./deploy_batch.sh build` | `gradle clean buildBatchJar` |
| `./deploy_batch.sh upload` | Upload MinIO |
| `./deploy_batch.sh deploy` | **build + upload** |
| `./deploy_batch.sh clean` | Xóa JAR trên MinIO |

**Khi deploy lại:** Sửa Scala Spark job → `deploy`. Airflow DAG dùng JAR mới ở lần submit tiếp theo.

---

### 5.4 Airflow DAGs

**Đường dẫn:** `dags/`  
**Script:** `deploy_airflow.sh upload`

- Mirror toàn bộ `dags/` (trừ `*.sh`, kubeconfig) → MinIO `airflow/dags/`
- Copy `schema_manager/clickhouse/*.sql` → `dags/schema_manager/clickhouse/` trước khi upload

**Khi deploy lại:** Sửa DAG Python, `jobs/`, `table_configs.yaml` → `upload`.

Patch sync (nếu cần): `dags/patch_airflow_dag_sync.sh`

---

### 5.5 Report Dashboard — Backend (Go)

**Đường dẫn:** `projects/backend/`  
**Script:** `deploy_backend.sh`  
**Chạy full stack:** `cd projects && ./run_local.sh`

| Lệnh | Mô tả |
|------|--------|
| `./deploy_backend.sh build` | `go build` → `build/kaizen-report-api` |
| `./deploy_backend.sh run` | Chạy API `:8090` |
| `./deploy_backend.sh clean` | Xóa `build/` |

**Env chính** (`backend/.env`):

| Biến | Mặc định local |
|------|----------------|
| `API_PORT` | 8090 |
| `CLICKHOUSE_HOST` | localhost (hoặc IP Tailscale) |
| `CLICKHOUSE_PORT` | 9000 / 30900 |
| `SUPERSET_HOST` | http://localhost:32288 |
| `SUPERSET_EMBED_DOMAIN` | http://localhost:8092 |

**API:** `/healthz`, `/api/v1/reports/*`, `/api/v1/bi/*`

Chi tiết: [projects/backend/README.md](../projects/backend/README.md)

---

### 5.6 Report Dashboard — Frontend (React)

**Đường dẫn:** `projects/frontend/`  
**Script:** `deploy_frontend.sh`

| Lệnh | Mô tả |
|------|--------|
| `./deploy_frontend.sh run` | Dev server `:8092` |
| `./deploy_frontend.sh build` | Production `dist/` |
| `./deploy_frontend.sh clean` | Xóa `node_modules`, `dist` |

`VITE_API_BASE_URL` để trống → Vite proxy `/api` → backend.

Tab **Phân tích BI:** `@superset-ui/embedded-sdk` + proxy `/embedded`, `/static` → Superset.

Chi tiết: [projects/frontend/README.md](../projects/frontend/README.md)

---

### 5.7 Superset BI

**Đường dẫn:** `projects/superset/`  
**Data:** ClickHouse views `report.v_bao_cao_*` (cùng nguồn với Go API)

| Việc | Lệnh |
|------|------|
| **Deploy + nhúng BI (khuyến nghị)** | `./publish_superset.sh --tailscale --force --embed` |
| Setup dataset (một lần) | `./bootstrap.sh --tailscale` |
| Deploy/cập nhật dashboard only | `./deploy_superset.sh --force` |
| Deploy + backup MinIO | `./publish_superset.sh --force --embed --minio` |
| Import MinIO cluster khác | `./publish_superset.sh --import-minio --embed` |

**Sửa chart/layout:** `config/dashboards/<slug>.yaml` → `publish_superset.sh --force --embed`

Chi tiết publish: [projects/docs/publish-superset.md](../projects/docs/publish-superset.md)  
Chi tiết embed: [projects/docs/superset-and-embed.md](../projects/docs/superset-and-embed.md)  
Chi tiết Superset: [projects/superset/README.md](../projects/superset/README.md)

---

### 5.8 Report UI — một lệnh (`run_local.sh`)

**Đường dẫn:** `projects/run_local.sh`

Tự động khi có kubectl:

1. Port-forward ClickHouse `:9000`, Superset `:32288`
2. PVC Superset, grant guest permissions
3. `enable-embed` + detect IP (LAN/Tailscale)
4. `save-query-context`, `export-registry`
5. Build + start backend `:8090`, frontend `:8092`

**Đồng nghiệp (không kubectl):**

```bash
cd projects/backend && cp .env.tailscale.example .env
cd .. && ./run_local.sh
```

`CLICKHOUSE_HOST` = IP Tailscale K3s, `PORT=30900`.

Chi tiết: [projects/README.md](../projects/README.md)

---

## 6. Airflow — DAG & thứ tự chạy

### DAG chính

| DAG | Mô tả | Lịch |
|-----|--------|------|
| `master_data_pipeline` | Sinh master data PostgreSQL | Manual |
| `incremental_data_pipeline` | Sinh giao dịch incremental | Manual |
| `spark_master_data_ingest_pipeline` | Ingest master → `nessie.fnd.*` | Manual / SIL |
| `spark_transaction_data_ingest_pipeline` | Ingest transaction → FND | Manual / SIL |
| `spark_dw_transform_pipeline` | DW + serving + push CH + views | Manual / SIL |
| `sil_daily_pipeline` | Chuỗi ingest → transform | **00:30** hàng ngày |
| `spark_dw_maintenance_pipeline` | Compaction, expire snapshot | **02:00** |
| `spark_partition_realignment_pipeline` | Partition evolution (một lần) | Manual |
| `spark_transaction_simulation_pipeline` | Mô phỏng giao dịch batch | Manual |
| `spark_dw_clear_nessie_pipeline` | Xóa dữ liệu Nessie (dev) | Manual |
| `spark_dw_clear_serving_pipeline` | Xóa serving (dev) | Manual |

### Thứ tự greenfield (manual trigger)

```text
master_data_pipeline
    → spark_master_data_ingest_pipeline
    → (Flink stream HOẶC incremental_data_pipeline)
    → spark_transaction_data_ingest_pipeline
    → spark_dw_transform_pipeline
```

Sau transform: kiểm tra ClickHouse `report.financial_report`, `report.fund_report` và views `report.v_bao_cao_*`.

### Maintenance

- **Hàng ngày:** `spark_dw_maintenance_pipeline` (tự chạy 02:00)
- **Một lần:** `spark_partition_realignment_pipeline` khi đổi partition strategy

---

## 7. Ports & dịch vụ K3s

| Dịch vụ | Namespace (thường) | Port local (forward) | NodePort / ghi chú |
|---------|-------------------|----------------------|---------------------|
| ClickHouse native | `databases` / `clickhouse-service` | **9000** | `clickhouse-shared` **30900** (Tailscale) |
| ClickHouse HTTP | `databases` | 8123 | 30123 |
| MinIO | (grep `minio`) | **31900** → 9000 | Upload JAR/DAGs |
| Flink JobManager | (grep `jobmanager`) | — | Submit qua `kubectl exec` |
| Superset | `apps` / `apache-superset` | **32288** → 8888 | BI + embed API |
| Report Backend | local | **8090** | Không deploy K3s |
| Report Frontend | local | **8092** | Không deploy K3s |

**Kubeconfig:** `dags/config.k3s-new` hoặc `K3S_CONF` / `~/.kube/config.k3s-new`

---

## 8. Deploy hàng ngày — chạy lại khi nào?

| Bạn sửa… | Chạy lại |
|----------|----------|
| Flink Java | `deploy_stream.sh stream` hoặc `restart` |
| Spark Scala | `deploy_batch.sh deploy` + trigger DAG trên Airflow |
| DAG Python / `jobs/` / `table_configs.yaml` | `deploy_airflow.sh upload` |
| ClickHouse DDL | Sửa `schema_manager/clickhouse/` → `deploy_airflow.sh upload` + trigger transform (hoặc chạy SQL tay) |
| Go API | `deploy_backend.sh build` hoặc `run_local.sh` |
| React UI | Refresh dev server / `deploy_frontend.sh build` |
| Superset chart YAML | `./publish_superset.sh --force --embed` |
| Chỉ có data mới (pipeline) | **Không** cần redeploy UI/Superset |
| Đổi IP mở UI (Tailscale) | `./publish_superset.sh --force --embed` hoặc `enable-embed --domain` |
| Restart pod Superset | `publish_superset.sh --embed` (hoặc grant lại) |

---

## 9. Script vận hành (ops)

Script ở **root repo** (nhiều file local, không track git — copy từ máy dev nếu thiếu):

| Script | Mô tả |
|--------|--------|
| `clean_data.sh` | TRUNCATE PG + xóa ES documents |
| `clean_transaction_data.sh` | Dọn giao dịch |
| `check_nessie.sh` | Kiểm tra Nessie |
| `check_spark_cluster.sh` | Kiểm tra Spark |
| `clear_nessie_*.sh` | Xóa layer Nessie (dev) |
| `setup_nessie_db.sh` | Setup Nessie DB |

DAG tương ứng trên Airflow: `spark_dw_clear_nessie_pipeline`, `spark_dw_clear_serving_pipeline`.

---

## 10. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Hướng xử lý |
|-------------|-------------|-------------|
| `kubectl` not found | Thiếu kubeconfig | Copy `config.k3s-new` hoặc dùng `.env.tailscale` |
| MinIO upload fail | Port chưa mở | Port-forward MinIO `:31900` hoặc sửa `config.ini` |
| Gradle not found | Chưa cài | `brew install gradle`, JDK 17+ |
| Flink job không RUNNING | JM pod / JAR lỗi | `deploy_stream.sh status`, xem log JM |
| Airflow DAG không đổi | Chưa sync MinIO | `deploy_airflow.sh upload`, đợi scheduler |
| UI trống / không data | CH chưa có data | Chạy `spark_dw_transform_pipeline` |
| ClickHouse connection refused | Sai host/port | `run_local.sh` forward hoặc Tailscale `30900` |
| Tab BI 403 | Thiếu domain / quyền Gamma | `grant_guest_embed_permissions.sh` + `enable-embed --domain` |
| Tab BI blank iframe | Thiếu Vite proxy | Kiểm tra `frontend/vite.config.ts` |
| Superset deploy fail | Sai Python | Dùng `superset/.venv/bin/python3` |
| `superset_deploy` not found | Chưa pip install | `pip install -r requirements.txt` trong venv |

---

## Bảng script deploy (trong git)

| Script | Thư mục | Lệnh chính |
|--------|---------|------------|
| `deploy_stream.sh` | `projects/stream-processing/` | `stream`, `restart`, `stop`, `status` |
| `deploy_batch.sh` | `projects/batch-processing/` | `deploy` |
| `deploy_airflow.sh` | `dags/` | `upload` |
| `deploy_backend.sh` | `projects/backend/` | `build`, `run` |
| `deploy_frontend.sh` | `projects/frontend/` | `run`, `build` |
| `run_local.sh` | `projects/` | Full UI stack |
| `publish_superset.sh` | `projects/superset/` | **Deploy + embed + (optional) MinIO** |
| `bootstrap.sh` | `projects/superset/` | Dataset CH (một lần) |
| `deploy_superset.sh` | `projects/superset/` | Dashboard từ YAML only |
| `sync_minio.sh` | `projects/superset/` | Backup dashboard ZIP |

---

## Liên kết tài liệu

| Tài liệu | Nội dung |
|----------|----------|
| [README.md](../README.md) | Kiến trúc, quick start |
| [projects/README.md](../projects/README.md) | Report UI, Tailscale |
| [schema_manager/README.md](../schema_manager/README.md) | DDL PG, ES, ClickHouse |
| [projects/superset/README.md](../projects/superset/README.md) | Superset YAML deploy |
| [projects/docs/publish-superset.md](../projects/docs/publish-superset.md) | **publish_superset.sh** — một lệnh deploy + embed |
| [projects/docs/superset-and-embed.md](../projects/docs/superset-and-embed.md) | Nhúng BI FE/BE |
| [projects/backend/README.md](../projects/backend/README.md) | Go API |
| [projects/frontend/README.md](../projects/frontend/README.md) | React UI |
