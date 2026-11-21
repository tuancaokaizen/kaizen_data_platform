# Kaizen Analytics

Extensible analytics package: RFM rules + KMeans clusters + RandomForest cold-start, per-segment FP-Growth, and replenishment planning (Excel-Solver style). Models checkpoint to **MinIO**; ClickHouse keeps the catalog + marts.

Gói analytics mở rộng: luật RFM + cụm KMeans + RF cold-start, FP-Growth theo phân khúc, kế hoạch bổ sung tồn. Model checkpoint lên **MinIO**; ClickHouse giữ catalog + marts.

## Cycle (daily)

```
customer_segments (CPU sklearn)
  ├─ RFM rule labels (Frequency ≥ RF_MIN_FREQ)
  ├─ KMeans → Cluster* + joblib → MinIO s3://ml-models/kaizen/...
  ├─ RandomForest cold-start → MinIO
  ├─ report.ml_checkpoint (ArtifactUri + metrics)
  └─ report.segment_profile
        ↓
association_rules (CPU FP-Growth per SegmentCode + ALL)
        ↓
replenishment_plan (parallel after segments)
        ↓
refresh_report_views
```

Schedule: Airflow DAG `kaizen_analytics_pipeline` at `0 3 * * *` (after lakehouse).

**GPU:** cluster has NVIDIA nodes, but this stack stays on CPU (sklearn / FP-Growth / PuLP do not use CUDA). GPU remains for interactive `ccr-gpu-dev` / future cuML·XGBoost.

## Layout

```
kaizen_analytics/
  pipeline.py
  settings.py
  clickhouse_client.py
  steps/
  modeling/
    checkpoint.py      # CH catalog + MinIO load/save
    minio_store.py     # S3 put/get joblib
    clustering.py
    classifier.py
  mining/fpgrowth.py
  solvers/
```

## MinIO model layout

| Object | Example |
|--------|---------|
| Artifact | `s3://ml-models/kaizen/customer_kmeans/2026-11-06/artifact.joblib` |
| Metrics sidecar | `s3://ml-models/kaizen/customer_kmeans/2026-11-06/metrics.json` |
| Catalog | `report.ml_checkpoint.ArtifactUri` |

Env knobs (optional):

- `KAIZEN_ML_MINIO_ENDPOINT` (default `http://minio.storage.svc.cluster.local:9000`)
- `KAIZEN_ML_MINIO_ACCESS_KEY` / `KAIZEN_ML_MINIO_SECRET_KEY` (or `MINIO_*` / `S3_*`)
- `KAIZEN_ML_MINIO_BUCKET` (default `ml-models`)
- `KAIZEN_ML_MINIO_PREFIX` (default `kaizen`)

## Airflow

```bash
./dags/deploy_airflow.sh upload
# Install worker deps if needed: boto3 scikit-learn joblib pulp
# airflow dags unpause kaizen_analytics_pipeline
# airflow dags trigger kaizen_analytics_pipeline --conf '{"asOf":"2026-11-06"}'
```

Verify after run:

1. MinIO objects under `ml-models/kaizen/`
2. `SELECT * FROM report.ml_checkpoint ORDER BY AsOfDate DESC LIMIT 5`
3. Rows in `customer_segment`, `segment_profile`, `association_rule`, `replenishment_plan`

Dependency: `customer_segments >> association_rules` (rules read segments).

## Outputs (ClickHouse `report`)

| Table | View (VI) | Used by |
|-------|-----------|---------|
| `feature_customer_rfm` | `v_feature_khach_rfm` | Feature Store + SplitSet train/val/test |
| `customer_segment` | `v_phan_khuc_khach_hang` | RFM + cluster dashboard |
| `segment_profile` | `v_thong_ke_phan_khuc` | RFM + demographic stats by segment |
| `ml_checkpoint` | — | Catalog (URI → MinIO joblib) |
| `association_rule` | `v_luat_ket_hop_san_pham` | Basket rules by segment |
| `replenishment_plan` | `v_ke_hoach_bo_sung_ton` | Solver dashboard |

## Extend

1. **New step**: implement `AnalyticsStep` in `steps/`.
2. **New solver**: implement `ReplenishmentSolver`, register in `solvers/factory.py`.
3. **Knobs**: `KAIZEN_ANALYTICS_*` and `KAIZEN_ML_*` (see `settings.py` / `minio_store.py`).
