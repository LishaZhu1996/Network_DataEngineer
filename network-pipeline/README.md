# Network Metrics Pipeline

End-to-end daily ingestion pipeline for cell-tower network metrics.  
Implements a **Bronze → Silver → Gold** medallion architecture orchestrated with Apache Airflow.

---

## Architecture & Design Choices

```
S3: raw-telecom-network-data/
│   network_metrics_YYYYMMDD.csv
│
├── bronze/  (raw parquet, metadata-only enrichment)
├── silver/  (cleaned facts + SCD2 dim table)
└── gold/    (hourly + daily aggregations)
```

### Layer responsibilities

| Layer | Content | Format | Key design choice |
|-------|---------|--------|-------------------|
| **Bronze** | Exact copy of source CSV + pipeline metadata (`_run_id`, `_source_file`, `_ingested_at`) | Parquet | Immutable raw store; enables full re-processing without re-fetching from S3 |
| **Silver (clean)** | Type-cast, deduplicated measurements; unix ts → UTC datetime | Parquet | Partitioned by `execution_date` for efficient date-range queries |
| **Silver (dim)** | SCD Type 2 tower×region dimension | Parquet | Tracks attribute drift over time; current-only view via `is_current=TRUE` |
| **Gold** | Hourly & daily aggregations per region | Parquet | Pre-aggregated for BI tools / Superset; small and fast to query |

### SCD Type 2 design

- **Natural key**: `tower_id + region`
- **`hash_key`**: `MD5(tower_id | region)` — stable identifier across versions
- **`hash_diff`**: `MD5(avg_signal_strength | total_data_volume_mb)` — change detector
- One representative row per `(tower_id, region)` per day is computed (avg signal, sum volume) and compared against the current record.
- If `hash_diff` differs → old row gets `effective_to = today`, `is_current = False`; new row inserted.
- If unchanged → no write (idempotent).
- **`dim_tower_region_history`**: full version history.
- **`dim_tower_region_current`**: view filtered to `is_current = TRUE` — always the latest state.

### Control table

One row per `(run_id, table_name)` tracks pipeline lifecycle:

```
status: running → success | fail | skipped
```

- Inserted at DAG start (`status = running`).
- Updated to `success` when each layer finishes.
- Updated to `fail` with `error_message` in the `on_failure_callback`.
- Updated to `skipped` (all rows) when no source file is found for that date.

### Data quality checks

Checks run after bronze ingestion, before silver:

| Check | Level | Action |
|-------|-------|--------|
| File not empty | `fail` | Abort pipeline |
| Required columns present | `fail` | Abort pipeline |
| Null values in key columns (tower_id, region, timestamp) | `fail` | Abort pipeline |
| Null values in metric columns | `warn` | Log, continue |
| `signal_strength` in [-120, -40] dBm | `warn` | Log anomalies |
| `data_volume_mb >= 0` | `fail` | Abort pipeline |
| Timestamp within expected file date | `warn` | Log, continue |
| Duplicate rows on (tower_id, region, timestamp) | `warn` | Drop duplicates in silver |

Results are written to `./data/dq_logs/dq_log_{date}.json` and summarised in the `dq_log` table.

---

## Task Breakdown

```
start_run           → Insert 'running' rows into pipeline_control for all tables
check_s3_file       → BranchPythonOperator: file present? → ingest_bronze : mark_skipped
mark_skipped        → Update all control rows to 'skipped'; end
ingest_bronze       → Read CSV from S3, add metadata, write Parquet to bronze zone
run_dq_checks       → Validate bronze data; abort on hard failures
process_silver      → Clean data, write silver_clean; apply SCD2 on dim_tower_region
process_gold        → Aggregate to hourly and daily gold tables
mark_success        → Update any remaining 'running' rows to 'success'
end_pipeline        → EmptyOperator (NONE_FAILED_MIN_ONE_SUCCESS trigger rule)
```

Failure at any task → `on_failure_callback` fires → control table row updated to `fail` with error message + alerting hook.

---

## Assumptions

1. The S3 source file is named exactly `network_metrics_YYYYMMDD.csv` matching the Airflow `ds` (execution date).
2. `tower_id + region` is the natural key for the SCD2 dimension.
3. Signal strength outside [-120, -40] dBm is treated as a warning (not a hard failure), since the sample data contains values like -94.5 dBm which are near the boundary.
4. Multiple readings per tower per day are expected (the sample shows ~15-minute intervals). The SCD2 dim uses daily aggregates (avg signal, sum volume) as the representative snapshot.
5. Re-runs for the same `execution_date` are safe: bronze write is overwrite, silver SCD2 is idempotent (hash comparison), gold writes overwrite the date partition.
6. The control table lives in PostgreSQL (production) or SQLite (local dev, via `PIPELINE_DB_CONN` env var).

---

## Local Development

```bash
pip install -r requirements.txt

# Point to local filesystem (default)
export USE_LOCAL_FS=true
export LOCAL_SOURCE_PATH=./sample_data

# Run a single day end-to-end (without Airflow)
python3 - <<'EOF'
import sys; sys.path.insert(0, "src")
from bronze_ingestion import ingest_bronze
from silver_processing import process_silver
from gold_aggregation import process_gold

DATE = "2025-07-23"
RUN_ID = "local-test-001"

ingest_bronze(DATE, RUN_ID)
process_silver(DATE, RUN_ID)
process_gold(DATE, RUN_ID)
print("Done — check ./data/")
EOF
```

---

## Monitoring & Alerting Strategy

The `on_failure_callback` on every task provides the alerting integration point:

```python
def _on_failure(context):
    # 1. Update control table to 'fail'
    # 2. Send Slack/PagerDuty/SNS notification with:
    #      - DAG name, task name, run_id
    #      - Error message
    #      - Link to Airflow logs
```

**Recommended alerting tiers:**

| Scenario | Alert channel | Priority |
|----------|--------------|----------|
| File missing (skipped) | Slack #data-ops | Low |
| DQ hard failure (schema/null) | Slack #data-incidents | Medium |
| Bronze/Silver/Gold task failure (1st retry) | Slack #data-incidents | Medium |
| All 3 retries exhausted | PagerDuty | High |

**Additional monitoring recommendations:**
- Airflow SLAs: set `sla=timedelta(hours=3)` on `mark_success` to catch slow runs.
- Row-count anomaly detection: compare today's record count against a 7-day rolling average in `run_dq_checks`; alert if deviation > 20%.
- Superset dashboard on the `dq_log` and `pipeline_control` tables for ops visibility.

---

## File Structure

```
network-pipeline/
├── dags/
│   └── network_metrics_dag.py        # Airflow DAG
├── src/
│   ├── bronze_ingestion.py
│   ├── silver_processing.py
│   ├── gold_aggregation.py
│   └── utils/
│       ├── control_table.py
│       ├── data_quality.py
│       └── scd2.py
├── sql/
│   └── ddl.sql                        # Reference DDL for all tables
├── mock_tables/
│   ├── bronze_sample.csv
│   ├── silver_clean_sample.csv
│   ├── silver_dim_history_sample.csv
│   ├── gold_hourly_sample.csv
│   ├── gold_daily_sample.csv
│   └── validation_log_sample.csv
├── sample_data/
│   └── network_metrics_20250723.csv
├── requirements.txt
└── README.md
```
