# Network Metrics Pipeline

End-to-end daily ingestion pipeline for cell-tower network metrics.  
Implements a **Bronze → Silver → Gold** medallion architecture orchestrated with Apache Airflow, with all output tables written as **Apache Iceberg** tables.

---

## Assumption
- It can happen that the destination doesn't have new file for specific day, but as long as the file is detected, it should not be empty (should have some data for that day)。
- For a given timestamp, the primary key should be the combination of tower_id + region。
- Noraml range of signal strenth is (-120.0 , -40.0) and data volumne should be larger than 0.

---

## Overall Architecture

```
S3: raw-telecom-network-data/
│   network_metrics_YYYYMMDD.csv          ← daily drop
│
├── bronze/  network_metrics_raw          ← raw copy 
├── silver/  network_metrics_clean        ← cleaned, typed, deduplicated
└── gold/    fact_tower_daily_snapshot    ← one row per tower per day (primary output)
             network_metrics_hourly       ← aggregated per region per hour
             network_metrics_daily        ← aggregated per region per day
```

All gold and silver tables are **Iceberg tables** — time-travel, schema evolution, and partition pruning come for free. Bronze is Parquet locally and Iceberg on S3.

---

## Project structure

```
network-pipeline/
├── dags/
│   └── network_metrics_dag.py          # Airflow DAG for orchastration (daily + backfill)
├── data/                               # Result of local run (parquet files)
│   └── bronze/                         
│   └── dq_logs/                        # DQ report
│   └── gold/
│   └── silver/
├── src/
│   ├── __init__.py                     # Centralised config (paths, env vars, table names)
│   ├── bronze_ingestion.py
│   ├── silver_processing.py
│   ├── gold_aggregation.py
│   └── utils/
│       ├── __init__.py                 # Iceberg catalog helpers (get_catalog, write_iceberg, read_iceberg)
│       ├── control_table.py
│       └── data_quality.py
├── sql/
│   └── ddl.sql                         # Reference DDL — schemas for all Iceberg tables
├── mock_tables/                        # Sample output CSVs (reference / documentation only)
├── sample_data/
│   └── network_metrics_20250723.csv
├── requirements.txt
└── README.md
```

### Configuration (`src/config.py`)

All paths, environment variables, and table names are declared once here and imported by every module — nothing is hardcoded in the processing files.

Key variables:

| Variable | Default | Override via |
|----------|---------|--------------|
| `USE_LOCAL` | `false` | `USE_LOCAL_FS=true` (local dev only) |
| `BASE_DATA_PATH` | `./data` | `BASE_DATA_PATH` |
| `S3_BUCKET` | `raw-telecom-network-data` | `S3_BUCKET` |
| `ICEBERG_CATALOG_TYPE` | `sql` (SQLite local) | `ICEBERG_CATALOG_TYPE=glue` |
| `DB_CONN` | SQLite at `./data/pipeline_control.db` | `PIPELINE_DB_CONN` |

### Iceberg helpers (`src/utils/__init__.py`)

- `get_catalog()` — returns a local `SqlCatalog` (SQLite-backed) or AWS `GlueCatalog` based on env.
- `write_iceberg(df, namespace, table_name, overwrite_filter)` — creates the table on first write; when `overwrite_filter` is set, deletes matching rows before appending (used for backfill).
- `read_iceberg(namespace, table_name, filters)` — reads an Iceberg table back into a DataFrame.

---

## Control table

One row per `(run_id, table_name, execution_date)`:

```
status: running → success | fail | skipped
```

| Status | When |
|--------|------|
| `running` | Inserted at DAG start |
| `success` | Updated when each layer finishes |
| `fail` | Set in `on_failure_callback` with error message |
| `skipped` | Set when no source file found for that date |

Save necessary information about the dag run, including run_id, table_name, execution_date, status, duration, date_inserted, error (if applicable, etc).

---

## DAG flow

```
start_run
    │
    │  Insert 'running' rows into pipeline_control for all tables
    │
check_s3_file  ───────────────────────────────┐
    │ file found                              │ no file for any date
    │                                         │
ingest_bronze                           mark_skipped
    │                                         │
    │  Read CSV from S3                       │  Update control table → 'skipped'
    │  Write to bronze (Parquet/Iceberg)      │
    │                                    end_pipeline
run_dq_checks                                 ▲
    │                                         │
    │  7 checks (schema, nulls, ranges,       │
    │  duplicates, freshness)                 │
    │  Abort on hard failures                 │
    │                                         │
process_silver                                │
    │                                         │
    │  Cast types, drop nulls/dupes           │
    │  Convert unix ts → UTC datetime         │
    │  Write silver.network_metrics_clean     │
    │  (Iceberg, partitioned by date)         │
    │                                         │
process_gold                                  │
    │                                         │
    │  fact_tower_daily_snapshot              │
    │  network_metrics_hourly                 │
    │  network_metrics_daily                  │
    │  (all Iceberg, partitioned by date)     │
    │                                         │
mark_success                                  │
    │                                         │
    │  Update control table → 'success'       │
    │                                         │
end_pipeline ─────────────────────────────────┘
             (NONE_FAILED_MIN_ONE_SUCCESS)
```

**On any task failure:**
- `on_failure_callback` fires immediately
- Control table updated to `'fail'` with error message
- Airflow retries up to 3 times with 5-minute back-off
- After all retries exhausted → alert team (Slack / PagerDuty)

---

## DAG: daily run vs backfill

### Daily run (default)
Processes the Airflow execution date (`ds`) automatically at 02:00 UTC.

### Backfill / reload
Trigger with a `start_date` + `end_date` in the conf to reload a date range:

```bash
airflow dags trigger network_metrics_pipeline \
  --conf '{"start_date": "2025-07-01", "end_date": "2025-07-23"}'
```

Backfill behaviour:
- Each date in the range is processed independently.
- If the source file is **missing** for a date → that date is skipped and logged; other dates continue.
- Gold Iceberg tables have existing rows for those dates **deleted before re-insert** — fully idempotent.
- Control table gets one row per `(table, date)` for full auditability.

---

## Data quality checks 

| Check | Severity | Action on failure |
|-------|----------|-------------------|
| File not empty | `fail` | Abort |
| Required columns present | `fail` | Abort |
| Null in key columns (tower_id, region, timestamp) | `warn` | Log, continue |
| `signal_strength` in [-120, -40] dBm | `warn` | Log anomalies |
| `data_volume_mb ≥ 0` | `fail` | Abort |
| Timestamp within file date | `warn` | Log, continue |
| Duplicate rows on (tower_id, region, timestamp) | `warn` | Drop in silver |

Results written to `./data/dq_logs/dq_log_{date}.json`.
When doing the DQ check, if any `'fail'` DQ check detected, the dag will be failed.

---

## Data Models

### Bronze — Raw Copy

**Model: Append-only flat table, no transformation**

```
bronze.network_metrics_raw
├── tower_id          INTEGER
├── region            VARCHAR
├── timestamp         BIGINT          ← unix ts, untouched
├── signal_strength   FLOAT
├── data_volume_mb    FLOAT
├── _run_id           VARCHAR         ← pipeline metadata
├── _source_file      VARCHAR
├── _ingested_at      TIMESTAMP
└── _execution_date   DATE            ← partition key
```

**Why:** Bronze is an immutable audit copy of the source. No business logic applied — if anything goes wrong downstream you can always re-process from here without re-fetching from S3.

---

### Silver — Cleaned Measurements

**Model: Cleaned, enriched and typed**

```
silver.network_metrics_clean
├── tower_id          INTEGER         ← cast + validated
├── region            VARCHAR
├── timestamp         BIGINT
├── signal_strength   FLOAT
├── data_volume_mb    FLOAT
├── event_datetime    TIMESTAMP       ← converted from unix ts
├── event_date        DATE            ← derived
├── event_hour        SMALLINT        ← derived, used by gold hourly
├── _execution_date   DATE            ← partition key
└── _run_id           VARCHAR
```

**Why:** Silver preserves the original grain (one row per reading) but with clean types and derived time columns. Keeping full granularity here means gold can be re-aggregated differently in future without going back to bronze. Partitioned by `_execution_date` so each day's data is isolated.

---

### Gold — Aggregated Outputs

**Three tables, each at a different grain:**

```
gold.fact_tower_daily_snapshot      ← primary analytical table
├── tower_id                INTEGER  ─┐
├── region                  VARCHAR   ├─ natural key
├── execution_date          DATE      ┘  partition key
├── avg_signal_strength     FLOAT
├── min_signal_strength     FLOAT    ← worst reading of the day
├── max_signal_strength     FLOAT    ← best reading of the day
├── stddev_signal_strength  FLOAT    ← instability indicator
├── total_data_volume_mb    FLOAT
├── avg_data_volume_mb      FLOAT
└── reading_count           INTEGER  ← how many 15-min readings

gold.network_metrics_hourly         ← operational monitoring
├── region                  VARCHAR  ─┐
├── event_hour              SMALLINT  ├─ natural key
├── execution_date          DATE      ┘  partition key
├── total_data_volume_mb    FLOAT
├── avg_signal_strength     FLOAT
├── tower_count             INTEGER
└── record_count            INTEGER

gold.network_metrics_daily          ← reporting / dashboards
├── region                  VARCHAR  ─┐ natural key
├── execution_date          DATE      ┘  partition key
├── total_data_volume_mb    FLOAT
├── avg_signal_strength     FLOAT
├── tower_count             INTEGER
└── record_count            INTEGER
```

Each layer adds value without destroying the previous one — bronze protects against source data loss, silver is the single source of truth for all analytical re-processing, gold is optimised for consumption so BI tools and analysts never touch raw data directly.

---

## Additional explanation for Gold layer design

### `fact_tower_daily_snapshot` — primary output

One row per `(tower_id, region, execution_date)`, capturing the full daily performance picture of each tower:

| Column | Description |
|--------|-------------|
| `tower_id`, `region` | Natural key |
| `avg_signal_strength` | Mean signal across all readings that day (dBm) |
| `min_signal_strength` | Worst signal that day |
| `max_signal_strength` | Best signal that day |
| `stddev_signal_strength` | Variability — high stddev = unstable tower |
| `total_data_volume_mb` | Total throughput for the day |
| `avg_data_volume_mb` | Average per reading |
| `reading_count` | Number of 15-min interval readings |
| `execution_date` | Partition key |

> **Why a fact table, not SCD2?**  
> `signal_strength` and `data_volume_mb` are **metrics**, not descriptive attributes. They are expected to change with every reading. SCD2 is designed for slowly changing attributes (e.g. a tower's region assignment, vendor, or operational status). Applying SCD2 to measurements would expire a record almost every day, which defeats the purpose. A daily snapshot fact table captures the same history more naturally and is simpler to query.

### `network_metrics_hourly`
Per `(region, event_hour, execution_date)` — total volume, avg signal, tower count.

### `network_metrics_daily`
Per `(region, execution_date)` — daily rollup for reporting.

---

## Notification  


---
## Local development

```bash
pip install -r requirements.txt

# Run a single day end-to-end (no Airflow needed)
BASE=`pwd`
export USE_LOCAL_FS=true
export BASE_DATA_PATH=$BASE/data
export LOCAL_SOURCE_PATH=$BASE/sample_data

python3 - <<'EOF'
import sys; sys.path.insert(0, "src")
from bronze_ingestion import ingest_bronze
from silver_processing import process_silver
from gold_aggregation import process_gold

DATE, RUN_ID = "2025-07-23", "local-test-001"
ingest_bronze(DATE, RUN_ID)
process_silver(DATE, RUN_ID)
process_gold(DATE, RUN_ID)
print("Done — check ./data/")
EOF
```

---

## Production deployment

1. Set `USE_LOCAL_FS=false`
2. Set `S3_BUCKET` to your data lake bucket
3. Set `ICEBERG_CATALOG_TYPE=glue` (AWS Glue catalog)
4. Set `PIPELINE_DB_CONN` to your PostgreSQL connection string
5. Configure `aws_default` Airflow connection with appropriate IAM role
6. Install `pyiceberg[glue,s3]` instead of `pyiceberg[sql-sqlite]`
