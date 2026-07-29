# Network Metrics Pipeline

End-to-end daily ingestion pipeline for cell-tower network metrics.  
Implements a **Bronze → Silver → Gold** medallion architecture orchestrated with Apache Airflow, with all output tables written as **Apache Iceberg** tables.

---

## Assumption
- The file will arrive at S3 everyday before 2am.
- It can happen that the destination doesn't have new file for specific day, but as long as the file is detected, it should not be empty (should have some data for that day).
- For a given timestamp, the natrue primary key should be the combination of tower_id + region.
- Noraml range of signal strenth is (-120.0 , -40.0) and data volumne should be larger than 0.
- Notification needed when all of the retries failed.

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

All gold and silver tables are **Iceberg tables** — time-travel and partition pruning come for free. Bronze is Parquet locally and Iceberg on S3.

---

## Project structure

```
network-pipeline/
├── dags/
│   └── network_metrics_dag.py          # Airflow DAG for orchastration (daily + backfill)
├── src/
│   ├── __init__.py
│   ├── config.py                       # Centralised config (paths, env vars, table names)
│   ├── bronze_ingestion.py
│   ├── silver_processing.py
│   ├── gold_aggregation.py
│   └── utils/
│       ├── __init__.py                 # Iceberg catalog helpers (get_catalog, write_iceberg, read_iceberg)
│       ├── control_table.py
│       └── data_quality.py
├── tests/
│   ├── conftest.py                     # sys.path setup + env defaults for all tests
│   ├── test_data_quality.py            # 25 tests — all 8 DQ checks (incl. schema evolution)
│   ├── test_silver_processing.py       # 11 tests — _clean() logic
│   ├── test_gold_aggregation.py        # 21 tests — all 3 gold builders
│   └── test_control_table.py           # 9 tests — control table CRUD
├── sql/
│   └── ddl.sql                         # Reference DDL — schemas for all Iceberg tables
├── mock_tables/                        # Sample output CSVs (reference / documentation only)
├── sample_data/
│   └── network_metrics_20250723.csv
├── requirements.txt
└── README.md
```

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
    │  Schema evolution check:                │
    │    type change  → abort (RuntimeError)  │
    │    new/less column → warn, auto-add/drop│
    │    write bronze Iceberg table           │
    │                                    end_pipeline
run_dq_checks                                 ▲
    │                                         │
    │  7 checks against bronze data:          │
    │    - not_empty                          │
    │    - schema_columns (required cols)     │
    │    - null_check                         │
    │    - signal_range                       │
    │    - data_volume_positive               │
    │    - timestamp_freshness                │
    │    - duplicate_rows                     │
    │  Writes DQ log; aborts on fail          │
    │                                         │
process_silver                                │
    │                                         │
    │  Cast types, drop nulls/dupes           │
    │  Convert unix ts → UTC datetime         │
    │  Evolve + write silver Iceberg table    │
    │  (partitioned by _execution_date)       │
    │                                         │
process_gold                                  │
    │                                         │
    │  fact_tower_daily_snapshot              │
    │  network_metrics_hourly                 │
    │  network_metrics_daily                  │
    │  (all Iceberg, partitioned by           │
    │   execution_date)                       │
    │                                         │
mark_success                                  │
    │                                         │
    │  Update control table → 'success'       │
    │                                         │
end_pipeline ─────────────────────────────────┘

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
If some issue happens and fail the pipeline, the number of the retry is 3 and retry window is 5 mins.
If Dag run multiple times per day, idempotent logic is applied using `overwrite_filter`. 

### Backfill / reload
Trigger with a `start_date` + `end_date` in the conf to reload a date range:

```bash
airflow dags trigger network_metrics_pipeline \
  --conf '{"start_date": "2025-07-01", "end_date": "2025-07-23"}'
```

Backfill behaviour:
- Each date in the range is processed independently.
- If the source file is **missing** for a date → that date is skipped and logged; other dates continue.
- Iceberg tables have existing rows for those dates **overwrite** — fully idempotent.
- Control table gets one row per `(table, date)` for full auditability.

---

## Alerting Strategy

Slack notifications are sent via `_on_failure` callback and `_on_sla_miss` callback. Only one notification tool is used — no PagerDuty.

### When Slack fires

| Scenario | Trigger | Channel |
|----------|---------|---------|
| All retries exhausted (`try_number >= max_tries`) | `on_failure_callback` | `#data-incidents` |
| Pipeline not completed within 3 hours of schedule | `sla_miss_callback` | `#data-incidents` |

**Intermediate retries are intentionally silent** — transient failures (network blip, S3 throttle) often self-heal. The team is only notified once the task has definitively failed after all 3 retries.

### Slack message includes

```
:red_circle: Pipeline failure — all retries exhausted
> DAG:      network_metrics_pipeline
> Task:     process_silver
> Date:     2025-07-23
> Run ID:   scheduled__2025-07-23T02:00:00+00:00
> Attempts: 4
> Error:    RuntimeError: Hard DQ failures: ['not_empty']
> Logs:     https://airflow.internal/log?dag_id=...
```

---

## Data Ingestion/Models

### Schema evolution

The pipeline detects and handles source schema changes automatically:

| Change type | DQ severity | Write-layer behaviour |
|-------------|-------------|----------------------|
| New column added in source | `warn` | `write_iceberg` calls `update_schema().add_column()` before appending — existing rows show `NULL` for the new column |
| Column dropped from source | `warn` | Iceberg column retained; new rows write `NULL` for that column |
| Column type changed (e.g. float→string) | `fail` | Pipeline aborts — prevents data corruption in existing Iceberg rows |

The detection runs in `_check_schema_evolution` (DQ layer) and the auto-evolve runs in `_evolve_schema` inside `write_iceberg` (utils layer) — catching it in DQ first gives a clear alert before any write is attempted.

---

### Data quality checks 

| Check | Severity | Action on failure |
|-------|----------|-------------------|
| File not empty | `fail` | Abort |
| Required columns present | `fail` | Abort |
| Null in key columns (tower_id, region, timestamp) | `warn` | Log |
| `signal_strength` in [-120, -40] dBm | `warn` | Log |
| `data_volume_mb ≥ 0` | `warn` | Log |
| Timestamp within file date | `warn` | Log |
| Duplicate rows on (tower_id, region, timestamp) | `warn` | Drop in silver |

Results written to `./data/dq_logs/dq_log_{date}.json`.
When doing the DQ check, if any `'fail'` DQ check detected, the dag will be failed.

---

### Bronze — Raw Copy

**Model: Append-only flat, partitioned, no transformation**

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

**Model: Append-based, partitioned, cleaned, enriched and typed**

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

**Three partitoned tables, each at a different grain:**

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

## Improvement
- In real practice, split ingestion pipeline and data model. We may have more than one table to ingestion and process, so in that case, we can create metadata driven solution by having a meta data json file/table for the ingestion. 
- Use for example, dbt for data model running. So if one of the script failed, the downsteam indepent script will still running and we won't miss the data for that part. Also while applying orchestrator functionality, the etl model will run after the raw table got updated. 
- Also to have a raw data DQ report, having the data volume we received comparison by time to validate the data we received and identify the potention issue. 
- Build the functionality to inform related team for the data quality issue we identified everyday. 
