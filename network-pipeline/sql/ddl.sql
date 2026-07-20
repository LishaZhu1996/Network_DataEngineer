-- =============================================================================
-- Network Metrics Pipeline — Reference DDL
-- All tables are managed as Iceberg tables in production (AWS Glue catalog).
-- This file documents the logical schemas; actual table creation is handled
-- by PyIceberg on first write (catalog.create_table).
-- =============================================================================

-- ─── Control Table (PostgreSQL / SQLite) ─────────────────────────────────────
-- Not an Iceberg table — lives in the pipeline metadata database.
CREATE TABLE IF NOT EXISTS pipeline_control (
    id                  SERIAL          PRIMARY KEY,
    run_id              VARCHAR(255)    NOT NULL,
    table_name          VARCHAR(255)    NOT NULL,
    execution_date      DATE,
    file_name           VARCHAR(500),
    start_datetime      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finish_datetime     TIMESTAMP,
    status              VARCHAR(50)     NOT NULL DEFAULT 'running',  -- running | success | fail | skipped
    error_message       TEXT,
    records_processed   INTEGER,
    UNIQUE (run_id, table_name, execution_date)
);

-- ─── Bronze Layer — Iceberg ───────────────────────────────────────────────────
-- s3://raw-telecom-network-data/warehouse/bronze/network_metrics_raw/
CREATE TABLE IF NOT EXISTS bronze.network_metrics_raw (
    tower_id            INTEGER,
    region              VARCHAR(100),
    timestamp           BIGINT,
    signal_strength     DOUBLE,
    data_volume_mb      DOUBLE,
    -- pipeline metadata
    _run_id             VARCHAR(255),
    _source_file        VARCHAR(500),
    _ingested_at        VARCHAR(30),
    _execution_date     VARCHAR(10)
)
USING iceberg
PARTITIONED BY (_execution_date);

-- ─── Silver Layer — Iceberg ───────────────────────────────────────────────────
-- s3://raw-telecom-network-data/warehouse/silver/network_metrics_clean/
CREATE TABLE IF NOT EXISTS silver.network_metrics_clean (
    tower_id            INTEGER         NOT NULL,
    region              VARCHAR(100)    NOT NULL,
    timestamp           BIGINT          NOT NULL,
    signal_strength     DOUBLE,
    data_volume_mb      DOUBLE,
    event_datetime      VARCHAR(30),
    event_date          VARCHAR(10),
    event_hour          INTEGER,
    _execution_date     VARCHAR(10),
    _run_id             VARCHAR(255),
    _source_file        VARCHAR(500),
    _ingested_at        VARCHAR(30)
)
USING iceberg
PARTITIONED BY (_execution_date);

-- ─── Gold Layer — Iceberg ─────────────────────────────────────────────────────

-- One row per tower per day — full performance snapshot
-- s3://raw-telecom-network-data/warehouse/gold/fact_tower_daily_snapshot/
CREATE TABLE IF NOT EXISTS gold.fact_tower_daily_snapshot (
    tower_id                INTEGER         NOT NULL,
    region                  VARCHAR(100)    NOT NULL,
    execution_date          VARCHAR(10)     NOT NULL,
    avg_signal_strength     DOUBLE,
    min_signal_strength     DOUBLE,
    max_signal_strength     DOUBLE,
    stddev_signal_strength  DOUBLE,
    total_data_volume_mb    DOUBLE,
    avg_data_volume_mb      DOUBLE,
    reading_count           INTEGER
)
USING iceberg
PARTITIONED BY (execution_date);

-- Hourly aggregation per region
-- s3://raw-telecom-network-data/warehouse/gold/network_metrics_hourly/
CREATE TABLE IF NOT EXISTS gold.network_metrics_hourly (
    region                  VARCHAR(100)    NOT NULL,
    event_hour              INTEGER         NOT NULL,
    total_data_volume_mb    DOUBLE          NOT NULL,
    avg_signal_strength     DOUBLE          NOT NULL,
    tower_count             INTEGER,
    record_count            INTEGER,
    execution_date          VARCHAR(10)     NOT NULL
)
USING iceberg
PARTITIONED BY (execution_date);

-- Daily summary per region
-- s3://raw-telecom-network-data/warehouse/gold/network_metrics_daily/
CREATE TABLE IF NOT EXISTS gold.network_metrics_daily (
    region                  VARCHAR(100)    NOT NULL,
    total_data_volume_mb    DOUBLE          NOT NULL,
    avg_signal_strength     DOUBLE          NOT NULL,
    tower_count             INTEGER,
    record_count            INTEGER,
    execution_date          VARCHAR(10)     NOT NULL
)
USING iceberg
PARTITIONED BY (execution_date);

-- ─── Data Quality Log — Iceberg ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline.dq_log (
    run_id          VARCHAR(255),
    execution_date  VARCHAR(10),
    check_name      VARCHAR(100),
    status          VARCHAR(20),    -- pass | warn | fail
    message         TEXT,
    failed_rows     INTEGER,
    checked_at      VARCHAR(30)
)
USING iceberg
PARTITIONED BY (execution_date);
