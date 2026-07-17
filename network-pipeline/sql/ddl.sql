-- =============================================================================
-- Network Metrics Pipeline — DDL
-- =============================================================================

-- ─── Control Table ────────────────────────────────────────────────────────────
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
    UNIQUE (run_id, table_name)
);

-- ─── Bronze Layer ─────────────────────────────────────────────────────────────
-- Stored as Parquet on S3: s3://raw-telecom-network-data/bronze/network_metrics/{date}/data.parquet
-- Shown here as a reference schema only.
CREATE TABLE IF NOT EXISTS bronze.network_metrics_raw (
    tower_id            INTEGER,
    region              VARCHAR(100),
    timestamp           BIGINT,
    signal_strength     FLOAT,
    data_volume_mb      FLOAT,
    -- pipeline metadata
    _run_id             VARCHAR(255),
    _source_file        VARCHAR(500),
    _ingested_at        TIMESTAMP,
    _execution_date     DATE
);

-- ─── Silver Layer ─────────────────────────────────────────────────────────────

-- Cleaned measurements (fact table, all individual readings)
CREATE TABLE IF NOT EXISTS silver.network_metrics_clean (
    tower_id            INTEGER         NOT NULL,
    region              VARCHAR(100)    NOT NULL,
    timestamp           BIGINT          NOT NULL,
    signal_strength     FLOAT,
    data_volume_mb      FLOAT,
    event_datetime      TIMESTAMP,          -- converted from unix ts
    event_date          DATE,
    event_hour          SMALLINT,
    _execution_date     DATE,
    _run_id             VARCHAR(255),
    _source_file        VARCHAR(500),
    _ingested_at        TIMESTAMP,
    PRIMARY KEY (tower_id, region, timestamp)
);

-- SCD Type 2 history table — one row per version of each tower+region combination
CREATE TABLE IF NOT EXISTS silver.dim_tower_region_history (
    surrogate_key           SERIAL          PRIMARY KEY,
    hash_key                CHAR(32)        NOT NULL,   -- MD5(tower_id | region)
    hash_diff               CHAR(32)        NOT NULL,   -- MD5(avg_signal_strength | total_data_volume_mb)
    tower_id                INTEGER         NOT NULL,
    region                  VARCHAR(100)    NOT NULL,
    avg_signal_strength     FLOAT,
    total_data_volume_mb    FLOAT,
    record_count            INTEGER,
    snapshot_date           DATE,
    -- SCD2 validity window
    effective_from          DATE            NOT NULL,
    effective_to            DATE,                       -- NULL = currently active
    is_current              BOOLEAN         NOT NULL DEFAULT TRUE,
    dw_created_at           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dim_history_hash_key     ON silver.dim_tower_region_history (hash_key);
CREATE INDEX IF NOT EXISTS idx_dim_history_is_current   ON silver.dim_tower_region_history (is_current);

-- Current view — only latest records (is_current = TRUE)
CREATE VIEW silver.dim_tower_region_current AS
    SELECT *
    FROM   silver.dim_tower_region_history
    WHERE  is_current = TRUE;

-- ─── Gold Layer ───────────────────────────────────────────────────────────────

-- Hourly aggregation
CREATE TABLE IF NOT EXISTS gold.network_metrics_hourly (
    region                  VARCHAR(100)    NOT NULL,
    event_hour              SMALLINT        NOT NULL,
    total_data_volume_mb    FLOAT           NOT NULL,
    avg_signal_strength     FLOAT           NOT NULL,
    tower_count             INTEGER,
    record_count            INTEGER,
    execution_date          DATE            NOT NULL,
    PRIMARY KEY (region, event_hour, execution_date)
);

-- Daily summary report
CREATE TABLE IF NOT EXISTS gold.network_metrics_daily (
    region                  VARCHAR(100)    NOT NULL,
    total_data_volume_mb    FLOAT           NOT NULL,
    avg_signal_strength     FLOAT           NOT NULL,
    tower_count             INTEGER,
    record_count            INTEGER,
    execution_date          DATE            NOT NULL,
    PRIMARY KEY (region, execution_date)
);

-- ─── Data Quality Log ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dq_log (
    id              SERIAL          PRIMARY KEY,
    run_id          VARCHAR(255),
    execution_date  DATE,
    check_name      VARCHAR(100),
    status          VARCHAR(20),    -- pass | warn | fail
    message         TEXT,
    failed_rows     INTEGER,
    checked_at      TIMESTAMP
);
