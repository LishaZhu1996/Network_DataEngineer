"""
Centralised configuration for the network-metrics pipeline.

Importable as `from config import X` from any module inside src/ (since src/
is on sys.path), regardless of whether the module is run directly as a script
or via the package (python -m src.module).
"""
import os

# ── Storage mode ───────────────────────────────────────────────────────────────
USE_LOCAL: bool = os.getenv("USE_LOCAL_FS", "false").lower() == "true"

# ── Local paths ────────────────────────────────────────────────────────────────
BASE_DATA_PATH    = os.getenv("BASE_DATA_PATH", "./data")
SOURCE_PATH       = os.getenv("LOCAL_SOURCE_PATH", "./sample_data")
BRONZE_PATH       = os.path.join(BASE_DATA_PATH, "bronze")
SILVER_PATH       = os.path.join(BASE_DATA_PATH, "silver")
GOLD_PATH         = os.path.join(BASE_DATA_PATH, "gold")
DQ_LOG_PATH       = os.path.join(BASE_DATA_PATH, "dq_logs")
ICEBERG_WAREHOUSE = os.path.join(BASE_DATA_PATH, "warehouse")

# ── S3 ─────────────────────────────────────────────────────────────────────────
S3_BUCKET        = os.getenv("S3_BUCKET", "raw-telecom-network-data")
S3_BRONZE_PREFIX = os.getenv("S3_BRONZE_PREFIX", "bronze/network_metrics")
S3_SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "silver")
S3_GOLD_PREFIX   = os.getenv("S3_GOLD_PREFIX", "gold")

# ── Iceberg catalog ────────────────────────────────────────────────────────────
# Local  → SqlCatalog backed by SQLite
# Cloud  → set ICEBERG_CATALOG_TYPE=glue and ICEBERG_CATALOG_URI to Glue endpoint
ICEBERG_CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
ICEBERG_CATALOG_URI  = os.getenv(
    "ICEBERG_CATALOG_URI",
    f"sqlite:///{os.path.abspath(os.path.join(BASE_DATA_PATH, 'iceberg_catalog.db'))}",
)

# ── Database (control table) ───────────────────────────────────────────────────
DB_CONN     = os.getenv("PIPELINE_DB_CONN", f"sqlite:///{os.path.abspath(os.path.join(BASE_DATA_PATH, 'pipeline_control.db'))}")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")

# ── Source file naming ─────────────────────────────────────────────────────────
def source_file_name(execution_date: str) -> str:
    """e.g. '2025-07-23' → 'network_metrics_20250723.csv'"""
    return f"network_metrics_{execution_date.replace('-', '')}.csv"

# ── Control table — tracked layers per run ─────────────────────────────────────
PIPELINE_TABLES = [
    "bronze.network_metrics_raw",
    "silver.network_metrics_clean",
    "gold.fact_tower_daily_snapshot",
    "gold.network_metrics_hourly",
    "gold.network_metrics_daily",
]
