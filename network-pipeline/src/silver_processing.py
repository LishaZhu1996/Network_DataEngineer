"""
Silver layer processing.

Steps:
  1. Load bronze data.
  2. Run data quality checks — log results; abort on hard failures.
  3. Clean and enrich:
       - Cast types.
       - Convert unix timestamp → UTC datetime.
       - Drop duplicates on (tower_id, region, timestamp).
       - Drop rows with null key fields.
  4. Write silver.network_metrics_clean as an Iceberg table.
"""
import json
import logging
import os

import pandas as pd

from bronze_ingestion import read_bronze
from config import (
    USE_LOCAL, SILVER_PATH, DQ_LOG_PATH,
    S3_BUCKET, S3_SILVER_PREFIX,
    source_file_name,
)
from utils.data_quality import run_all_checks, has_failures

logger = logging.getLogger(__name__)


def process_silver(execution_date: str, run_id: str) -> dict:
    """
    Returns {"clean_records": int, "dq_failures": int}
    Raises RuntimeError on hard DQ failures.
    """
    df_bronze = read_bronze(execution_date)

    # ── Data quality ──────────────────────────────────────────────────────────
    dq_results = run_all_checks(df_bronze, execution_date)
    _write_dq_log(dq_results, execution_date, run_id)

    if has_failures(dq_results):
        failed = [r.check_name for r in dq_results if r.status == "fail"]
        raise RuntimeError(f"Hard DQ failures: {failed}")

    # ── Clean ─────────────────────────────────────────────────────────────────
    df = _clean(df_bronze, execution_date, run_id)

    # ── Write silver_clean ────────────────────────────────────────────────────
    _write_silver_clean(df, execution_date)
    logger.info("Silver clean complete — %d rows", len(df))

    return {
        "clean_records": len(df),
        "dq_failures": sum(1 for r in dq_results if r.status == "fail"),
    }


def read_silver_clean(execution_date: str) -> pd.DataFrame:
    """Load silver_clean for a given execution_date (used by gold layer)."""
    if USE_LOCAL:
        path = os.path.join(SILVER_PATH, "clean", f"network_metrics_{execution_date}.parquet")
        return pd.read_parquet(path)

    from utils import read_iceberg
    return read_iceberg("silver", "network_metrics_clean",
                        filters={"_execution_date": execution_date})


# ─── private helpers ──────────────────────────────────────────────────────────

def _clean(df: pd.DataFrame, execution_date: str, run_id: str) -> pd.DataFrame:
    df = df.copy()

    df["tower_id"]       = pd.to_numeric(df["tower_id"], errors="coerce").astype("Int64")
    df["signal_strength"] = pd.to_numeric(df["signal_strength"], errors="coerce")
    df["data_volume_mb"] = pd.to_numeric(df["data_volume_mb"], errors="coerce")

    df["event_datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["event_date"]     = df["event_datetime"].dt.date.astype(str)
    df["event_hour"]     = df["event_datetime"].dt.hour

    df = df.dropna(subset=["tower_id", "region", "timestamp"])
    df = df.drop_duplicates(subset=["tower_id", "region", "timestamp"], keep="first")

    df["_execution_date"] = execution_date
    df["_run_id"]         = run_id

    return df.reset_index(drop=True)


def _write_silver_clean(df: pd.DataFrame, execution_date: str):
    if USE_LOCAL:
        out_dir = os.path.join(SILVER_PATH, "clean")
        os.makedirs(out_dir, exist_ok=True)
        df.to_parquet(os.path.join(out_dir, f"network_metrics_{execution_date}.parquet"), index=False)
        return

    from utils import write_iceberg
    write_iceberg(
        df,
        namespace="silver",
        table_name="network_metrics_clean",
        overwrite_filter={"_execution_date": execution_date},
    )


def _write_dq_log(results, execution_date: str, run_id: str):
    records = [r.to_dict() for r in results]
    for r in records:
        r["execution_date"] = execution_date
        r["run_id"] = run_id

    if USE_LOCAL:
        os.makedirs(DQ_LOG_PATH, exist_ok=True)
        path = os.path.join(DQ_LOG_PATH, f"dq_log_{execution_date}.json")
        with open(path, "w") as f:
            json.dump(records, f, indent=2)
