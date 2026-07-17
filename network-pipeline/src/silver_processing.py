"""
Silver layer processing.

Steps:
  1. Load bronze data.
  2. Run data quality checks — log results; abort on hard failures.
  3. Clean and enrich:
       - Cast types.
       - Convert unix timestamp → datetime.
       - Drop duplicates on (tower_id, region, timestamp).
       - Filter rows that fail hard DQ rules.
  4. Write silver_clean (all cleaned measurements, partitioned by date).
  5. Apply SCD Type 2 on dim_tower_region:
       - Aggregate to one representative row per (tower_id, region) per day.
       - Compute hash_key / hash_diff.
       - Compare against existing current records.
       - Expire changed rows, insert new/changed rows.
       - Write dim_tower_region_history and refresh dim_tower_region_current.
"""
import io
import json
import logging
import os

import pandas as pd

from bronze_ingestion import read_bronze
from utils.data_quality import run_all_checks, has_failures
from utils.scd2 import compute_hashes, apply_scd2

logger = logging.getLogger(__name__)

USE_LOCAL = os.getenv("USE_LOCAL_FS", "true").lower() == "true"
LOCAL_SILVER_PATH = os.getenv("LOCAL_SILVER_PATH", "./data/silver")
LOCAL_DQ_LOG_PATH = os.getenv("LOCAL_DQ_LOG_PATH", "./data/dq_logs")
S3_BUCKET = os.getenv("S3_BUCKET", "raw-telecom-network-data")
S3_SILVER_PREFIX = os.getenv("S3_SILVER_PREFIX", "silver")


def process_silver(execution_date: str, run_id: str) -> dict:
    """
    Returns {"clean_records": int, "scd2_inserted": int, "scd2_expired": int, "dq_failures": int}
    Raises RuntimeError if hard DQ failures are found.
    """
    df_bronze = read_bronze(execution_date)

    # ── Data quality ─────────────────────────────────────────────────────────
    dq_results = run_all_checks(df_bronze, execution_date)
    _write_dq_log(dq_results, execution_date, run_id)

    if has_failures(dq_results):
        failed = [r.check_name for r in dq_results if r.status == "fail"]
        raise RuntimeError(f"Hard DQ failures: {failed}")

    # ── Clean ─────────────────────────────────────────────────────────────────
    df = _clean(df_bronze, execution_date)

    # ── Write silver_clean ───────────────────────────────────────────────────
    _write_parquet(df, f"{LOCAL_SILVER_PATH}/clean", f"network_metrics_{execution_date}.parquet",
                   f"{S3_SILVER_PREFIX}/clean/{execution_date}/data.parquet")
    logger.info("Wrote silver_clean — %d rows", len(df))

    # ── SCD2 dim_tower_region ────────────────────────────────────────────────
    daily_agg = _aggregate_for_scd2(df, execution_date)
    daily_agg = compute_hashes(daily_agg)

    existing_current = _load_dim_current()
    to_expire, to_insert = apply_scd2(daily_agg, existing_current, execution_date)

    _update_dim_history(to_expire, to_insert, execution_date)

    logger.info(
        "SCD2 complete — %d inserted, %d expired",
        len(to_insert), len(to_expire),
    )
    return {
        "clean_records": len(df),
        "scd2_inserted": len(to_insert),
        "scd2_expired": len(to_expire),
        "dq_failures": sum(1 for r in dq_results if r.status == "fail"),
    }


# ─── helpers ─────────────────────────────────────────────────────────────────

def _clean(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    df = df.copy()

    # Cast types
    df["tower_id"] = pd.to_numeric(df["tower_id"], errors="coerce").astype("Int64")
    df["signal_strength"] = pd.to_numeric(df["signal_strength"], errors="coerce")
    df["data_volume_mb"] = pd.to_numeric(df["data_volume_mb"], errors="coerce")

    # Convert unix timestamp → datetime UTC
    df["event_datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["event_date"] = df["event_datetime"].dt.date.astype(str)
    df["event_hour"] = df["event_datetime"].dt.hour

    # Drop rows with null key fields
    df = df.dropna(subset=["tower_id", "region", "timestamp"])

    # Drop exact duplicates on natural key
    df = df.drop_duplicates(subset=["tower_id", "region", "timestamp"], keep="first")

    # Add silver metadata
    df["_execution_date"] = execution_date

    return df.reset_index(drop=True)


def _aggregate_for_scd2(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """Latest daily snapshot per (tower_id, region) used for SCD2 tracking."""
    agg = (
        df.groupby(["tower_id", "region"], as_index=False)
        .agg(
            avg_signal_strength=("signal_strength", "mean"),
            total_data_volume_mb=("data_volume_mb", "sum"),
            record_count=("timestamp", "count"),
        )
        .round({"avg_signal_strength": 4, "total_data_volume_mb": 4})
    )
    agg["snapshot_date"] = execution_date
    return agg


def _load_dim_current() -> pd.DataFrame:
    path = os.path.join(LOCAL_SILVER_PATH, "dim_tower_region_history.parquet")
    if USE_LOCAL:
        if not os.path.exists(path):
            return pd.DataFrame()
        df = pd.read_parquet(path)
        return df[df["is_current"] == True] if not df.empty else pd.DataFrame()

    import boto3
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_SILVER_PREFIX}/dim_tower_region_history/latest.parquet")
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        return df[df["is_current"] == True]
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()


def _update_dim_history(to_expire: pd.DataFrame, to_insert: pd.DataFrame, execution_date: str):
    history_path = os.path.join(LOCAL_SILVER_PATH, "dim_tower_region_history.parquet")

    if USE_LOCAL:
        os.makedirs(LOCAL_SILVER_PATH, exist_ok=True)

        # Load existing history
        if os.path.exists(history_path):
            existing = pd.read_parquet(history_path)
        else:
            existing = pd.DataFrame()

        if not to_expire.empty and not existing.empty:
            # Apply expirations
            for _, row in to_expire.iterrows():
                mask = (existing["hash_key"] == row["hash_key"]) & (existing["is_current"] == True)
                existing.loc[mask, "effective_to"] = row["effective_to"]
                existing.loc[mask, "is_current"] = False

        if not to_insert.empty:
            existing = pd.concat([existing, to_insert], ignore_index=True)

        existing.to_parquet(history_path, index=False)

        # Refresh current view (latest records only)
        current_path = os.path.join(LOCAL_SILVER_PATH, "dim_tower_region_current.parquet")
        if not existing.empty:
            current = existing[existing["is_current"] == True]
            current.to_parquet(current_path, index=False)


def _write_parquet(df: pd.DataFrame, local_dir: str, local_name: str, s3_key: str):
    if USE_LOCAL:
        os.makedirs(local_dir, exist_ok=True)
        df.to_parquet(os.path.join(local_dir, local_name), index=False)
        return
    import boto3
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.getvalue())


def _write_dq_log(results, execution_date: str, run_id: str):
    records = [r.to_dict() for r in results]
    for r in records:
        r["execution_date"] = execution_date
        r["run_id"] = run_id

    if USE_LOCAL:
        os.makedirs(LOCAL_DQ_LOG_PATH, exist_ok=True)
        path = os.path.join(LOCAL_DQ_LOG_PATH, f"dq_log_{execution_date}.json")
        with open(path, "w") as f:
            json.dump(records, f, indent=2)
