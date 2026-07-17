"""
Gold layer aggregations.

Reads silver_clean and produces:
  - gold_hourly  : total_data_volume_mb + avg_signal_strength per region per hour
  - gold_daily   : total_data_volume_mb + avg_signal_strength per region for the day
"""
import io
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

USE_LOCAL = os.getenv("USE_LOCAL_FS", "true").lower() == "true"
LOCAL_SILVER_CLEAN = os.getenv("LOCAL_SILVER_PATH", "./data/silver") + "/clean"
LOCAL_GOLD_PATH = os.getenv("LOCAL_GOLD_PATH", "./data/gold")
S3_BUCKET = os.getenv("S3_BUCKET", "raw-telecom-network-data")
S3_GOLD_PREFIX = os.getenv("S3_GOLD_PREFIX", "gold")


def process_gold(execution_date: str, run_id: str) -> dict:
    """Returns {"hourly_records": int, "daily_records": int}."""
    df = _load_silver_clean(execution_date)

    hourly = _aggregate_hourly(df, execution_date)
    daily = _aggregate_daily(df, execution_date)

    _write_parquet(hourly, LOCAL_GOLD_PATH + "/hourly", f"hourly_{execution_date}.parquet",
                   f"{S3_GOLD_PREFIX}/hourly/{execution_date}/data.parquet")
    _write_parquet(daily, LOCAL_GOLD_PATH + "/daily", f"daily_{execution_date}.parquet",
                   f"{S3_GOLD_PREFIX}/daily/{execution_date}/data.parquet")

    logger.info("Gold complete — %d hourly rows, %d daily rows", len(hourly), len(daily))
    return {"hourly_records": len(hourly), "daily_records": len(daily)}


def _load_silver_clean(execution_date: str) -> pd.DataFrame:
    if USE_LOCAL:
        path = os.path.join(LOCAL_SILVER_CLEAN, f"network_metrics_{execution_date}.parquet")
        return pd.read_parquet(path)
    import boto3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"silver/clean/{execution_date}/data.parquet")
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _aggregate_hourly(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    hourly = (
        df.groupby(["region", "event_hour"], as_index=False)
        .agg(
            total_data_volume_mb=("data_volume_mb", "sum"),
            avg_signal_strength=("signal_strength", "mean"),
            tower_count=("tower_id", "nunique"),
            record_count=("timestamp", "count"),
        )
        .round({"total_data_volume_mb": 2, "avg_signal_strength": 4})
    )
    hourly["execution_date"] = execution_date
    hourly["run_id"] = ""  # filled by caller if needed
    return hourly.sort_values(["region", "event_hour"]).reset_index(drop=True)


def _aggregate_daily(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    daily = (
        df.groupby("region", as_index=False)
        .agg(
            total_data_volume_mb=("data_volume_mb", "sum"),
            avg_signal_strength=("signal_strength", "mean"),
            tower_count=("tower_id", "nunique"),
            record_count=("timestamp", "count"),
        )
        .round({"total_data_volume_mb": 2, "avg_signal_strength": 4})
    )
    daily["execution_date"] = execution_date
    return daily.sort_values("region").reset_index(drop=True)


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
