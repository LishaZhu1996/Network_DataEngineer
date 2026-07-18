"""
Gold layer aggregations.

Reads silver_clean and produces three Iceberg tables:
  - gold.fact_tower_daily_snapshot : one row per (tower_id, region, date)
  - gold.network_metrics_hourly    : aggregated per region per hour
  - gold.network_metrics_daily     : aggregated per region per day

For backfill / reload, existing rows for the execution_date are deleted
before new data is inserted (via overwrite_filter in write_iceberg).
"""
import logging
import os

import pandas as pd

from silver_processing import read_silver_clean
from config import USE_LOCAL, GOLD_PATH, S3_BUCKET, S3_GOLD_PREFIX

logger = logging.getLogger(__name__)


def process_gold(execution_date: str, run_id: str) -> dict:
    """Returns {"fact_records": int, "hourly_records": int, "daily_records": int}."""
    df = read_silver_clean(execution_date)

    fact    = _build_fact_daily_snapshot(df, execution_date)
    hourly  = _build_hourly(df, execution_date)
    daily   = _build_daily(df, execution_date)

    _write("fact_tower_daily_snapshot", fact,   execution_date)
    _write("network_metrics_hourly",    hourly, execution_date)
    _write("network_metrics_daily",     daily,  execution_date)

    logger.info(
        "Gold complete — fact=%d, hourly=%d, daily=%d",
        len(fact), len(hourly), len(daily),
    )
    return {
        "fact_records":   len(fact),
        "hourly_records": len(hourly),
        "daily_records":  len(daily),
    }


# ─── builders ─────────────────────────────────────────────────────────────────

def _build_fact_daily_snapshot(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """
    One row per (tower_id, region) per day — full performance snapshot.
    Captures signal distribution (avg/min/max/stddev) and total throughput.
    """
    fact = (
        df.groupby(["tower_id", "region"], as_index=False)
        .agg(
            avg_signal_strength  =("signal_strength", "mean"),
            min_signal_strength  =("signal_strength", "min"),
            max_signal_strength  =("signal_strength", "max"),
            stddev_signal_strength=("signal_strength", "std"),
            total_data_volume_mb =("data_volume_mb",  "sum"),
            avg_data_volume_mb   =("data_volume_mb",  "mean"),
            reading_count        =("timestamp",       "count"),
        )
        .round({
            "avg_signal_strength":   4,
            "min_signal_strength":   4,
            "max_signal_strength":   4,
            "stddev_signal_strength":4,
            "total_data_volume_mb":  2,
            "avg_data_volume_mb":    2,
        })
    )
    fact["execution_date"] = execution_date
    return fact.sort_values(["tower_id", "region"]).reset_index(drop=True)


def _build_hourly(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    hourly = (
        df.groupby(["region", "event_hour"], as_index=False)
        .agg(
            total_data_volume_mb=("data_volume_mb",  "sum"),
            avg_signal_strength =("signal_strength", "mean"),
            tower_count         =("tower_id",        "nunique"),
            record_count        =("timestamp",       "count"),
        )
        .round({"total_data_volume_mb": 2, "avg_signal_strength": 4})
    )
    hourly["execution_date"] = execution_date
    return hourly.sort_values(["region", "event_hour"]).reset_index(drop=True)


def _build_daily(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    daily = (
        df.groupby("region", as_index=False)
        .agg(
            total_data_volume_mb=("data_volume_mb",  "sum"),
            avg_signal_strength =("signal_strength", "mean"),
            tower_count         =("tower_id",        "nunique"),
            record_count        =("timestamp",       "count"),
        )
        .round({"total_data_volume_mb": 2, "avg_signal_strength": 4})
    )
    daily["execution_date"] = execution_date
    return daily.sort_values("region").reset_index(drop=True)


# ─── write ─────────────────────────────────────────────────────────────────────

def _write(table_name: str, df: pd.DataFrame, execution_date: str):
    if USE_LOCAL:
        out_dir = os.path.join(GOLD_PATH, table_name)
        os.makedirs(out_dir, exist_ok=True)
        df.to_parquet(
            os.path.join(out_dir, f"{table_name}_{execution_date}.parquet"),
            index=False,
        )
        return

    from utils import write_iceberg
    write_iceberg(
        df,
        namespace="gold",
        table_name=table_name,
        partition_cols=["execution_date"],
        overwrite_filter={"execution_date": execution_date},
    )
