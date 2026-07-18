"""
Data quality checks run against the bronze DataFrame before silver processing.
Each check appends a structured record to dq_log; failures are collected and
returned — the caller decides whether to raise or warn.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"tower_id", "region", "timestamp", "signal_strength", "data_volume_mb"}
EXPECTED_DTYPES = {
    "tower_id": "int",
    "region": "object",
    "timestamp": "int",
    "signal_strength": "float",
    "data_volume_mb": "float",
}
SIGNAL_MIN, SIGNAL_MAX = -120.0, -40.0  # dBm valid range
DATA_VOL_MIN = 0.0


@dataclass
class DQResult:
    check_name: str
    status: str          # "pass" | "warn" | "fail"
    message: str
    failed_rows: int = 0
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "status": self.status,
            "message": self.message,
            "failed_rows": self.failed_rows,
            "checked_at": self.checked_at,
        }


def run_all_checks(df: pd.DataFrame, execution_date: str) -> List[DQResult]:
    results: List[DQResult] = []

    results.append(_check_not_empty(df))
    results.append(_check_schema(df))
    results.append(_check_nulls(df))
    results.append(_check_signal_range(df))
    results.append(_check_data_volume(df))
    results.append(_check_timestamp_freshness(df, execution_date))
    results.append(_check_duplicates(df))

    for r in results:
        level = logging.WARNING if r.status == "warn" else (logging.ERROR if r.status == "fail" else logging.INFO)
        logger.log(level, "[DQ] %-35s → %s | %s", r.check_name, r.status.upper(), r.message)

    return results


def has_failures(results: List[DQResult]) -> bool:
    return any(r.status == "fail" for r in results)


# ─── individual checks ────────────────────────────────────────────────────────

def _check_not_empty(df: pd.DataFrame) -> DQResult:
    if len(df) == 0:
        return DQResult("not_empty", "fail", "File contains zero rows.")
    return DQResult("not_empty", "pass", f"{len(df)} rows found.")


def _check_schema(df: pd.DataFrame) -> DQResult:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        return DQResult("schema_columns", "fail", f"Missing columns: {missing}")
    extra = set(df.columns) - REQUIRED_COLUMNS
    msg = "All required columns present."
    if extra:
        msg += f" Extra columns (ignored): {extra}"
    return DQResult("schema_columns", "pass", msg)


def _check_nulls(df: pd.DataFrame) -> DQResult:
    null_counts = df[list(REQUIRED_COLUMNS & set(df.columns))].isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if null_cols.empty:
        return DQResult("null_check", "pass", "No nulls in required columns.")
    total_nulls = int(null_cols.sum())
    detail = ", ".join(f"{c}={n}" for c, n in null_cols.items())
    return DQResult("null_check", "warn", f"Nulls found — {detail}", failed_rows=total_nulls)
    

def _check_signal_range(df: pd.DataFrame) -> DQResult:
    if "signal_strength" not in df.columns:
        return DQResult("signal_range", "warn", "Column signal_strength missing, skipped.")
    out_of_range = df[(df["signal_strength"] < SIGNAL_MIN) | (df["signal_strength"] > SIGNAL_MAX)]
    if len(out_of_range) == 0:
        return DQResult("signal_range", "pass", f"All values in [{SIGNAL_MIN}, {SIGNAL_MAX}] dBm.")
    return DQResult(
        "signal_range", "warn",
        f"{len(out_of_range)} rows outside expected range [{SIGNAL_MIN}, {SIGNAL_MAX}] dBm.",
        failed_rows=len(out_of_range),
    )


def _check_data_volume(df: pd.DataFrame) -> DQResult:
    if "data_volume_mb" not in df.columns:
        return DQResult("data_volume_positive", "warn", "Column data_volume_mb missing, skipped.")
    negatives = df[df["data_volume_mb"] < DATA_VOL_MIN]
    if len(negatives) == 0:
        return DQResult("data_volume_positive", "pass", "All data_volume_mb values ≥ 0.")
    return DQResult(
        "data_volume_positive", "fail",
        f"{len(negatives)} rows with negative data_volume_mb.",
        failed_rows=len(negatives),
    )


def _check_timestamp_freshness(df: pd.DataFrame, execution_date: str) -> DQResult:
    if "timestamp" not in df.columns:
        return DQResult("timestamp_freshness", "warn", "Column timestamp missing, skipped.")
    try:
        exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")
        exec_ts_min = int(exec_dt.replace(hour=0, minute=0, second=0).timestamp())
        exec_ts_max = int(exec_dt.replace(hour=23, minute=59, second=59).timestamp())
        out = df[(df["timestamp"] < exec_ts_min) | (df["timestamp"] > exec_ts_max)]
        if len(out) == 0:
            return DQResult("timestamp_freshness", "pass", f"All timestamps within {execution_date}.")
        return DQResult(
            "timestamp_freshness", "warn",
            f"{len(out)} rows have timestamps outside {execution_date}.",
            failed_rows=len(out),
        )
    except Exception as e:
        return DQResult("timestamp_freshness", "warn", f"Could not validate freshness: {e}")


def _check_duplicates(df: pd.DataFrame) -> DQResult:
    dupe_cols = [c for c in ["tower_id", "region", "timestamp"] if c in df.columns]
    if not dupe_cols:
        return DQResult("duplicate_rows", "warn", "Key columns missing, skipped.")
    dupes = df.duplicated(subset=dupe_cols, keep=False)
    count = int(dupes.sum())
    if count == 0:
        return DQResult("duplicate_rows", "pass", "No duplicate rows.")
    return DQResult("duplicate_rows", "warn", f"{count} duplicate rows on {dupe_cols}.", failed_rows=count)
