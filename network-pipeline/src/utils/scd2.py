"""
SCD Type 2 helpers.

hash_key  = MD5 of the natural key (tower_id + region)
hash_diff = MD5 of all tracked attributes (signal_strength, data_volume_mb)

Workflow per daily run:
  1. Compute representative row per tower_id+region (daily aggregate).
  2. Load existing current records from the history table.
  3. For each incoming record:
       - NEW key             → insert with is_current=True
       - Existing, same diff → no-op (idempotent)
       - Existing, new diff  → expire old row, insert new row
"""
import hashlib
from datetime import date, datetime, timezone

import pandas as pd


def md5(*parts: str) -> str:
    joined = "|".join(str(p) for p in parts)
    return hashlib.md5(joined.encode()).hexdigest()


def compute_hashes(df: pd.DataFrame) -> pd.DataFrame:
    """Add hash_key and hash_diff columns to a silver-aggregated DataFrame."""
    df = df.copy()
    df["hash_key"] = df.apply(
        lambda r: md5(r["tower_id"], r["region"]), axis=1
    )
    df["hash_diff"] = df.apply(
        lambda r: md5(r["avg_signal_strength"], r["total_data_volume_mb"]), axis=1
    )
    return df


def apply_scd2(
    incoming: pd.DataFrame,
    existing: pd.DataFrame,
    run_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (records_to_expire, records_to_insert).

    incoming : daily aggregate with hash_key, hash_diff columns
    existing : current rows from dim_tower_region_history (is_current=True)
    run_date : YYYY-MM-DD string
    """
    today = run_date
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    existing_map: dict[str, dict] = {}
    if not existing.empty:
        existing_map = {
            row["hash_key"]: row.to_dict()
            for _, row in existing.iterrows()
        }

    to_expire: list[dict] = []
    to_insert: list[dict] = []

    for _, row in incoming.iterrows():
        hk = row["hash_key"]
        hd = row["hash_diff"]
        new_record = _build_record(row, effective_from=today, is_current=True, created_at=now_ts)

        if hk not in existing_map:
            # Brand-new tower+region combination
            to_insert.append(new_record)
        elif existing_map[hk]["hash_diff"] != hd:
            # Attributes changed → expire old, insert new
            expired = existing_map[hk].copy()
            expired["effective_to"] = today
            expired["is_current"] = False
            to_expire.append(expired)
            to_insert.append(new_record)
        # else: unchanged → no action

    expire_df = pd.DataFrame(to_expire) if to_expire else pd.DataFrame()
    insert_df = pd.DataFrame(to_insert) if to_insert else pd.DataFrame()
    return expire_df, insert_df


def _build_record(row: pd.Series, effective_from: str, is_current: bool, created_at: str) -> dict:
    return {
        "hash_key": row["hash_key"],
        "hash_diff": row["hash_diff"],
        "tower_id": row["tower_id"],
        "region": row["region"],
        "avg_signal_strength": row["avg_signal_strength"],
        "total_data_volume_mb": row["total_data_volume_mb"],
        "record_count": row.get("record_count", None),
        "effective_from": effective_from,
        "effective_to": None,
        "is_current": is_current,
        "dw_created_at": created_at,
    }
