"""
Unit tests for silver_processing._clean()

Tests the cleaning logic in isolation — no file I/O, no S3.
"""
import pandas as pd
import pytest

from silver_processing import _clean

EXECUTION_DATE = "2025-07-23"
RUN_ID = "test-run-001"


@pytest.fixture
def raw_df():
    """Simulates a bronze DataFrame as it comes out of read_bronze()."""
    return pd.DataFrame({
        "tower_id":        [101, 102, 103, 102],
        "region":          ["North", "South", "East", "South"],
        "timestamp":       [1753228800, 1753232400, 1753236000, 1753232400],  # row 3 is duplicate of row 1
        "signal_strength": ["-75.0", "-85.0", "-65.0", "-85.0"],  # strings — needs casting
        "data_volume_mb":  [100.0, 200.0, 150.0, 200.0],
        "_run_id":         ["x", "x", "x", "x"],
        "_source_file":    ["f.csv"] * 4,
        "_ingested_at":    ["2025-07-23 02:00:00"] * 4,
        "_execution_date": [EXECUTION_DATE] * 4,
    })


# ── type casting ──────────────────────────────────────────────────────────────

def test_clean_casts_signal_strength_to_float(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert df["signal_strength"].dtype == float

def test_clean_casts_tower_id_to_int(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert pd.api.types.is_integer_dtype(df["tower_id"])


# ── timestamp conversion ──────────────────────────────────────────────────────

def test_clean_adds_event_datetime(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert "event_datetime" in df.columns

def test_clean_adds_event_hour(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert "event_hour" in df.columns
    assert df["event_hour"].between(0, 23).all()

def test_clean_adds_event_date(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert "event_date" in df.columns


# ── deduplication ─────────────────────────────────────────────────────────────

def test_clean_drops_duplicate_rows(raw_df):
    # raw_df has 4 rows but row 3 is a duplicate of row 1
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert len(df) == 3

def test_clean_keeps_first_on_duplicate(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    # tower 102 / South should appear exactly once
    assert len(df[(df["tower_id"] == 102) & (df["region"] == "South")]) == 1


# ── null handling ─────────────────────────────────────────────────────────────

def test_clean_drops_rows_with_null_tower_id(raw_df):
    raw_df.loc[0, "tower_id"] = None
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert 101 not in df["tower_id"].values

def test_clean_drops_rows_with_null_region(raw_df):
    raw_df.loc[1, "region"] = None
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    # row 1 (tower 102, None) is dropped — null region
    # row 3 (tower 102, South) is NOT a duplicate of row 1 after the null, so it survives
    # remaining: row 0 (101/North), row 2 (103/East), row 3 (102/South) = 3 rows
    assert len(df) == 3
    assert df["region"].isnull().sum() == 0

def test_clean_drops_rows_with_null_timestamp(raw_df):
    raw_df.loc[2, "timestamp"] = None
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert 103 not in df["tower_id"].values


# ── metadata columns ──────────────────────────────────────────────────────────

def test_clean_adds_execution_date(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert (df["_execution_date"] == EXECUTION_DATE).all()

def test_clean_adds_run_id(raw_df):
    df = _clean(raw_df, EXECUTION_DATE, RUN_ID)
    assert (df["_run_id"] == RUN_ID).all()
