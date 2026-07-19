"""
Unit tests for utils/data_quality.py

Each check is tested in isolation with a minimal DataFrame that targets
exactly the condition being verified.
"""
import pandas as pd
import pytest

from utils.data_quality import (
    run_all_checks,
    has_failures,
    _check_not_empty,
    _check_schema,
    _check_nulls,
    _check_signal_range,
    _check_data_volume,
    _check_timestamp_freshness,
    _check_duplicates,
)

EXECUTION_DATE = "2025-07-23"

# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_df():
    """Minimal DataFrame that passes all checks."""
    return pd.DataFrame({
        "tower_id":        [101, 102, 103],
        "region":          ["North", "South", "East"],
        "timestamp":       [1753228800, 1753232400, 1753236000],  # 2025-07-23
        "signal_strength": [-75.0, -85.0, -65.0],
        "data_volume_mb":  [100.0, 200.0, 150.0],
    })


# ── _check_not_empty ──────────────────────────────────────────────────────────

def test_not_empty_pass(valid_df):
    result = _check_not_empty(valid_df)
    assert result.status == "pass"

def test_not_empty_fail():
    result = _check_not_empty(pd.DataFrame())
    assert result.status == "fail"
    assert "zero rows" in result.message


# ── _check_schema ─────────────────────────────────────────────────────────────

def test_schema_pass(valid_df):
    result = _check_schema(valid_df)
    assert result.status == "pass"

def test_schema_missing_column(valid_df):
    result = _check_schema(valid_df.drop(columns=["signal_strength"]))
    assert result.status == "fail"
    assert "signal_strength" in result.message

def test_schema_extra_column_is_pass(valid_df):
    df = valid_df.copy()
    df["extra_col"] = 1
    result = _check_schema(df)
    assert result.status == "pass"
    assert "extra_col" in result.message


# ── _check_nulls ──────────────────────────────────────────────────────────────

def test_nulls_pass(valid_df):
    result = _check_nulls(valid_df)
    assert result.status == "pass"

def test_nulls_warn_on_metric_column(valid_df):
    df = valid_df.copy()
    df.loc[0, "signal_strength"] = None
    result = _check_nulls(df)
    assert result.status == "warn"
    assert result.failed_rows >= 1

def test_nulls_warn_on_key_column(valid_df):
    df = valid_df.copy()
    df.loc[0, "tower_id"] = None
    result = _check_nulls(df)
    assert result.status == "warn"


# ── _check_signal_range ───────────────────────────────────────────────────────

def test_signal_range_pass(valid_df):
    result = _check_signal_range(valid_df)
    assert result.status == "pass"

def test_signal_range_warn_above_max(valid_df):
    df = valid_df.copy()
    df.loc[0, "signal_strength"] = -30.0  # above -40 dBm
    result = _check_signal_range(df)
    assert result.status == "warn"
    assert result.failed_rows == 1

def test_signal_range_warn_below_min(valid_df):
    df = valid_df.copy()
    df.loc[0, "signal_strength"] = -125.0  # below -120 dBm
    result = _check_signal_range(df)
    assert result.status == "warn"
    assert result.failed_rows == 1

def test_signal_range_missing_column():
    df = pd.DataFrame({"tower_id": [1]})
    result = _check_signal_range(df)
    assert result.status == "warn"
    assert "missing" in result.message


# ── _check_data_volume ────────────────────────────────────────────────────────

def test_data_volume_pass(valid_df):
    result = _check_data_volume(valid_df)
    assert result.status == "pass"

def test_data_volume_warn_negative(valid_df):
    df = valid_df.copy()
    df.loc[1, "data_volume_mb"] = -5.0
    result = _check_data_volume(df)
    assert result.status == "warn"
    assert result.failed_rows == 1

def test_data_volume_zero_is_pass(valid_df):
    df = valid_df.copy()
    df.loc[0, "data_volume_mb"] = 0.0
    result = _check_data_volume(df)
    assert result.status == "pass"


# ── _check_timestamp_freshness ────────────────────────────────────────────────

def test_timestamp_freshness_pass(valid_df):
    result = _check_timestamp_freshness(valid_df, EXECUTION_DATE)
    assert result.status == "pass"

def test_timestamp_freshness_warn_wrong_date(valid_df):
    df = valid_df.copy()
    df.loc[0, "timestamp"] = 1700000000  # 2023-11-14 — wrong day
    result = _check_timestamp_freshness(df, EXECUTION_DATE)
    assert result.status == "warn"
    assert result.failed_rows == 1

def test_timestamp_freshness_missing_column():
    df = pd.DataFrame({"tower_id": [1]})
    result = _check_timestamp_freshness(df, EXECUTION_DATE)
    assert result.status == "warn"


# ── _check_duplicates ─────────────────────────────────────────────────────────

def test_duplicates_pass(valid_df):
    result = _check_duplicates(valid_df)
    assert result.status == "pass"

def test_duplicates_warn(valid_df):
    df = pd.concat([valid_df, valid_df.iloc[[0]]], ignore_index=True)
    result = _check_duplicates(df)
    assert result.status == "warn"
    assert result.failed_rows == 2  # both copies of the duplicated row are flagged


# ── run_all_checks + has_failures ─────────────────────────────────────────────

def test_run_all_checks_clean_data(valid_df):
    results = run_all_checks(valid_df, EXECUTION_DATE)
    assert len(results) == 7
    assert not has_failures(results)

def test_run_all_checks_detects_hard_failure():
    # Missing required column is a hard fail
    df = pd.DataFrame({"tower_id": [1], "region": ["North"],
                       "timestamp": [1753228800], "data_volume_mb": [100.0]})  # signal_strength missing
    results = run_all_checks(df, EXECUTION_DATE)
    assert has_failures(results)

def test_has_failures_false_on_warns(valid_df):
    df = valid_df.copy()
    df.loc[0, "signal_strength"] = -125.0  # triggers warn only
    results = run_all_checks(df, EXECUTION_DATE)
    assert not has_failures(results)
