"""
Unit tests for gold_aggregation builders.

All three builders are pure functions (DataFrame in → DataFrame out),
so they can be tested without any I/O.
"""
import pandas as pd
import pytest

from gold_aggregation import (
    _build_fact_daily_snapshot,
    _build_hourly,
    _build_daily,
)

EXECUTION_DATE = "2025-07-23"


@pytest.fixture
def silver_df():
    """Simulates silver_clean for one day — 6 readings across 2 towers and 3 regions."""
    return pd.DataFrame({
        "tower_id":        [101, 101, 102, 102, 103, 103],
        "region":          ["North", "North", "South", "South", "East", "East"],
        "timestamp":       [1753228800, 1753232400, 1753236000, 1753239600, 1753243200, 1753246800],
        "signal_strength": [-70.0, -80.0, -85.0, -95.0, -60.0, -70.0],
        "data_volume_mb":  [100.0, 200.0, 150.0, 250.0, 50.0, 100.0],
        "event_hour":      [0, 1, 2, 3, 4, 5],
        "event_date":      [EXECUTION_DATE] * 6,
        "_execution_date": [EXECUTION_DATE] * 6,
    })


# ── fact_tower_daily_snapshot ─────────────────────────────────────────────────

def test_fact_snapshot_grain(silver_df):
    """One row per (tower_id, region)."""
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    assert len(fact) == 3  # 101/North, 102/South, 103/East

def test_fact_snapshot_no_duplicate_keys(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    assert fact.duplicated(subset=["tower_id", "region"]).sum() == 0

def test_fact_snapshot_avg_signal(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    row = fact[(fact["tower_id"] == 101) & (fact["region"] == "North")].iloc[0]
    assert row["avg_signal_strength"] == pytest.approx(-75.0)

def test_fact_snapshot_min_max_signal(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    row = fact[(fact["tower_id"] == 102) & (fact["region"] == "South")].iloc[0]
    assert row["min_signal_strength"] == -95.0
    assert row["max_signal_strength"] == -85.0

def test_fact_snapshot_total_volume(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    row = fact[(fact["tower_id"] == 101) & (fact["region"] == "North")].iloc[0]
    assert row["total_data_volume_mb"] == pytest.approx(300.0)

def test_fact_snapshot_reading_count(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    assert (fact["reading_count"] == 2).all()  # each tower+region has 2 readings

def test_fact_snapshot_has_execution_date(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    assert (fact["execution_date"] == EXECUTION_DATE).all()

def test_fact_snapshot_required_columns(silver_df):
    fact = _build_fact_daily_snapshot(silver_df, EXECUTION_DATE)
    expected = {
        "tower_id", "region", "execution_date",
        "avg_signal_strength", "min_signal_strength", "max_signal_strength",
        "stddev_signal_strength", "total_data_volume_mb", "avg_data_volume_mb",
        "reading_count",
    }
    assert expected.issubset(set(fact.columns))


# ── network_metrics_hourly ────────────────────────────────────────────────────

def test_hourly_grain(silver_df):
    """One row per (region, event_hour)."""
    hourly = _build_hourly(silver_df, EXECUTION_DATE)
    assert len(hourly) == 6  # each of 6 rows has a unique region+hour combo

def test_hourly_no_duplicate_keys(silver_df):
    hourly = _build_hourly(silver_df, EXECUTION_DATE)
    assert hourly.duplicated(subset=["region", "event_hour"]).sum() == 0

def test_hourly_total_volume(silver_df):
    hourly = _build_hourly(silver_df, EXECUTION_DATE)
    row = hourly[(hourly["region"] == "North") & (hourly["event_hour"] == 0)].iloc[0]
    assert row["total_data_volume_mb"] == pytest.approx(100.0)

def test_hourly_has_execution_date(silver_df):
    hourly = _build_hourly(silver_df, EXECUTION_DATE)
    assert (hourly["execution_date"] == EXECUTION_DATE).all()

def test_hourly_tower_count(silver_df):
    hourly = _build_hourly(silver_df, EXECUTION_DATE)
    # Each region+hour has exactly 1 tower
    assert (hourly["tower_count"] == 1).all()


# ── network_metrics_daily ─────────────────────────────────────────────────────

def test_daily_grain(silver_df):
    """One row per region."""
    daily = _build_daily(silver_df, EXECUTION_DATE)
    assert len(daily) == 3  # North, South, East

def test_daily_no_duplicate_regions(silver_df):
    daily = _build_daily(silver_df, EXECUTION_DATE)
    assert daily.duplicated(subset=["region"]).sum() == 0

def test_daily_total_volume_per_region(silver_df):
    daily = _build_daily(silver_df, EXECUTION_DATE)
    north = daily[daily["region"] == "North"].iloc[0]
    assert north["total_data_volume_mb"] == pytest.approx(300.0)

def test_daily_avg_signal_per_region(silver_df):
    daily = _build_daily(silver_df, EXECUTION_DATE)
    east = daily[daily["region"] == "East"].iloc[0]
    assert east["avg_signal_strength"] == pytest.approx(-65.0)

def test_daily_has_execution_date(silver_df):
    daily = _build_daily(silver_df, EXECUTION_DATE)
    assert (daily["execution_date"] == EXECUTION_DATE).all()

def test_daily_record_count(silver_df):
    daily = _build_daily(silver_df, EXECUTION_DATE)
    assert (daily["record_count"] == 2).all()
