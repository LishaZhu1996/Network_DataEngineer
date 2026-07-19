"""
Unit tests for utils/control_table.py

Uses an in-memory SQLite database so tests are fast and leave no state on disk.
"""
import pytest
from utils.control_table import ControlTable

CONN = "sqlite://"  # in-memory SQLite — destroyed after each test


@pytest.fixture
def ctrl():
    return ControlTable(CONN)


# ── insert_run ────────────────────────────────────────────────────────────────

def test_insert_run_creates_row(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23")
    rows = _fetch_all(ctrl)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-001"
    assert rows[0]["table_name"] == "bronze.network_metrics_raw"
    assert rows[0]["status"] == "running"

def test_insert_run_sets_start_datetime(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23")
    rows = _fetch_all(ctrl)
    assert rows[0]["start_datetime"] is not None

def test_insert_run_stores_file_name(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23",
                    file_name="network_metrics_20250723.csv")
    rows = _fetch_all(ctrl)
    assert rows[0]["file_name"] == "network_metrics_20250723.csv"

def test_insert_multiple_tables(ctrl):
    for table in ["bronze.network_metrics_raw", "silver.network_metrics_clean", "gold.fact_tower_daily_snapshot"]:
        ctrl.insert_run("run-001", table, "2025-07-23")
    rows = _fetch_all(ctrl)
    assert len(rows) == 3


# ── update_status ─────────────────────────────────────────────────────────────

def test_update_status_to_success(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23")
    ctrl.update_status("run-001", "bronze.network_metrics_raw", "success", records_processed=100)
    rows = _fetch_all(ctrl)
    assert rows[0]["status"] == "success"
    assert rows[0]["records_processed"] == 100
    assert rows[0]["finish_datetime"] is not None

def test_update_status_to_fail_with_error(ctrl):
    ctrl.insert_run("run-001", "silver.network_metrics_clean", "2025-07-23")
    ctrl.update_status("run-001", "silver.network_metrics_clean", "fail",
                       error_message="RuntimeError: Hard DQ failure")
    rows = _fetch_all(ctrl)
    assert rows[0]["status"] == "fail"
    assert "DQ failure" in rows[0]["error_message"]

def test_update_status_only_affects_matching_row(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23")
    ctrl.insert_run("run-001", "silver.network_metrics_clean", "2025-07-23")
    ctrl.update_status("run-001", "bronze.network_metrics_raw", "success")
    rows = {r["table_name"]: r for r in _fetch_all(ctrl)}
    assert rows["bronze.network_metrics_raw"]["status"] == "success"
    assert rows["silver.network_metrics_clean"]["status"] == "running"


# ── update_all_running ────────────────────────────────────────────────────────

def test_update_all_running_to_skipped(ctrl):
    for table in ["bronze.network_metrics_raw", "silver.network_metrics_clean"]:
        ctrl.insert_run("run-001", table, "2025-07-23")
    ctrl.update_all_running("run-001", "skipped", error_message="No file found")
    rows = _fetch_all(ctrl)
    assert all(r["status"] == "skipped" for r in rows)
    assert all("No file" in r["error_message"] for r in rows)

def test_update_all_running_does_not_overwrite_success(ctrl):
    ctrl.insert_run("run-001", "bronze.network_metrics_raw", "2025-07-23")
    ctrl.insert_run("run-001", "silver.network_metrics_clean", "2025-07-23")
    ctrl.update_status("run-001", "bronze.network_metrics_raw", "success")
    ctrl.update_all_running("run-001", "fail")
    rows = {r["table_name"]: r for r in _fetch_all(ctrl)}
    # Only the still-running row should be updated
    assert rows["bronze.network_metrics_raw"]["status"] == "success"
    assert rows["silver.network_metrics_clean"]["status"] == "fail"


# ── helpers ───────────────────────────────────────────────────────────────────

def _fetch_all(ctrl) -> list[dict]:
    from sqlalchemy import text
    with ctrl.engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM pipeline_control"))
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]
