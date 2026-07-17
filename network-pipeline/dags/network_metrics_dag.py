"""
Airflow DAG: network_metrics_pipeline

Schedule : daily at 02:00 UTC (file expected to land by ~01:30)
Retries  : 3 per task, 5-minute back-off

Flow:
  start_run
      │
  check_s3_file  ──(branch)──┐
      │ file found            │ no file
  ingest_bronze          mark_skipped
      │                       │
  run_dq_checks          end_pipeline
      │
  process_silver
      │
  process_gold
      │
  mark_success
      │
  end_pipeline

Failure at any task triggers on_failure_callback → control table updated to 'fail'.
"""
import logging
import os
import sys

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Allow importing from src/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from bronze_ingestion import ingest_bronze
from silver_processing import process_silver
from gold_aggregation import process_gold
from utils.control_table import ControlTable

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
S3_BUCKET = os.getenv("S3_BUCKET", "raw-telecom-network-data")
DB_CONN = os.getenv("PIPELINE_DB_CONN", "sqlite:///./data/pipeline_control.db")
USE_LOCAL = os.getenv("USE_LOCAL_FS", "true").lower() == "true"

TABLES = [
    "bronze.network_metrics_raw",
    "silver.network_metrics_clean",
    "silver.dim_tower_region_history",
    "silver.dim_tower_region_current",
    "gold.network_metrics_hourly",
    "gold.network_metrics_daily",
]

# ─── Callbacks ────────────────────────────────────────────────────────────────

def _on_failure(context):
    """Mark the failing table's control row as 'fail' and log to Slack/email."""
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    error = str(context.get("exception", "Unknown error"))

    ctrl = ControlTable(DB_CONN)
    # Best-effort — map task_id to table name
    table_map = {
        "ingest_bronze": "bronze.network_metrics_raw",
        "run_dq_checks": "silver.network_metrics_clean",
        "process_silver": "silver.network_metrics_clean",
        "process_gold": "gold.network_metrics_hourly",
    }
    table_name = table_map.get(task_id, task_id)
    ctrl.update_status(run_id, table_name, "fail", error_message=error[:2000])

    # ── Alerting hook ──────────────────────────────────────────────────────
    # Replace with actual Slack/PagerDuty/SNS notification.
    logger.error(
        "ALERT | DAG=%s | Task=%s | RunID=%s | Error: %s",
        context["dag"].dag_id, task_id, run_id, error,
    )


# ─── Task functions ───────────────────────────────────────────────────────────

def _start_run(**context):
    execution_date = context["ds"]
    run_id = context["run_id"]
    date_nodash = execution_date.replace("-", "")
    file_name = f"network_metrics_{date_nodash}.csv"

    ctrl = ControlTable(DB_CONN)
    for table in TABLES:
        ctrl.insert_run(
            run_id=run_id,
            table_name=table,
            execution_date=execution_date,
            file_name=file_name,
        )
    logger.info("Control table initialised for run_id=%s", run_id)


def _check_s3_file(**context) -> str:
    """Returns the task_id of the next branch."""
    execution_date = context["ds"]
    date_nodash = execution_date.replace("-", "")
    file_key = f"network_metrics_{date_nodash}.csv"

    if USE_LOCAL:
        local_path = os.path.join(
            os.getenv("LOCAL_SOURCE_PATH", "./sample_data"), file_key
        )
        file_exists = os.path.exists(local_path)
    else:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id="aws_default")
        file_exists = hook.check_for_key(key=file_key, bucket_name=S3_BUCKET)

    logger.info("File %s → exists=%s", file_key, file_exists)
    return "ingest_bronze" if file_exists else "mark_skipped"


def _mark_skipped(**context):
    ctrl = ControlTable(DB_CONN)
    ctrl.update_all_running(
        run_id=context["run_id"],
        status="skipped",
        error_message=f"No source file for {context['ds']}",
    )
    logger.info("All tasks marked skipped for run_id=%s", context["run_id"])


def _ingest_bronze(**context):
    records = ingest_bronze(
        execution_date=context["ds"],
        run_id=context["run_id"],
    )
    ctrl = ControlTable(DB_CONN)
    ctrl.update_status(
        context["run_id"], "bronze.network_metrics_raw", "success",
        records_processed=records,
    )


def _run_dq_checks(**context):
    """
    Standalone DQ task — surfaces warns/fails before silver processing.
    Silver processing re-runs DQ internally; this task provides early visibility.
    """
    from bronze_ingestion import read_bronze
    from utils.data_quality import run_all_checks, has_failures

    df = read_bronze(context["ds"])
    results = run_all_checks(df, context["ds"])

    fails = [r for r in results if r.status == "fail"]
    if fails:
        names = [r.check_name for r in fails]
        raise ValueError(f"DQ hard failures: {names}")

    warns = [r for r in results if r.status == "warn"]
    if warns:
        logger.warning("DQ warnings: %s", [r.check_name for r in warns])


def _process_silver(**context):
    stats = process_silver(
        execution_date=context["ds"],
        run_id=context["run_id"],
    )
    ctrl = ControlTable(DB_CONN)
    ctrl.update_status(
        context["run_id"], "silver.network_metrics_clean", "success",
        records_processed=stats["clean_records"],
    )
    ctrl.update_status(
        context["run_id"], "silver.dim_tower_region_history", "success",
        records_processed=stats["scd2_inserted"],
    )
    ctrl.update_status(
        context["run_id"], "silver.dim_tower_region_current", "success",
    )


def _process_gold(**context):
    stats = process_gold(
        execution_date=context["ds"],
        run_id=context["run_id"],
    )
    ctrl = ControlTable(DB_CONN)
    ctrl.update_status(
        context["run_id"], "gold.network_metrics_hourly", "success",
        records_processed=stats["hourly_records"],
    )
    ctrl.update_status(
        context["run_id"], "gold.network_metrics_daily", "success",
        records_processed=stats["daily_records"],
    )


def _mark_success(**context):
    ctrl = ControlTable(DB_CONN)
    # Catch any rows still sitting on 'running' (edge case) and close them out
    ctrl.update_all_running(run_id=context["run_id"], status="success")
    logger.info("Pipeline completed successfully for run_id=%s", context["run_id"])


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
    "email_on_failure": False,  # handled in _on_failure
}

with DAG(
    dag_id="network_metrics_pipeline",
    description="Daily ingestion of cell-tower metrics: S3 → Bronze → Silver (SCD2) → Gold",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["telecom", "network", "medallion"],
    max_active_runs=1,
) as dag:

    start_run = PythonOperator(
        task_id="start_run",
        python_callable=_start_run,
    )

    check_s3_file = BranchPythonOperator(
        task_id="check_s3_file",
        python_callable=_check_s3_file,
    )

    mark_skipped = PythonOperator(
        task_id="mark_skipped",
        python_callable=_mark_skipped,
    )

    ingest_bronze_task = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_ingest_bronze,
    )

    run_dq_checks_task = PythonOperator(
        task_id="run_dq_checks",
        python_callable=_run_dq_checks,
    )

    process_silver_task = PythonOperator(
        task_id="process_silver",
        python_callable=_process_silver,
    )

    process_gold_task = PythonOperator(
        task_id="process_gold",
        python_callable=_process_gold,
    )

    mark_success = PythonOperator(
        task_id="mark_success",
        python_callable=_mark_success,
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    start_run >> check_s3_file

    check_s3_file >> mark_skipped >> end_pipeline
    check_s3_file >> ingest_bronze_task >> run_dq_checks_task >> process_silver_task >> process_gold_task >> mark_success >> end_pipeline
