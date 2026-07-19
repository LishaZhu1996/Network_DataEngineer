"""
Airflow DAG: network_metrics_pipeline

Schedule : daily at 02:00 UTC
Retries  : 3 per task, 5-minute back-off

─── Normal daily run ───────────────────────────────────────────────────────────
Trigger without conf → processes the execution date (ds).

─── Backfill / reload run ──────────────────────────────────────────────────────
Trigger with conf to reload a date range:

  airflow dags trigger network_metrics_pipeline \\
    --conf '{"start_date": "2025-07-01", "end_date": "2025-07-23"}'

Each date in the range is processed independently:
  - If the source file is missing for a date it is skipped (logged + control table = skipped).
  - Gold tables have existing rows for those dates deleted before re-insert (idempotent reload).

─── DAG flow ────────────────────────────────────────────────────────────────────
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
"""
import logging
import os
import sys
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config import DB_CONN, S3_BUCKET, AWS_CONN_ID, USE_LOCAL, PIPELINE_TABLES, SOURCE_PATH, source_file_name
from utils.control_table import ControlTable

logger = logging.getLogger(__name__)


# ─── Backfill helpers ─────────────────────────────────────────────────────────

def _date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= end_dt:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def _resolve_dates(context) -> list[str]:
    """
    Return the list of dates to process for this DAG run.
    Daily run   → [ds]
    Backfill    → [start_date .. end_date]
    """
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    start = conf.get("start_date")
    end   = conf.get("end_date")

    if start and end:
        dates = list(_date_range(start, end))
        logger.info("Backfill mode: %d dates (%s → %s)", len(dates), start, end)
        return dates

    return [context["ds"]]


def _file_exists(execution_date: str) -> bool:
    file_name = source_file_name(execution_date)
    if USE_LOCAL:
        return os.path.exists(os.path.join(SOURCE_PATH, file_name))
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    return S3Hook(aws_conn_id=AWS_CONN_ID).check_for_key(key=file_name, bucket_name=S3_BUCKET)


# ─── Alerting ────────────────────────────────────────────────────────────────
# Configure these in Airflow connections / environment variables:
#   SLACK_WEBHOOK_CONN_ID → Airflow HTTP connection with the Slack webhook URL
#   AIRFLOW_BASE_URL      → e.g. https://airflow.internal (for log deep-links)

SLACK_CONN_ID    = os.getenv("SLACK_WEBHOOK_CONN_ID", "slack_webhook_data_incidents")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")


def _slack_notify(message: str) -> None:
    """Post a message to Slack via an Airflow HTTP connection (webhook URL)."""
    try:
        from airflow.providers.http.hooks.http import HttpHook
        hook = HttpHook(method="POST", http_conn_id=SLACK_CONN_ID)
        hook.run(endpoint="", data={"text": message}, headers={"Content-Type": "application/json"})
    except Exception as e:
        logger.error("Slack notification failed: %s", e)


def _log_url(context) -> str:
    ti = context["task_instance"]
    return (
        f"{AIRFLOW_BASE_URL}/log"
        f"?dag_id={ti.dag_id}&task_id={ti.task_id}"
        f"&execution_date={context['ds']}&try_number={ti.try_number}"
    )


# ─── Failure callback ─────────────────────────────────────────────────────────

def _on_failure(context):
    task_id    = context["task_instance"].task_id
    run_id     = context["run_id"]
    dag_id     = context["dag"].dag_id
    error      = str(context.get("exception", "Unknown error"))
    try_number = context["task_instance"].try_number
    max_tries  = context["task_instance"].max_tries  # retries + 1

    table_map = {
        "ingest_bronze":  "bronze.network_metrics_raw",
        "run_dq_checks":  "silver.network_metrics_clean",
        "process_silver": "silver.network_metrics_clean",
        "process_gold":   "gold.fact_tower_daily_snapshot",
    }
    table_name = table_map.get(task_id, task_id)
    ControlTable(DB_CONN).update_status(run_id, table_name, "fail", error_message=error[:2000])

    log_url = _log_url(context)

    # ── Slack: only after all retries exhausted ───────────────────────────────
    if try_number >= max_tries:
        slack_msg = (
            f":red_circle: *Pipeline failure — all retries exhausted*\n"
            f">*DAG:* `{dag_id}`\n"
            f">*Task:* `{task_id}`\n"
            f">*Date:* `{context['ds']}`\n"
            f">*Run ID:* `{run_id}`\n"
            f">*Attempts:* {max_tries}\n"
            f">*Error:* {error[:300]}\n"
            f">*Logs:* {log_url}"
        )
        _slack_notify(slack_msg)

    logger.error("ALERT | DAG=%s | Task=%s | RunID=%s | Try=%s/%s | Error: %s",
                 dag_id, task_id, run_id, try_number, max_tries, error)


def _on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Fires if mark_success hasn't completed within the SLA window."""
    msg = (
        f":warning: *SLA breach* — `{dag.dag_id}` has not completed within the expected window.\n"
        f">Blocked tasks: {[ti.task_id for ti in blocking_tis]}"
    )
    _slack_notify(msg)


# ─── Task callables ───────────────────────────────────────────────────────────

def _start_run(**context):
    run_id = context["run_id"]
    dates  = _resolve_dates(context)
    ctrl   = ControlTable(DB_CONN)

    for d in dates:
        for table in PIPELINE_TABLES:
            ctrl.insert_run(
                run_id=run_id,
                table_name=table,
                execution_date=d,
                file_name=source_file_name(d),
            )
    context["task_instance"].xcom_push(key="dates", value=dates)
    logger.info("Initialised control table for %d date(s), run_id=%s", len(dates), run_id)


def _check_s3_file(**context) -> str:
    """
    For a single daily run: branch directly.
    For a backfill: a missing file on any date is a warning, not a hard stop
    (handled per-date inside processing tasks). Branch to ingest_bronze if at
    least one date has a file.
    """
    dates = context["task_instance"].xcom_pull(key="dates", task_ids="start_run")
    found = [d for d in dates if _file_exists(d)]
    missing = [d for d in dates if not _file_exists(d)]

    if missing:
        logger.warning("No source file for dates: %s — will be skipped", missing)

    if found:
        context["task_instance"].xcom_push(key="dates_with_file", value=found)
        context["task_instance"].xcom_push(key="dates_missing",   value=missing)
        return "ingest_bronze"

    return "mark_skipped"


def _mark_skipped(**context):
    ctrl   = ControlTable(DB_CONN)
    run_id = context["run_id"]
    dates  = context["task_instance"].xcom_pull(key="dates", task_ids="start_run")
    ctrl.update_all_running(run_id, "skipped",
                            error_message=f"No source file for date(s): {dates}")
    logger.info("All tasks marked skipped for run_id=%s", run_id)


def _ingest_bronze(**context):
    from bronze_ingestion import ingest_bronze

    run_id = context["run_id"]
    dates  = context["task_instance"].xcom_pull(key="dates_with_file", task_ids="check_s3_file")
    ctrl   = ControlTable(DB_CONN)
    total  = 0

    for d in dates:
        n = ingest_bronze(execution_date=d, run_id=run_id)
        ctrl.update_status(run_id, "bronze.network_metrics_raw", "success",
                           records_processed=n)
        total += n

    logger.info("Bronze complete — %d rows across %d date(s)", total, len(dates))


def _run_dq_checks(**context):
    from bronze_ingestion import read_bronze
    from utils.data_quality import run_all_checks, has_failures

    dates = context["task_instance"].xcom_pull(key="dates_with_file", task_ids="check_s3_file")
    all_failures = []

    for d in dates:
        df = read_bronze(d)
        results = run_all_checks(df, d)
        fails = [r.check_name for r in results if r.status == "fail"]
        if fails:
            all_failures.append(f"{d}: {fails}")
        warns = [r.check_name for r in results if r.status == "warn"]
        if warns:
            logger.warning("DQ warnings for %s: %s", d, warns)

    if all_failures:
        raise ValueError(f"DQ hard failures: {all_failures}")


def _process_silver(**context):
    from silver_processing import process_silver

    run_id = context["run_id"]
    dates  = context["task_instance"].xcom_pull(key="dates_with_file", task_ids="check_s3_file")
    ctrl   = ControlTable(DB_CONN)
    total  = 0

    for d in dates:
        stats = process_silver(execution_date=d, run_id=run_id)
        ctrl.update_status(run_id, "silver.network_metrics_clean", "success",
                           records_processed=stats["clean_records"])
        total += stats["clean_records"]

    logger.info("Silver complete — %d total clean rows", total)


def _process_gold(**context):
    from gold_aggregation import process_gold

    run_id = context["run_id"]
    dates  = context["task_instance"].xcom_pull(key="dates_with_file", task_ids="check_s3_file")
    ctrl   = ControlTable(DB_CONN)

    for d in dates:
        stats = process_gold(execution_date=d, run_id=run_id)
        ctrl.update_status(run_id, "gold.fact_tower_daily_snapshot", "success",
                           records_processed=stats["fact_records"])
        ctrl.update_status(run_id, "gold.network_metrics_hourly", "success",
                           records_processed=stats["hourly_records"])
        ctrl.update_status(run_id, "gold.network_metrics_daily", "success",
                           records_processed=stats["daily_records"])


def _mark_success(**context):
    ControlTable(DB_CONN).update_all_running(
        run_id=context["run_id"], status="success"
    )
    logger.info("Pipeline completed successfully for run_id=%s", context["run_id"])


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="network_metrics_pipeline",
    description=(
        "Daily ingestion of cell-tower metrics S3 → Bronze → Silver → Gold (Iceberg). "
        "Supports date-range backfill via dag_run conf: {start_date, end_date}."
    ),
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args=default_args,
    sla_miss_callback=_on_sla_miss,
    tags=["telecom", "network", "medallion", "iceberg"],
    max_active_runs=1,
    params={
        "start_date": None,  # optional YYYY-MM-DD; if set with end_date → backfill mode
        "end_date":   None,  # optional YYYY-MM-DD
    },
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

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_ingest_bronze,
    )

    run_dq_checks = PythonOperator(
        task_id="run_dq_checks",
        python_callable=_run_dq_checks,
    )

    process_silver = PythonOperator(
        task_id="process_silver",
        python_callable=_process_silver,
    )

    process_gold = PythonOperator(
        task_id="process_gold",
        python_callable=_process_gold,
    )

    mark_success = PythonOperator(
        task_id="mark_success",
        python_callable=_mark_success,
        sla=timedelta(hours=3),  # alert if pipeline hasn't finished by 05:00 UTC
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    start_run >> check_s3_file

    check_s3_file >> mark_skipped    >> end_pipeline
    check_s3_file >> ingest_bronze >> run_dq_checks >> process_silver >> process_gold >> mark_success >> end_pipeline
