"""
Control table operations.
One row per (run_id, table_name) — tracks lifecycle of each layer per DAG run.
"""
from datetime import datetime, timezone
from sqlalchemy import create_engine, text


class ControlTable:
    TABLE = "pipeline_control"

    def __init__(self, conn_string: str):
        self.engine = create_engine(conn_string)
        self._ensure_table()

    def _ensure_table(self):
        with self.engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.TABLE} (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id          TEXT    NOT NULL,
                    table_name      TEXT    NOT NULL,
                    execution_date  TEXT,
                    file_name       TEXT,
                    start_datetime  TEXT    NOT NULL,
                    finish_datetime TEXT,
                    status          TEXT    NOT NULL DEFAULT 'running',
                    error_message   TEXT,
                    records_processed INTEGER
                )
            """))
            conn.commit()

    def insert_run(self, run_id: str, table_name: str, execution_date: str, file_name: str = None):
        with self.engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {self.TABLE}
                    (run_id, table_name, execution_date, file_name, start_datetime, status)
                VALUES
                    (:run_id, :table_name, :execution_date, :file_name, :start_dt, 'running')
            """), {
                "run_id": run_id,
                "table_name": table_name,
                "execution_date": execution_date,
                "file_name": file_name,
                "start_dt": _now(),
            })
            conn.commit()

    def update_status(
        self,
        run_id: str,
        table_name: str,
        status: str,
        error_message: str = None,
        records_processed: int = None,
    ):
        with self.engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE {self.TABLE}
                SET    status            = :status,
                       finish_datetime   = :finish_dt,
                       error_message     = COALESCE(:error_message, error_message),
                       records_processed = COALESCE(:records, records_processed)
                WHERE  run_id = :run_id AND table_name = :table_name
            """), {
                "status": status,
                "finish_dt": _now(),
                "error_message": error_message,
                "records": records_processed,
                "run_id": run_id,
                "table_name": table_name,
            })
            conn.commit()

    def update_all_running(self, run_id: str, status: str, error_message: str = None):
        """Flip every still-running row for a run_id to the given status (used for skipped/fail)."""
        with self.engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE {self.TABLE}
                SET    status          = :status,
                       finish_datetime = :finish_dt,
                       error_message   = COALESCE(:error_message, error_message)
                WHERE  run_id = :run_id AND status = 'running'
            """), {
                "status": status,
                "finish_dt": _now(),
                "error_message": error_message,
                "run_id": run_id,
            })
            conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
