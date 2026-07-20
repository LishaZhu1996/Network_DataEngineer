"""
Bronze layer ingestion.

Reads the raw CSV for execution_date from S3 (or local FS), attaches pipeline
metadata columns, and writes an Iceberg table to the bronze zone.
No transformation of values — bronze is an immutable copy of source.
"""
import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from config import (
    USE_LOCAL, SOURCE_PATH, BRONZE_PATH,
    S3_BUCKET, S3_BRONZE_PREFIX, AWS_CONN_ID,
    source_file_name,
)

logger = logging.getLogger(__name__)


def ingest_bronze(execution_date: str, run_id: str) -> int:
    """
    Download the daily CSV, add metadata, write to bronze Iceberg table.
    Returns the number of records ingested.
    """
    file_name = source_file_name(execution_date)
    logger.info("Reading source file: %s", file_name)

    df = _read_source(file_name)

    _check_source_schema(df)

    df["_run_id"]          = run_id
    df["_source_file"]     = file_name
    df["_ingested_at"]     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["_execution_date"]  = execution_date

    _write_bronze(df, execution_date)
    logger.info("Bronze ingestion complete — %d rows", len(df))
    return len(df)


def read_bronze(execution_date: str) -> pd.DataFrame:
    """Load bronze data for a given execution_date (used by silver layer)."""
    if USE_LOCAL:
        path = os.path.join(BRONZE_PATH, f"network_metrics_{execution_date}.parquet")
        return pd.read_parquet(path)

    import boto3
    s3 = boto3.client("s3")
    obj = s3.get_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_BRONZE_PREFIX}/{execution_date}/data.parquet",
    )
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


# ─── private helpers ──────────────────────────────────────────────────────────

def _check_source_schema(df: pd.DataFrame) -> None:
    """Abort before any write if the source CSV has a type change."""
    from utils.data_quality import _check_schema_evolution
    result = _check_schema_evolution(df)
    if result.status == "fail":
        raise RuntimeError(f"Schema evolution check failed — aborting bronze write: {result.message}")

def _read_source(file_name: str) -> pd.DataFrame:
    if USE_LOCAL:
        return pd.read_csv(os.path.join(SOURCE_PATH, file_name))

    import boto3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=file_name)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def _write_bronze(df: pd.DataFrame, execution_date: str):
    """
    Bronze writes to Parquet (local) or Iceberg (S3/production).
    Local uses plain Parquet so that the bronze layer can be read back without
    a catalog; Iceberg is used for cloud where catalog management is available.
    """
    if USE_LOCAL:
        os.makedirs(BRONZE_PATH, exist_ok=True)
        out = os.path.join(BRONZE_PATH, f"network_metrics_{execution_date}.parquet")
        df.to_parquet(out, index=False)
        logger.info("Wrote bronze (parquet) → %s", out)
        return

    from src.utils import write_iceberg
    write_iceberg(
        df,
        namespace="bronze",
        table_name="network_metrics_raw",
        overwrite_filter={"_execution_date": execution_date},
    )
