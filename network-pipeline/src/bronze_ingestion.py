"""
Bronze layer ingestion.

Reads the raw CSV from S3 (or local FS), attaches pipeline metadata columns,
and writes a Parquet file to the bronze zone. No transformation of values.
"""
import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

USE_LOCAL = os.getenv("USE_LOCAL_FS", "true").lower() == "true"
LOCAL_BRONZE_PATH = os.getenv("LOCAL_BRONZE_PATH", "./data/bronze")
LOCAL_SOURCE_PATH = os.getenv("LOCAL_SOURCE_PATH", "./sample_data")
S3_BUCKET = os.getenv("S3_BUCKET", "raw-telecom-network-data")
S3_BRONZE_PREFIX = os.getenv("S3_BRONZE_PREFIX", "bronze/network_metrics")


def ingest_bronze(execution_date: str, run_id: str) -> int:
    """
    Download the daily CSV for execution_date, add metadata, write to bronze.
    Returns the number of records ingested.
    """
    date_nodash = execution_date.replace("-", "")
    file_name = f"network_metrics_{date_nodash}.csv"

    logger.info("Reading source file: %s", file_name)
    df = _read_source(file_name)

    df["_run_id"] = run_id
    df["_source_file"] = file_name
    df["_ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["_execution_date"] = execution_date

    _write_bronze(df, execution_date)
    logger.info("Bronze ingestion complete — %d rows", len(df))
    return len(df)


def _read_source(file_name: str) -> pd.DataFrame:
    if USE_LOCAL:
        path = os.path.join(LOCAL_SOURCE_PATH, file_name)
        return pd.read_csv(path)

    import boto3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=file_name)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def _write_bronze(df: pd.DataFrame, execution_date: str):
    if USE_LOCAL:
        os.makedirs(LOCAL_BRONZE_PATH, exist_ok=True)
        out = os.path.join(LOCAL_BRONZE_PATH, f"network_metrics_{execution_date}.parquet")
        df.to_parquet(out, index=False)
        logger.info("Wrote bronze → %s", out)
        return

    import boto3
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_BRONZE_PREFIX}/{execution_date}/data.parquet",
        Body=buf.getvalue(),
    )


def read_bronze(execution_date: str) -> pd.DataFrame:
    """Used by downstream layers to load bronze data."""
    if USE_LOCAL:
        path = os.path.join(LOCAL_BRONZE_PATH, f"network_metrics_{execution_date}.parquet")
        return pd.read_parquet(path)

    import boto3, io
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{S3_BRONZE_PREFIX}/{execution_date}/data.parquet")
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))
