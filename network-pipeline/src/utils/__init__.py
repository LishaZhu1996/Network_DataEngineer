"""
Iceberg catalog helpers shared across all processing layers.
"""
import os
import logging
from typing import List, Optional

import pyarrow as pa
import pandas as pd

logger = logging.getLogger(__name__)


def get_catalog():
    """Return a PyIceberg catalog based on ICEBERG_CATALOG_TYPE env var."""
    # Import here so the rest of the codebase doesn't hard-depend on pyiceberg
    # being installed (useful if running DQ-only tests locally).
    from config import ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE, USE_LOCAL

    if ICEBERG_CATALOG_TYPE == "sql" or USE_LOCAL:
        from pyiceberg.catalog.sql import SqlCatalog
        warehouse_abs = os.path.abspath(ICEBERG_WAREHOUSE)
        os.makedirs(warehouse_abs, exist_ok=True)
        return SqlCatalog(
            "local",
            **{
                "uri": ICEBERG_CATALOG_URI,
                "warehouse": f"file://{warehouse_abs}",
            },
        )

    # AWS Glue catalog for production
    from pyiceberg.catalog.glue import GlueCatalog
    from config import S3_BUCKET
    return GlueCatalog("glue", warehouse=f"s3://{S3_BUCKET}/warehouse")


def write_iceberg(
    df: pd.DataFrame,
    namespace: str,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    overwrite_filter: Optional[dict] = None,
) -> None:
    """
    Write a DataFrame to an Iceberg table.

    overwrite_filter: dict of {column: value} used to delete matching rows
                      before appending (used for backfill / date-range reload).
    """
    from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

    catalog = get_catalog()
    full_name = f"{namespace}.{table_name}"
    pa_table = pa.Table.from_pandas(df, preserve_index=False)

    # Ensure namespace exists
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Create table on first write
    try:
        ice_table = catalog.load_table(full_name)
    except NoSuchTableError:
        if partition_cols:
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform
            ice_schema = pa_table.schema
            from pyiceberg.schema import Schema
            from pyiceberg.io.pyarrow import pyarrow_to_schema
            iceberg_schema = pyarrow_to_schema(ice_schema)
            fields = [
                PartitionField(
                    source_id=iceberg_schema.find_field(col).field_id,
                    field_id=1000 + i,
                    transform=IdentityTransform(),
                    name=col,
                )
                for i, col in enumerate(partition_cols)
            ]
            partition_spec = PartitionSpec(*fields)
            ice_table = catalog.create_table(full_name, schema=iceberg_schema, partition_spec=partition_spec)
        else:
            ice_table = catalog.create_table(full_name, schema=pa_table.schema)
        logger.info("Created Iceberg table %s (partitioned by %s)", full_name, partition_cols)

    # Delete rows matching overwrite filter before re-inserting (backfill)
    if overwrite_filter:
        _delete_rows(ice_table, overwrite_filter)

    ice_table.append(pa_table)
    logger.info("Wrote %d rows to Iceberg table %s", len(df), full_name)


def read_iceberg(namespace: str, table_name: str, filters: Optional[dict] = None) -> pd.DataFrame:
    """Read an Iceberg table into a DataFrame, optionally filtered."""
    from pyiceberg.exceptions import NoSuchTableError

    catalog = get_catalog()
    full_name = f"{namespace}.{table_name}"
    try:
        ice_table = catalog.load_table(full_name)
    except NoSuchTableError:
        return pd.DataFrame()

    scan = ice_table.scan()
    if filters:
        import pyiceberg.expressions as expr
        conditions = [expr.EqualTo(col, val) for col, val in filters.items()]
        for condition in conditions:
            scan = scan.filter(condition)

    return scan.to_pandas()


def _delete_rows(ice_table, filters: dict) -> None:
    import pyiceberg.expressions as expr
    conditions = [expr.EqualTo(col, val) for col, val in filters.items()]
    for condition in conditions:
        ice_table.delete(condition)
    logger.info("Deleted rows matching %s from %s", filters, ice_table.name())
