from typing import Dict

import pyarrow as pa

from src.utils.iceberg_client import get_iceberg_client
from src.utils.logger import get_logger

logger = get_logger(__name__)


def load_table_as_arrow(table_name: str, allow_empty: bool = False) -> pa.Table:
    table = get_iceberg_client().load_table(table_name)
    if table is None:
        raise ValueError(f"Table {table_name} does not exist")

    data = table.scan().to_arrow()
    if data.num_rows == 0 and not allow_empty:
        raise ValueError(f"Table {table_name} has no staged rows")

    logger.info(f"Loaded {data.num_rows} rows from {table_name}")
    return data


def overwrite_staging_table(table_name: str, data: pa.Table, allow_empty: bool = False) -> int:
    if data.num_rows == 0 and not allow_empty:
        raise ValueError(f"Refusing to stage empty table for {table_name}")

    client = get_iceberg_client()
    try:
        client.get_or_create_table(table_name, schema=data.schema)
    except Exception as exc:
        if not table_name.startswith("staging."):
            raise

        logger.warning(
            "Recreating stale staging table %s after get/create failed: %s",
            table_name,
            exc,
        )
        client.recreate_table(table_name, schema=data.schema)

    rows_loaded = client.overwrite_data(table_name, data)

    logger.info(f"Staged {rows_loaded} rows to {table_name}")
    return rows_loaded


def overwrite_staging_tables(tables: Dict[str, pa.Table], allow_empty: bool = False) -> Dict[str, int]:
    return {
        table_name: overwrite_staging_table(table_name, data, allow_empty=allow_empty)
        for table_name, data in tables.items()
    }
