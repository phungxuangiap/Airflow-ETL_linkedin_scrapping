from datetime import datetime
from typing import Dict

import pyarrow as pa

from src.constants.table_names import (
    STAGING_SILVER_JOBS,
    STAGING_SILVER_COMPANIES,
    STAGING_GOLD_DIM_COMPANY,
    STAGING_GOLD_DIM_LOCATION,
    STAGING_GOLD_DIM_DATE,
    STAGING_GOLD_DIM_SOURCE,
    STAGING_GOLD_DIM_ROLE,
    STAGING_GOLD_DIM_LEVEL,
    STAGING_GOLD_DIM_WORKING_MODEL,
    STAGING_GOLD_DIM_TECHSTACK,
    STAGING_GOLD_FACT_HIRING,
    STAGING_GOLD_BRIDGE_TECH_FACT,
)
from src.utils.iceberg_client import get_iceberg_client
from src.utils.logger import get_logger
from src.utils.minio_client import get_minio_client

logger = get_logger(__name__)

STAGING_TABLES = [
    STAGING_SILVER_JOBS,
    STAGING_SILVER_COMPANIES,
    STAGING_GOLD_DIM_COMPANY,
    STAGING_GOLD_DIM_LOCATION,
    STAGING_GOLD_DIM_DATE,
    STAGING_GOLD_DIM_SOURCE,
    STAGING_GOLD_DIM_ROLE,
    STAGING_GOLD_DIM_LEVEL,
    STAGING_GOLD_DIM_WORKING_MODEL,
    STAGING_GOLD_DIM_TECHSTACK,
    STAGING_GOLD_FACT_HIRING,
    STAGING_GOLD_BRIDGE_TECH_FACT,
]


def clean_staging_objects() -> Dict[str, int]:
    client = get_minio_client()
    prefix = "staging/"
    objects = client.list_objects(prefix=prefix, recursive=True)

    deleted = 0
    for object_name in objects:
        if client.delete_object(object_name):
            deleted += 1

    logger.info(f"Deleted {deleted} objects under {prefix}")
    return {prefix: deleted}


def clean_iceberg_staging_tables() -> Dict[str, int]:
    client = get_iceberg_client()
    result = {}

    for table_name in STAGING_TABLES:
        table = client.load_table(table_name)
        if table is None:
            result[table_name] = 0
            logger.info(f"Skipping missing staging table {table_name}")
            continue

        empty_table = pa.Table.from_batches([], schema=table.schema().as_arrow())
        result[table_name] = client.overwrite_data(table_name, empty_table)
        logger.info(f"Cleared staging table {table_name}")

    return result


def run(load_date: str = None, **context) -> Dict[str, object]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Starting staging cleanup for load_date={load_date}")

    result = {
        'staging_objects_deleted': clean_staging_objects(),
        'iceberg_tables_cleared': clean_iceberg_staging_tables(),
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Staging cleanup completed: {result}")
    return result
