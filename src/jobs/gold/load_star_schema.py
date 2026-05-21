"""
Load dimension and fact tables to Gold layer Iceberg
"""
from datetime import datetime
from typing import Dict
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.constants.table_names import (
    GOLD_DIM_COMPANY,
    GOLD_DIM_LOCATION,
    GOLD_DIM_DATE,
    GOLD_DIM_SOURCE,
    GOLD_DIM_ROLE,
    GOLD_DIM_LEVEL,
    GOLD_DIM_WORKING_MODEL,
    GOLD_DIM_TECHSTACK,
    GOLD_FACT_HIRING,
    GOLD_BRIDGE_TECH_FACT
)

logger = get_logger(__name__)


def load_dimension_table(table_name: str, data: pa.Table, unique_key: str = "id") -> int:
    """
    Load dimension table with upsert logic

    Args:
        table_name: Iceberg table identifier
        data: PyArrow table with dimension data
        unique_key: Column name for deduplication

    Returns:
        Number of rows loaded
    """
    logger.info(f"Loading {data.num_rows} rows to {table_name}")

    client = get_iceberg_client()

    # Get or create table
    table = client.get_or_create_table(
        table_name,
        schema=data.schema,
        partition_spec=None  # Dimensions are not partitioned
    )

    # Upsert data
    rows_loaded = client.upsert_data(table_name, data, unique_key=unique_key)

    logger.info(f"Loaded {rows_loaded} rows to {table_name}")

    return rows_loaded


def load_fact_table(table_name: str, data: pa.Table, partition_field: str = None) -> int:
    """
    Load fact table with partitioning

    Args:
        table_name: Iceberg table identifier
        data: PyArrow table with fact data
        partition_field: Field to partition by

    Returns:
        Number of rows loaded
    """
    logger.info(f"Loading {data.num_rows} rows to {table_name}")

    client = get_iceberg_client()

    # Define partition spec if provided
    partition_spec = None
    if partition_field:
        partition_spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1, transform=IdentityTransform(), name=partition_field)
        )

    # Get or create table
    table = client.get_or_create_table(
        table_name,
        schema=data.schema,
        partition_spec=partition_spec
    )

    # Append data (facts are append-only)
    rows_loaded = client.append_data(table_name, data)

    logger.info(f"Loaded {rows_loaded} rows to {table_name}")

    return rows_loaded


def run(**context) -> Dict[str, int]:
    logger.info("Gold tables are loaded by the dimensions and fact build steps")

    result = {
        'status': 'completed',
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Gold layer load step completed: {result}")

    return result
