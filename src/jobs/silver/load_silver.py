from datetime import datetime
from typing import Dict
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.constants.table_names import SILVER_JOBS, SILVER_COMPANIES
from src.models.schema import SILVER_JOBS_SCHEMA, SILVER_COMPANIES_SCHEMA

logger = get_logger(__name__)


def load_jobs_to_silver(jobs_table: pa.Table) -> int:
    logger.info(f"Upserting {jobs_table.num_rows} jobs to Silver layer")

    client = get_iceberg_client()

    # Define partition spec for jobs
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1, transform=DayTransform(), name="processed_at_day")
    )

    # Get or create table
    table = client.get_or_create_table(
        SILVER_JOBS,
        schema=jobs_table.schema,
        partition_spec=partition_spec
    )

    # Upsert data (delete existing by job_url, then insert)
    rows_loaded = client.upsert_data(SILVER_JOBS, jobs_table, unique_key="job_url")

    logger.info(f"Upserted {rows_loaded} jobs to {SILVER_JOBS}")

    return rows_loaded


def load_companies_to_silver(companies_table: pa.Table) -> int:
    logger.info(f"Upserting {companies_table.num_rows} companies to Silver layer")

    client = get_iceberg_client()

    # Define partition spec for companies
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1, transform=IdentityTransform(), name="source_name")
    )

    # Get or create table
    table = client.get_or_create_table(
        SILVER_COMPANIES,
        schema=companies_table.schema,
        partition_spec=partition_spec
    )

    # Upsert data (delete existing by id, then insert)
    rows_loaded = client.upsert_data(SILVER_COMPANIES, companies_table, unique_key="id")

    logger.info(f"Upserted {rows_loaded} companies to {SILVER_COMPANIES}")

    return rows_loaded


def run(jobs_table: pa.Table, companies_table: pa.Table, **context) -> Dict[str, int]:
    logger.info("Starting Silver layer load")

    jobs_loaded = load_jobs_to_silver(jobs_table)
    companies_loaded = load_companies_to_silver(companies_table)

    result = {
        'jobs_loaded': jobs_loaded,
        'companies_loaded': companies_loaded,
        'total_rows': jobs_loaded + companies_loaded,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Silver layer load completed: {result}")

    return result
