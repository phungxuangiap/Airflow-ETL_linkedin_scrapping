from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_values

from src.configs.postgres_config import get_ingestion_postgres_dsn
from src.utils.logger import get_logger


LOGGER = get_logger(__name__)
RAW_JOB_COLUMNS = [
    "source_name",
    "title",
    "company",
    "company_location",
    "location_working_type",
    "working_type",
    "date_posted",
    "number_applicants",
    "job_url",
    "description",
    "salary",
    "role",
    "level",
]


def _job_values(job: Dict[str, Optional[str]]) -> tuple:
    return tuple(job.get(column) for column in RAW_JOB_COLUMNS)


def insert_raw_jobs(jobs: List[Dict[str, Optional[str]]]) -> int:
    if not jobs:
        LOGGER.info("No raw jobs to insert into PostgreSQL")
        return 0

    rows = [_job_values(job) for job in jobs if job.get("job_url")]
    if not rows:
        LOGGER.warning("Skip PostgreSQL insert because no jobs have job_url")
        return 0

    columns_sql = ", ".join(RAW_JOB_COLUMNS)
    update_sql = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in RAW_JOB_COLUMNS
        if column != "job_url"
    )
    query = f"""
        INSERT INTO raw_jobs ({columns_sql})
        VALUES %s
        ON CONFLICT (job_url) DO UPDATE SET
            {update_sql},
            updated_at = now()
    """

    with psycopg2.connect(get_ingestion_postgres_dsn()) as connection:
        with connection.cursor() as cursor:
            execute_values(cursor, query, rows)

    LOGGER.info("Upserted %s raw jobs into PostgreSQL", len(rows))
    return len(rows)
