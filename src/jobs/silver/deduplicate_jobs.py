from typing import Dict
import pyarrow as pa
import pyarrow.compute as pc

from src.utils.logger import get_logger

logger = get_logger(__name__)


def deduplicate_jobs(jobs_table: pa.Table) -> pa.Table:
    initial_count = jobs_table.num_rows
    logger.info(f"Deduplicating {initial_count} jobs")

    # Convert to pandas for deduplication (easier)
    df = jobs_table.to_pandas()

    # Remove duplicates based on job_url, keeping the first occurrence
    df_deduped = df.drop_duplicates(subset=['job_url', 'company_id', 'date_posted'], keep='first')

    # Convert back to PyArrow
    deduped_table = pa.Table.from_pandas(df_deduped, schema=jobs_table.schema)

    duplicates_removed = initial_count - deduped_table.num_rows
    logger.info(f"Removed {duplicates_removed} duplicate jobs, {deduped_table.num_rows} remaining")

    return deduped_table


def deduplicate_companies(companies_table: pa.Table) -> pa.Table:
    initial_count = companies_table.num_rows
    logger.info(f"Deduplicating {initial_count} companies")

    # Convert to pandas for deduplication
    df = companies_table.to_pandas()

    # Remove duplicates based on id, keeping the first occurrence
    df_deduped = df.drop_duplicates(subset=['id'], keep='first')

    # Convert back to PyArrow
    deduped_table = pa.Table.from_pandas(df_deduped, schema=companies_table.schema)

    duplicates_removed = initial_count - deduped_table.num_rows
    logger.info(f"Removed {duplicates_removed} duplicate companies, {deduped_table.num_rows} remaining")

    return deduped_table


def run(jobs_table: pa.Table, companies_table: pa.Table, **context) -> Dict[str, pa.Table]:
    logger.info("Starting deduplication")

    deduped_jobs = deduplicate_jobs(jobs_table)
    deduped_companies = deduplicate_companies(companies_table)

    result = {
        'jobs': deduped_jobs,
        'companies': deduped_companies,
        'jobs_count': deduped_jobs.num_rows,
        'companies_count': deduped_companies.num_rows
    }

    logger.info(f"Deduplication completed: {result['jobs_count']} jobs, {result['companies_count']} companies")

    return result
