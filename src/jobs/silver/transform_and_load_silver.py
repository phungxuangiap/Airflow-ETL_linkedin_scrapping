from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.constants.table_names import (
    SILVER_JOBS,
    SILVER_COMPANIES,
    STAGING_SILVER_JOBS,
    STAGING_SILVER_COMPANIES,
)
from src.jobs.staging.utils import load_table_as_arrow, overwrite_staging_table
from src.jobs.silver.clean_jobs import clean_api_source_jobs, clean_scrapped_source_jobs
from src.jobs.silver.deduplicate_jobs import deduplicate_jobs, deduplicate_companies
from src.jobs.silver.load_silver import load_jobs_to_silver, load_companies_to_silver

logger = get_logger(__name__)


def build_silver_batch(load_date: str) -> Dict[str, pa.Table]:
    logger.info(f"Building Silver batch for load_date={load_date}")

    logger.info("Step 1/4: Cleaning API source data...")
    api_data = clean_api_source_jobs(load_date)
    logger.info(f"  ✓ Cleaned {api_data['jobs'].num_rows} API jobs")
    logger.info(f"  ✓ Cleaned {api_data['companies'].num_rows} API companies")

    logger.info("Step 2/4: Cleaning scrapped source data...")
    scrapped_data = clean_scrapped_source_jobs(load_date)
    logger.info(f"  ✓ Cleaned {scrapped_data['jobs'].num_rows} scrapped jobs")
    logger.info(f"  ✓ Cleaned {scrapped_data['companies'].num_rows} scrapped companies")

    logger.info("Step 3/4: Merging data from both sources...")
    merged_jobs = pa.concat_tables([api_data['jobs'], scrapped_data['jobs']])
    merged_companies = pa.concat_tables([api_data['companies'], scrapped_data['companies']])
    logger.info(f"  ✓ Merged {merged_jobs.num_rows} total jobs")
    logger.info(f"  ✓ Merged {merged_companies.num_rows} total companies")

    logger.info("Step 4/4: Deduplicating data...")
    deduped_jobs = deduplicate_jobs(merged_jobs)
    deduped_companies = deduplicate_companies(merged_companies)

    jobs_removed = merged_jobs.num_rows - deduped_jobs.num_rows
    companies_removed = merged_companies.num_rows - deduped_companies.num_rows

    logger.info(f"  ✓ Removed {jobs_removed} duplicate jobs")
    logger.info(f"  ✓ Removed {companies_removed} duplicate companies")
    logger.info(f"  ✓ Final: {deduped_jobs.num_rows} unique jobs")
    logger.info(f"  ✓ Final: {deduped_companies.num_rows} unique companies")

    return {
        'jobs': deduped_jobs,
        'companies': deduped_companies,
        'api_jobs_input': api_data['jobs'].num_rows,
        'api_companies_input': api_data['companies'].num_rows,
        'scrapped_jobs_input': scrapped_data['jobs'].num_rows,
        'scrapped_companies_input': scrapped_data['companies'].num_rows,
        'merged_jobs': merged_jobs.num_rows,
        'merged_companies': merged_companies.num_rows,
        'duplicate_jobs_removed': jobs_removed,
        'duplicate_companies_removed': companies_removed,
    }


def process_to_staging(load_date: str = None, **context) -> Dict[str, int]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting Silver process to staging for load_date={load_date}")

    batch = build_silver_batch(load_date)
    jobs_staged = overwrite_staging_table(STAGING_SILVER_JOBS, batch['jobs'])
    companies_staged = overwrite_staging_table(STAGING_SILVER_COMPANIES, batch['companies'])

    result = {
        'api_jobs_input': batch['api_jobs_input'],
        'api_companies_input': batch['api_companies_input'],
        'scrapped_jobs_input': batch['scrapped_jobs_input'],
        'scrapped_companies_input': batch['scrapped_companies_input'],
        'merged_jobs': batch['merged_jobs'],
        'merged_companies': batch['merged_companies'],
        'duplicate_jobs_removed': batch['duplicate_jobs_removed'],
        'duplicate_companies_removed': batch['duplicate_companies_removed'],
        'jobs_staged': jobs_staged,
        'companies_staged': companies_staged,
        'total_records_staged': jobs_staged + companies_staged,
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Silver staging completed: {result}")
    return result


def promote_staging_to_silver(load_date: str = None, **context) -> Dict[str, int]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Promoting Silver staging to Silver for load_date={load_date}")

    jobs_table = load_table_as_arrow(STAGING_SILVER_JOBS)
    companies_table = load_table_as_arrow(STAGING_SILVER_COMPANIES)

    jobs_loaded = load_jobs_to_silver(jobs_table)
    companies_loaded = load_companies_to_silver(companies_table)

    result = {
        'jobs_loaded': jobs_loaded,
        'companies_loaded': companies_loaded,
        'total_records_loaded': jobs_loaded + companies_loaded,
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Silver promotion completed: {result}")
    return result


def run(load_date: str = None, **context) -> Dict[str, int]:
    staging_result = process_to_staging(load_date=load_date, **context)
    promotion_result = promote_staging_to_silver(load_date=staging_result['load_date'], **context)

    return {
        'staging': staging_result,
        'promotion': promotion_result,
        'load_date': staging_result['load_date'],
        'timestamp': datetime.now().isoformat()
    }
