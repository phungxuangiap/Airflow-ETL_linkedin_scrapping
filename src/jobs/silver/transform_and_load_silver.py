from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.constants.table_names import SILVER_JOBS, SILVER_COMPANIES
from src.jobs.silver.clean_jobs import clean_api_source_jobs, clean_scrapped_source_jobs
from src.jobs.silver.deduplicate_jobs import deduplicate_jobs, deduplicate_companies
from src.jobs.silver.load_silver import load_jobs_to_silver, load_companies_to_silver

logger = get_logger(__name__)


def run(load_date: str = None, **context) -> Dict[str, int]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting Silver layer transformation for load_date={load_date}")

    logger.info("Step 1/5: Cleaning API source data...")
    api_data = clean_api_source_jobs(load_date)
    logger.info(f"  ✓ Cleaned {api_data['jobs'].num_rows} API jobs")
    logger.info(f"  ✓ Cleaned {api_data['companies'].num_rows} API companies")

    logger.info("Step 2/5: Cleaning scrapped source data...")
    scrapped_data = clean_scrapped_source_jobs(load_date)
    logger.info(f"  ✓ Cleaned {scrapped_data['jobs'].num_rows} scrapped jobs")
    logger.info(f"  ✓ Cleaned {scrapped_data['companies'].num_rows} scrapped companies")

    logger.info("Step 3/5: Merging data from both sources...")
    merged_jobs = pa.concat_tables([api_data['jobs'], scrapped_data['jobs']])
    merged_companies = pa.concat_tables([api_data['companies'], scrapped_data['companies']])
    logger.info(f"  ✓ Merged {merged_jobs.num_rows} total jobs")
    logger.info(f"  ✓ Merged {merged_companies.num_rows} total companies")

    logger.info("Step 4/5: Deduplicating data...")
    deduped_jobs = deduplicate_jobs(merged_jobs)
    deduped_companies = deduplicate_companies(merged_companies)

    jobs_removed = merged_jobs.num_rows - deduped_jobs.num_rows
    companies_removed = merged_companies.num_rows - deduped_companies.num_rows

    logger.info(f"  ✓ Removed {jobs_removed} duplicate jobs")
    logger.info(f"  ✓ Removed {companies_removed} duplicate companies")
    logger.info(f"  ✓ Final: {deduped_jobs.num_rows} unique jobs")
    logger.info(f"  ✓ Final: {deduped_companies.num_rows} unique companies")

    logger.info("Step 5/5: Loading to Silver layer (Iceberg)...")
    jobs_loaded = load_jobs_to_silver(deduped_jobs)
    companies_loaded = load_companies_to_silver(deduped_companies)

    logger.info(f"  ✓ Loaded {jobs_loaded} jobs to {SILVER_JOBS}")
    logger.info(f"  ✓ Loaded {companies_loaded} companies to {SILVER_COMPANIES}")

    result = {
        # Input stats
        'api_jobs_input': api_data['jobs'].num_rows,
        'api_companies_input': api_data['companies'].num_rows,
        'scrapped_jobs_input': scrapped_data['jobs'].num_rows,
        'scrapped_companies_input': scrapped_data['companies'].num_rows,

        # Merged stats
        'merged_jobs': merged_jobs.num_rows,
        'merged_companies': merged_companies.num_rows,

        # Deduplication stats
        'duplicate_jobs_removed': jobs_removed,
        'duplicate_companies_removed': companies_removed,

        # Final output
        'jobs_loaded': jobs_loaded,
        'companies_loaded': companies_loaded,
        'total_records_loaded': jobs_loaded + companies_loaded,

        # Metadata
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info("=" * 70)
    logger.info("Silver layer transformation completed successfully!")
    logger.info(f"  Input:  {result['merged_jobs']} jobs, {result['merged_companies']} companies")
    logger.info(f"  Removed: {result['duplicate_jobs_removed']} duplicate jobs, {result['duplicate_companies_removed']} duplicate companies")
    logger.info(f"  Output: {result['jobs_loaded']} jobs, {result['companies_loaded']} companies")
    logger.info("=" * 70)

    return result
