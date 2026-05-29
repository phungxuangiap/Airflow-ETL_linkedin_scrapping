"""
Build fact_hiring table for Gold layer Star Schema
"""
from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.utils.duckdb_client import DuckDBClient
from src.jobs.gold.load_star_schema import load_fact_table
from src.constants.table_names import (
    SILVER_JOBS,
    GOLD_FACT_HIRING,
    GOLD_BRIDGE_TECH_FACT,
    STAGING_GOLD_FACT_HIRING,
    STAGING_GOLD_BRIDGE_TECH_FACT,
)
from src.jobs.staging.utils import load_table_as_arrow, overwrite_staging_table

logger = get_logger(__name__)


def register_silver_jobs(client: DuckDBClient) -> None:
    table = get_iceberg_client().load_table(SILVER_JOBS)
    if table is None:
        raise ValueError(f"Table {SILVER_JOBS} does not exist")

    client.connect().register("silver_jobs", table.scan().to_arrow())


def build_fact_hiring(load_date: str = None) -> pa.Table:
    """
    Build fact_hiring table from Silver jobs

    Args:
        load_date: Load date in YYYY-MM-DD format

    Returns:
        PyArrow table with fact_hiring data
    """
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Building fact_hiring for {load_date}")

    with DuckDBClient() as client:
        register_silver_jobs(client)
        query = f"""
            SELECT
                MD5(job_url) as id,
                company_id,
                MD5(source_name) as source_id,
                CASE
                    WHEN location_type = 'REMOTE' THEN 'LOC_REMOTE'
                    WHEN location_type = 'ONSITE' THEN 'LOC_ONSITE'
                    WHEN location_type = 'HYBRID' THEN 'LOC_HYBRID'
                    ELSE 'LOC_NA'
                END as location_id,
                STRFTIME(CAST(processed_at AS DATE), '%Y%m%d') as date_id,
                CASE
                    WHEN len(role) > 0 THEN MD5(role[1])
                    ELSE NULL
                END as role_id,
                MD5(level) as level_id,
                MD5(location_type) as working_model,
                CAST(0 AS INTEGER) as salary_min_vnd,
                CAST(0 AS INTEGER) as salary_max_vnd,
                number_applicants as number_of_applicants,
                'ACTIVE' as job_status
            FROM silver_jobs
        """

        fact_table = client.fetch_arrow_table(query)
        logger.info(f"Built fact_hiring with {fact_table.num_rows} rows")

        return fact_table


def build_bridge_tech_fact(load_date: str = None) -> pa.Table:
    """
    Build bridge_tech_fact table (many-to-many relationship)

    Args:
        load_date: Load date in YYYY-MM-DD format

    Returns:
        PyArrow table with bridge data
    """
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Building bridge_tech_fact for {load_date}")

    with DuckDBClient() as client:
        register_silver_jobs(client)
        query = f"""
            SELECT
                MD5(tech_item) as techstack_id,
                MD5(job_url) as fact_id
            FROM (
                SELECT
                    job_url,
                    UNNEST(techstacks) as tech_item
                FROM silver_jobs
                WHERE techstacks IS NOT NULL
                  AND len(techstacks) > 0
            )
        """

        bridge_table = client.fetch_arrow_table(query)
        logger.info(f"Built bridge_tech_fact with {bridge_table.num_rows} rows")

        return bridge_table


def build_fact_batch(load_date: str = None) -> Dict[str, pa.Table]:
    logger.info("Building fact and bridge batch")

    return {
        STAGING_GOLD_FACT_HIRING: build_fact_hiring(load_date),
        STAGING_GOLD_BRIDGE_TECH_FACT: build_bridge_tech_fact(load_date),
    }


def process_fact_to_staging(load_date: str = None, **context) -> Dict[str, int]:
    logger.info(f"Starting Gold fact process to staging for load_date={load_date}")

    fact_tables = build_fact_batch(load_date)
    result = {
        staging_table: overwrite_staging_table(staging_table, data, allow_empty=True)
        for staging_table, data in fact_tables.items()
    }
    result['load_date'] = load_date
    result['timestamp'] = datetime.now().isoformat()

    logger.info(f"Gold fact staging completed: {result}")
    return result


def promote_fact_to_gold(load_date: str = None, **context) -> Dict[str, int]:
    logger.info(f"Promoting Gold fact staging to Gold for load_date={load_date}")

    fact_hiring = load_table_as_arrow(STAGING_GOLD_FACT_HIRING, allow_empty=True)
    bridge_tech_fact = load_table_as_arrow(STAGING_GOLD_BRIDGE_TECH_FACT, allow_empty=True)

    result = {
        'fact_hiring': load_fact_table(GOLD_FACT_HIRING, fact_hiring) if fact_hiring.num_rows > 0 else 0,
        'bridge_tech_fact': load_fact_table(GOLD_BRIDGE_TECH_FACT, bridge_tech_fact) if bridge_tech_fact.num_rows > 0 else 0,
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Gold fact promotion completed: {result}")
    return result


def run(load_date: str = None, **context) -> Dict[str, int]:
    staging_result = process_fact_to_staging(load_date=load_date, **context)
    promotion_result = promote_fact_to_gold(load_date=load_date, **context)

    return {
        'staging': staging_result,
        'promotion': promotion_result,
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }
