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
from src.constants.table_names import SILVER_JOBS, GOLD_FACT_HIRING, GOLD_BRIDGE_TECH_FACT

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
            WHERE CAST(processed_at AS DATE) = DATE '{load_date}'
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
                WHERE CAST(processed_at AS DATE) = DATE '{load_date}'
                  AND techstacks IS NOT NULL
                  AND len(techstacks) > 0
            )
        """

        bridge_table = client.fetch_arrow_table(query)
        logger.info(f"Built bridge_tech_fact with {bridge_table.num_rows} rows")

        return bridge_table


def run(load_date: str = None, **context) -> Dict[str, int]:
    """
    Build fact and bridge tables

    Args:
        load_date: Load date in YYYY-MM-DD format
        **context: Airflow context

    Returns:
        Dict with row counts
    """
    logger.info("Building fact and bridge tables")

    fact_hiring = build_fact_hiring(load_date)
    bridge_tech_fact = build_bridge_tech_fact(load_date)

    result = {
        'fact_hiring': load_fact_table(GOLD_FACT_HIRING, fact_hiring),
        'bridge_tech_fact': load_fact_table(GOLD_BRIDGE_TECH_FACT, bridge_tech_fact),
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Fact tables built: {result}")

    return result
