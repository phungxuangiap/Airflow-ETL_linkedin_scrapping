"""
Build dimension tables for Gold layer Star Schema
"""
from datetime import datetime, timedelta
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.utils.duckdb_client import DuckDBClient
from src.jobs.gold.load_star_schema import load_dimension_table
from src.constants.table_names import (
    SILVER_JOBS,
    SILVER_COMPANIES,
    GOLD_DIM_COMPANY,
    GOLD_DIM_LOCATION,
    GOLD_DIM_DATE,
    GOLD_DIM_SOURCE,
    GOLD_DIM_ROLE,
    GOLD_DIM_LEVEL,
    GOLD_DIM_WORKING_MODEL,
    GOLD_DIM_TECHSTACK,
)

logger = get_logger(__name__)


def register_silver_table(client: DuckDBClient, table_name: str, view_name: str) -> None:
    table = get_iceberg_client().load_table(table_name)
    if table is None:
        raise ValueError(f"Table {table_name} does not exist")

    client.connect().register(view_name, table.scan().to_arrow())


def build_dim_company() -> pa.Table:
    """Build dim_company from Silver companies table"""
    logger.info("Building dim_company")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_COMPANIES, "silver_companies")
        query = """
            SELECT DISTINCT
                id,
                name,
                location,
                industry,
                COALESCE(
                    TRY_CAST(REPLACE(SPLIT_PART(company_size, '-', 1), ',', '') AS INTEGER),
                    0
                ) as company_size
            FROM silver_companies
            WHERE id != 'Unknown'
        """
        return client.fetch_arrow_table(query)


def build_dim_location() -> pa.Table:
    """Build dim_location from Silver jobs table"""
    logger.info("Building dim_location")

    # For now, create simple location dimension from location_type
    # In production, you'd parse actual location strings
    locations = [
        {'id': 'LOC_REMOTE', 'country': 'Remote', 'district': 'N/A', 'city': 'N/A'},
        {'id': 'LOC_ONSITE', 'country': 'Vietnam', 'district': 'N/A', 'city': 'N/A'},
        {'id': 'LOC_HYBRID', 'country': 'Vietnam', 'district': 'N/A', 'city': 'N/A'},
        {'id': 'LOC_NA', 'country': 'N/A', 'district': 'N/A', 'city': 'N/A'},
    ]

    return pa.Table.from_pylist(locations)


def build_dim_date(start_date: str = None, days: int = 365) -> pa.Table:
    """
    Build dim_date dimension table

    Args:
        start_date: Start date in YYYY-MM-DD format (default: today - 30 days)
        days: Number of days to generate (default: 365)
    """
    logger.info("Building dim_date")

    if start_date is None:
        start = datetime.now() - timedelta(days=30)
    else:
        start = datetime.strptime(start_date, "%Y-%m-%d")

    dates = []
    for i in range(days):
        date = start + timedelta(days=i)
        dates.append({
            'id': date.strftime('%Y%m%d'),
            'full_date': date.date(),
            'day_of_week': date.strftime('%A'),
            'month': date.month,
            'quarter': (date.month - 1) // 3 + 1,
            'year': date.year
        })

    return pa.Table.from_pylist(dates)


def build_dim_source() -> pa.Table:
    """Build dim_source from Silver jobs table"""
    logger.info("Building dim_source")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_JOBS, "silver_jobs")
        query = """
            SELECT DISTINCT
                MD5(source_name) as id,
                source_name
            FROM silver_jobs
            WHERE source_name != 'Unknown'
        """
        return client.fetch_arrow_table(query)


def build_dim_role() -> pa.Table:
    """Build dim_role from Silver jobs table"""
    logger.info("Building dim_role")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_JOBS, "silver_jobs")
        query = """
            SELECT DISTINCT
                MD5(role_item) as id,
                role_item as role_name
            FROM (
                SELECT UNNEST(role) as role_item
                FROM silver_jobs
                WHERE role IS NOT NULL AND len(role) > 0
            )
        """
        return client.fetch_arrow_table(query)


def build_dim_level() -> pa.Table:
    """Build dim_level from Silver jobs table"""
    logger.info("Building dim_level")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_JOBS, "silver_jobs")
        query = """
            SELECT DISTINCT
                MD5(level) as id,
                level as level_name
            FROM silver_jobs
            WHERE level != 'Unknown'
        """
        return client.fetch_arrow_table(query)


def build_dim_working_model() -> pa.Table:
    """Build dim_working_model from Silver jobs table"""
    logger.info("Building dim_working_model")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_JOBS, "silver_jobs")
        query = """
            SELECT DISTINCT
                MD5(location_type) as id,
                location_type as name
            FROM silver_jobs
            WHERE location_type != 'N/A'
        """
        return client.fetch_arrow_table(query)


def build_dim_techstack() -> pa.Table:
    """Build dim_techstack from Silver jobs table"""
    logger.info("Building dim_techstack")

    with DuckDBClient() as client:
        register_silver_table(client, SILVER_JOBS, "silver_jobs")
        query = """
            SELECT DISTINCT
                MD5(tech_item) as id,
                tech_item as tech_name,
                CASE
                    WHEN tech_item IN ('python', 'java', 'javascript', 'typescript', 'go', 'rust', 'c++') THEN 'Programming Language'
                    WHEN tech_item IN ('react', 'vue', 'angular', 'nextjs') THEN 'Frontend Framework'
                    WHEN tech_item IN ('django', 'flask', 'spring', 'express') THEN 'Backend Framework'
                    WHEN tech_item IN ('postgresql', 'mysql', 'mongodb', 'redis') THEN 'Database'
                    WHEN tech_item IN ('aws', 'gcp', 'azure', 'docker', 'kubernetes') THEN 'Cloud/DevOps'
                    ELSE 'Other'
                END as category
            FROM (
                SELECT UNNEST(techstacks) as tech_item
                FROM silver_jobs
                WHERE techstacks IS NOT NULL AND len(techstacks) > 0
            )
        """
        return client.fetch_arrow_table(query)


def run(**context) -> Dict[str, int]:
    """
    Build all dimension tables

    Returns:
        Dict with row counts for each dimension
    """
    logger.info("Building all dimension tables")

    dim_company = build_dim_company()
    dim_location = build_dim_location()
    dim_date = build_dim_date()
    dim_source = build_dim_source()
    dim_role = build_dim_role()
    dim_level = build_dim_level()
    dim_working_model = build_dim_working_model()
    dim_techstack = build_dim_techstack()

    result = {
        'dim_company': load_dimension_table(GOLD_DIM_COMPANY, dim_company),
        'dim_location': load_dimension_table(GOLD_DIM_LOCATION, dim_location),
        'dim_date': load_dimension_table(GOLD_DIM_DATE, dim_date),
        'dim_source': load_dimension_table(GOLD_DIM_SOURCE, dim_source),
        'dim_role': load_dimension_table(GOLD_DIM_ROLE, dim_role),
        'dim_level': load_dimension_table(GOLD_DIM_LEVEL, dim_level),
        'dim_working_model': load_dimension_table(GOLD_DIM_WORKING_MODEL, dim_working_model),
        'dim_techstack': load_dimension_table(GOLD_DIM_TECHSTACK, dim_techstack),
    }

    logger.info(f"Dimension tables built: {result}")

    return result
