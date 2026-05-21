"""
Build dimension tables for Gold layer Star Schema
"""
from datetime import datetime, timedelta
from typing import Dict
import pyarrow as pa
import pyarrow.compute as pc

from src.utils.logger import get_logger
from src.utils.iceberg_client import get_iceberg_client
from src.utils.duckdb_client import DuckDBClient
from src.constants.table_names import SILVER_JOBS, SILVER_COMPANIES

logger = get_logger(__name__)


def build_dim_company() -> pa.Table:
    """Build dim_company from Silver companies table"""
    logger.info("Building dim_company")

    with DuckDBClient() as client:
        query = f"""
            SELECT DISTINCT
                id,
                name,
                location,
                industry,
                CAST(
                    CASE
                        WHEN company_size LIKE '%-%' THEN CAST(SPLIT_PART(company_size, '-', 1) AS INTEGER)
                        ELSE 0
                    END AS INTEGER
                ) as company_size
            FROM iceberg_scan('{SILVER_COMPANIES}')
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
        query = f"""
            SELECT DISTINCT
                MD5(source_name) as id,
                source_name
            FROM iceberg_scan('{SILVER_JOBS}')
            WHERE source_name != 'Unknown'
        """
        return client.fetch_arrow_table(query)


def build_dim_role() -> pa.Table:
    """Build dim_role from Silver jobs table"""
    logger.info("Building dim_role")

    with DuckDBClient() as client:
        query = f"""
            SELECT DISTINCT
                MD5(role_item) as id,
                role_item as role_name
            FROM (
                SELECT UNNEST(role) as role_item
                FROM iceberg_scan('{SILVER_JOBS}')
                WHERE role IS NOT NULL AND len(role) > 0
            )
        """
        return client.fetch_arrow_table(query)


def build_dim_level() -> pa.Table:
    """Build dim_level from Silver jobs table"""
    logger.info("Building dim_level")

    with DuckDBClient() as client:
        query = f"""
            SELECT DISTINCT
                MD5(level) as id,
                level as level_name
            FROM iceberg_scan('{SILVER_JOBS}')
            WHERE level != 'Unknown'
        """
        return client.fetch_arrow_table(query)


def build_dim_working_model() -> pa.Table:
    """Build dim_working_model from Silver jobs table"""
    logger.info("Building dim_working_model")

    with DuckDBClient() as client:
        query = f"""
            SELECT DISTINCT
                MD5(location_type) as id,
                location_type as name
            FROM iceberg_scan('{SILVER_JOBS}')
            WHERE location_type != 'N/A'
        """
        return client.fetch_arrow_table(query)


def build_dim_techstack() -> pa.Table:
    """Build dim_techstack from Silver jobs table"""
    logger.info("Building dim_techstack")

    with DuckDBClient() as client:
        query = f"""
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
                FROM iceberg_scan('{SILVER_JOBS}')
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
        'dim_company': dim_company.num_rows,
        'dim_location': dim_location.num_rows,
        'dim_date': dim_date.num_rows,
        'dim_source': dim_source.num_rows,
        'dim_role': dim_role.num_rows,
        'dim_level': dim_level.num_rows,
        'dim_working_model': dim_working_model.num_rows,
        'dim_techstack': dim_techstack.num_rows,
    }

    # Store in XCom for next task
    context['ti'].xcom_push(key='dim_company', value=dim_company)
    context['ti'].xcom_push(key='dim_location', value=dim_location)
    context['ti'].xcom_push(key='dim_date', value=dim_date)
    context['ti'].xcom_push(key='dim_source', value=dim_source)
    context['ti'].xcom_push(key='dim_role', value=dim_role)
    context['ti'].xcom_push(key='dim_level', value=dim_level)
    context['ti'].xcom_push(key='dim_working_model', value=dim_working_model)
    context['ti'].xcom_push(key='dim_techstack', value=dim_techstack)

    logger.info(f"Dimension tables built: {result}")

    return result
