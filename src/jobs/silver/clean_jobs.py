from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.duckdb_client import DuckDBClient
from src.constants.job_fields import TECH_LIST, ROLE_LIST
from src.constants.paths import BRONZE_API_DATA_PATH, BRONZE_CRAWLER_DATA_PATH
from src.models.schema import SILVER_JOBS_SCHEMA, SILVER_COMPANIES_SCHEMA

logger = get_logger(__name__)


def clean_api_source_jobs(load_date: str = None) -> Dict[str, pa.Table]:
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Cleaning API source jobs for load_date={load_date}")

    with DuckDBClient() as client:
        bronze_path = f"{BRONZE_API_DATA_PATH}/load_date={load_date}/*.jsonl"

        # Clean jobs data
        jobs_query = f"""
            SELECT
                COALESCE(title, 'Unknown') as title,
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(organization)),
                            '\\s*(,?\\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\\.?|co\\.?))\\s*$',
                            '',
                            'g'
                        ),
                        ''
                    ),
                    'Unknown'
                ) AS company_id,
                COALESCE(source, 'Unknown') as source_name,
                CASE
                    WHEN remote_derived = true THEN 'REMOTE'
                    WHEN location_type IS NOT NULL THEN UPPER(location_type)
                    ELSE 'N/A'
                END as location_type,
                COALESCE(employment_type[1], 'Unknown') as employment_type,
                COALESCE(date_posted, 'Unknown') as date_posted,
                list_filter(
                    {TECH_LIST},
                    x -> regexp_matches(lower(COALESCE(title, '') || ' ' || COALESCE(organization, '')), '\\b' || x || '\\b')
                ) as techstacks,
                COALESCE(url, 'Unknown') as job_url,
                COALESCE(CAST(salary_raw AS VARCHAR), 'Unknown') as salary,
                COALESCE(seniority, 'Unknown') as level,
                list_filter(
                    {ROLE_LIST},
                    x -> regexp_matches(lower(title), '\\b' || x || '\\b')
                ) as role,
                COALESCE(CAST(0 AS INTEGER), 0) as number_applicants,
                CURRENT_TIMESTAMP as processed_at
            FROM (
                SELECT
                    CAST(title AS VARCHAR) AS title,
                    CAST(organization AS VARCHAR) AS organization,
                    CAST(source AS VARCHAR) AS source,
                    CAST(location_type AS VARCHAR) AS location_type,
                    employment_type,
                    CAST(date_posted AS VARCHAR) AS date_posted,
                    CAST(url AS VARCHAR) AS url,
                    CAST(salary_raw AS VARCHAR) AS salary_raw,
                    CAST(seniority AS VARCHAR) AS seniority,
                    TRY_CAST(remote_derived AS BOOLEAN) AS remote_derived
                FROM read_json_auto('{bronze_path}', ignore_errors=true, format='newline_delimited')
            ) raw_jobs
        """

        jobs_table = client.fetch_arrow_table(jobs_query)
        logger.info(f"Cleaned {jobs_table.num_rows} API jobs")

        # Clean companies data
        companies_query = f"""
            SELECT DISTINCT
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(organization)),
                            '\\s*(,?\\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\\.?|co\\.?))\\s*$',
                            '',
                            'g'
                        ),
                        ''
                    ),
                    'Unknown'
                ) AS id,
                COALESCE(organization, 'Unknown') as name,
                COALESCE(linkedin_org_industry, 'Unknown') as industry,
                COALESCE(linkedin_org_size, 'Unknown') as company_size,
                COALESCE(linkedin_org_headquarters, 'Unknown') as location,
                COALESCE(source, 'Unknown') as source_name
            FROM (
                SELECT
                    CAST(organization AS VARCHAR) AS organization,
                    CAST(linkedin_org_industry AS VARCHAR) AS linkedin_org_industry,
                    CAST(linkedin_org_size AS VARCHAR) AS linkedin_org_size,
                    CAST(linkedin_org_headquarters AS VARCHAR) AS linkedin_org_headquarters,
                    CAST(source AS VARCHAR) AS source
                FROM read_json_auto('{bronze_path}', ignore_errors=true, format='newline_delimited')
            ) raw_companies
        """

        companies_table = client.fetch_arrow_table(companies_query)
        logger.info(f"Cleaned {companies_table.num_rows} API companies")

    return {
        "jobs": jobs_table,
        "companies": companies_table
    }


def clean_scrapped_source_jobs(load_date: str = None) -> Dict[str, pa.Table]:
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Cleaning scrapped source jobs for load_date={load_date}")

    with DuckDBClient() as client:
        bronze_path = f"{BRONZE_CRAWLER_DATA_PATH}/load_date={load_date}/*.jsonl"

        # Clean jobs data
        jobs_query = f"""
            SELECT
                COALESCE(title, 'Unknown') as title,
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(company)),
                            '\\s*(,?\\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\\.?|co\\.?))\\s*$',
                            '',
                            'g'
                        ),
                        ''
                    ),
                    'Unknown'
                ) AS company_id,
                'linkedin_scraper' as source_name,
                COALESCE(UPPER(location_working_type), 'N/A') as location_type,
                COALESCE(working_type, 'Unknown') as employment_type,
                COALESCE(date_posted, 'Unknown') as date_posted,
                list_filter(
                    {TECH_LIST},
                    x -> regexp_matches(lower(COALESCE(description, '')), '\\b' || x || '\\b')
                ) as techstacks,
                COALESCE(job_url, 'Unknown') as job_url,
                COALESCE(salary, 'Unknown') as salary,
                COALESCE(level, 'Unknown') as level,
                list_filter(
                    {ROLE_LIST},
                    x -> regexp_matches(lower(COALESCE(role, '')), '\\b' || x || '\\b')
                ) as role,
                COALESCE(TRY_CAST(number_applicants AS INTEGER), 0) as number_applicants,
                CURRENT_TIMESTAMP as processed_at
            FROM (
                SELECT
                    CAST(title AS VARCHAR) AS title,
                    CAST(company AS VARCHAR) AS company,
                    CAST(location_working_type AS VARCHAR) AS location_working_type,
                    CAST(working_type AS VARCHAR) AS working_type,
                    CAST(date_posted AS VARCHAR) AS date_posted,
                    CAST(description AS VARCHAR) AS description,
                    CAST(job_url AS VARCHAR) AS job_url,
                    CAST(salary AS VARCHAR) AS salary,
                    CAST(level AS VARCHAR) AS level,
                    CAST(role AS VARCHAR) AS role,
                    CAST(number_applicants AS VARCHAR) AS number_applicants
                FROM read_json_auto('{bronze_path}', ignore_errors=true, format='newline_delimited')
            ) raw_jobs
        """

        jobs_table = client.fetch_arrow_table(jobs_query)
        logger.info(f"Cleaned {jobs_table.num_rows} scrapped jobs")

        # Clean companies data
        companies_query = f"""
            SELECT DISTINCT
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(company)),
                            '\\s*(,?\\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\\.?|co\\.?))\\s*$',
                            '',
                            'g'
                        ),
                        ''
                    ),
                    'Unknown'
                ) AS id,
                COALESCE(company, 'Unknown') as name,
                'Unknown' as industry,
                'Unknown' as company_size,
                COALESCE(company_location, 'Unknown') as location,
                'linkedin_scraper' as source_name
            FROM (
                SELECT
                    CAST(company AS VARCHAR) AS company,
                    CAST(company_location AS VARCHAR) AS company_location
                FROM read_json_auto('{bronze_path}', ignore_errors=true, format='newline_delimited')
            ) raw_companies
        """

        companies_table = client.fetch_arrow_table(companies_query)
        logger.info(f"Cleaned {companies_table.num_rows} scrapped companies")

    return {
        "jobs": jobs_table,
        "companies": companies_table
    }


def run(load_date: str = None, **context) -> Dict[str, int]:

    logger.info("Starting data cleaning")

    scrapped_data = clean_scrapped_source_jobs(load_date)

    result = {
        'api_jobs_cleaned': 0,
        'api_companies_cleaned': 0,
        'scrapped_jobs_cleaned': scrapped_data['jobs'].num_rows,
        'scrapped_companies_cleaned': scrapped_data['companies'].num_rows,
        'total_jobs': scrapped_data['jobs'].num_rows,
        'total_companies': scrapped_data['companies'].num_rows
    }

    logger.info(f"Cleaning completed: {result}")

    return result
