from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.duckdb_client import DuckDBClient
from src.constants.job_fields import TECH_LIST, ROLE_LIST
from src.constants.paths import BRONZE_API_DATA_PATH, BRONZE_CRAWLER_DATA_PATH
from src.models.schema import SILVER_JOBS_SCHEMA, SILVER_COMPANIES_SCHEMA

logger = get_logger(__name__)


def _build_keyword_list_filter_sql(keyword_list, text_expression: str) -> str:
    """
    Build a DuckDB list_filter expression that matches canonical keywords against text.

    The matcher keeps the original keyword as the output value while matching both:
    - boundary-aware regex form, allowing separators between multi-word terms
    - compact form, removing separators to catch variants like front-end/frontend or node.js/nodejs
    """
    return f"""
                list_filter(
                    {keyword_list},
                    x ->
                        regexp_matches(
                            lower(COALESCE({text_expression}, '')),
                            '(^|[^[:alnum:]_])' ||
                            regexp_replace(x, '\\\\s+', '[\\\\s._/+:-]+', 'g') ||
                            '([^[:alnum:]_]|$)'
                        )
                        OR (
                            length(regexp_replace(x, '[^[:alnum:]#+]+', '', 'g')) > 4
                            AND strpos(
                                regexp_replace(lower(COALESCE({text_expression}, '')), '[^[:alnum:]#+]+', '', 'g'),
                                regexp_replace(x, '[^[:alnum:]#+]+', '', 'g')
                            ) > 0
                        )
                )
    """


def _build_first_keyword_match_sql(keyword_list, text_expression: str, default: str = "Unknown") -> str:
    """Build a DuckDB expression returning the first canonical keyword matched in text."""
    list_filter_expression = _build_keyword_list_filter_sql(keyword_list, text_expression)
    return f"COALESCE(NULLIF(({list_filter_expression})[1], ''), '{default}')"


def clean_scrapped_source_jobs(load_date: str = None) -> Dict[str, pa.Table]:
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Cleaning scrapped source jobs for load_date={load_date}")

    with DuckDBClient() as client:
        bronze_path = f"{BRONZE_CRAWLER_DATA_PATH}/load_date={load_date}/*.jsonl"
        techstacks_expression = _build_keyword_list_filter_sql("tech_keywords", "description")
        role_expression = _build_first_keyword_match_sql("role_keywords", "description")

        # Clean jobs data
        jobs_query = f"""
            WITH keyword_lists AS (
                SELECT
                    {TECH_LIST} AS tech_keywords,
                    {ROLE_LIST} AS role_keywords
            )
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
                {techstacks_expression} as techstacks,
                COALESCE(job_url, 'Unknown') as job_url,
                COALESCE(salary, 'Unknown') as salary,
                COALESCE(level, 'Unknown') as level,
                {role_expression} as role,
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
                    CAST(number_applicants AS VARCHAR) AS number_applicants
                FROM read_json_auto('{bronze_path}', ignore_errors=true, format='newline_delimited')
            ) raw_jobs
            CROSS JOIN keyword_lists
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
