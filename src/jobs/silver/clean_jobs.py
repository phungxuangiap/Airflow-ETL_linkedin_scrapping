from datetime import datetime
from typing import Dict
import pyarrow as pa

from src.utils.logger import get_logger
from src.utils.duckdb_client import DuckDBClient
from src.constants.job_fields import (
    TECH_LIST,
    ROLE_LIST,
    EMPLOYMENT_TYPES,
    LOCATION_TYPES,
    SENIORITY_LEVELS,
)
from src.constants.paths import BRONZE_API_DATA_PATH, BRONZE_CRAWLER_DATA_PATH
from src.models.schema import SILVER_JOBS_SCHEMA, SILVER_COMPANIES_SCHEMA
from src.jobs.silver.job_field_expressions import (
    build_canonical_field_fallback_sql,
    build_employment_type_match_sql,
    build_experience_year_level_match_sql,
    build_first_keyword_match_sql,
    build_keyword_list_filter_sql,
    build_location_type_match_sql,
    build_salary_match_sql,
    build_seniority_level_match_sql,
    build_text_field_fallback_sql,
    build_unknown_fallback_sql,
)

logger = get_logger(__name__)


def clean_scrapped_source_jobs(load_date: str = None) -> Dict[str, pa.Table]:
    if load_date is None:
        load_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Cleaning scrapped source jobs for load_date={load_date}")

    with DuckDBClient() as client:
        bronze_path = f"{BRONZE_CRAWLER_DATA_PATH}/load_date={load_date}/*.jsonl"
        techstacks_expression = build_keyword_list_filter_sql("tech_keywords", "description")
        role_expression = build_first_keyword_match_sql("role_keywords", "title")
        employment_type_fallback_expression = build_employment_type_match_sql(
            "employment_type_keywords",
            "description",
            default="FULL_TIME",
        )
        employment_type_expression = build_canonical_field_fallback_sql(
            "working_type",
            build_employment_type_match_sql("employment_type_keywords", "working_type", default=""),
            employment_type_fallback_expression,
        )
        location_type_fallback_expression = build_location_type_match_sql(
            "location_type_keywords",
            "description",
            default="ON_SITE",
        )
        location_type_expression = build_canonical_field_fallback_sql(
            "location_working_type",
            build_location_type_match_sql("location_type_keywords", "location_working_type", default=""),
            location_type_fallback_expression,
        )
        level_keyword_expression = build_seniority_level_match_sql(
            "seniority_level_keywords",
            "description",
        )
        level_fallback_expression = build_unknown_fallback_sql(
            level_keyword_expression,
            build_experience_year_level_match_sql("description"),
        )
        level_expression = build_canonical_field_fallback_sql(
            "level",
            build_seniority_level_match_sql("seniority_level_keywords", "level", default=""),
            level_fallback_expression,
        )
        salary_expression = build_text_field_fallback_sql(
            "salary",
            build_salary_match_sql("description"),
        )
        company_id_expression = """
                MD5(
                    COALESCE(
                        NULLIF(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(LOWER(TRIM(company)), '[^[:alnum:]]+', '-', 'g'),
                                '(^-+|-+$)',
                                '',
                                'g'
                            ),
                            ''
                        ),
                        'unknown'
                    )
                )
        """
        date_posted_expression = """
                STRFTIME(
                    COALESCE(
                        TRY_CAST(NULLIF(TRIM(date_posted), '') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM(date_posted), ''), '%d-%m-%Y') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM(date_posted), ''), '%d/%m/%Y') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM(date_posted), ''), '%Y-%m-%d') AS DATE),
                        CAST(TRY_STRPTIME(NULLIF(TRIM(date_posted), ''), '%Y/%m/%d') AS DATE),
                        CAST(processed_at_value AS DATE)
                    ),
                    '%d-%m-%Y'
                )
        """

        # Clean jobs data
        jobs_query = f"""
            WITH keyword_lists AS (
                SELECT
                    {TECH_LIST} AS tech_keywords,
                    {ROLE_LIST} AS role_keywords,
                    {EMPLOYMENT_TYPES} AS employment_type_keywords,
                    {LOCATION_TYPES} AS location_type_keywords,
                    {SENIORITY_LEVELS} AS seniority_level_keywords,
                    CURRENT_TIMESTAMP AS processed_at_value
            )
            SELECT
                COALESCE(title, 'Unknown') as title,
                {company_id_expression} AS company_id,
                COALESCE(source_name, 'Unknown') as source_name,
                {location_type_expression} as location_type,
                {employment_type_expression} as employment_type,
                {date_posted_expression} as date_posted,
                {techstacks_expression} as techstacks,
                COALESCE(job_url, 'Unknown') as job_url,
                {salary_expression} as salary,
                {level_expression} as level,
                {role_expression} as role,
                COALESCE(TRY_CAST(number_applicants AS INTEGER), 0) as number_applicants,
                processed_at_value as processed_at
            FROM (
                SELECT
                    CAST(title AS VARCHAR) AS title,
                    CAST(company AS VARCHAR) AS company,
                    CAST(source_name AS VARCHAR) AS source_name,
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
                {company_id_expression} AS id,
                COALESCE(company, 'Unknown') as name,
                'Unknown' as industry,
                'Unknown' as company_size,
                COALESCE(company_location, 'Unknown') as location,
                'scraper' as source_name
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
