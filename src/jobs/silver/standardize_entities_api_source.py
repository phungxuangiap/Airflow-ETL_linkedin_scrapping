from constants.system_consts import ROLE_LIST, TECH_LIST
import duckdb
import os
from datetime import datetime

from models.silver import CompanySilver, JobSilver
def standardize_entities_api_source(
    load_date: str = datetime.now().strftime("%Y-%m-%d")
):
    con = duckdb.connect(database='temp_duckdb.duckdb')
    endpoint_host = os.getenv("S3_ENDPOINT", "http://minio:9000").replace('http://', '').replace('https://', '')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL json; LOAD json;")
    con.execute(f"""
        SET s3_endpoint = '{endpoint_host}';
        SET s3_access_key_id = '{os.getenv("S3_ACCESS_KEY")}';
        SET s3_secret_access_key = '{os.getenv("S3_SECRET_KEY")}';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)
    bronze_path = f"s3://{os.getenv('BUCKET')}/BRONZE/crawler_data/linkedin/jobs/load_date={load_date}/*.jsonl"
    try:
        query_standardize_jobs = f"""
            SELECT 
                COALESCE(title, 'Unknown') as {JobSilver.title},
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(company_name)), 
                            '\s*(,?\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\.?|co\.?))\s*$', 
                            '', 
                            'g'
                        ), 
                        ''
                    ), 
                    'Unknown'
                ) AS {JobSilver.company_id},
                COALESCE(company_name, 'Unknown') as company_name_raw,
                COALESCE(source, 'Unknown') as {JobSilver.source_name},
                COALESCE(remote_derived, 'N/A') as {JobSilver.location_type},
                COALESCE(employment_type[0], 'Unknown') as {JobSilver.employment_type},
                COALESCE(date_posted, 'Unknown') as {JobSilver.date_posted},
                list_filter(
                    {TECH_LIST}, 
                    x -> regexp_matches(lower(description), '\b' || x || '\b')
                ) as {JobSilver.techstacks},
                COALESCE(url, 'Unknown') as {JobSilver.job_url},
                COALESCE(salary_raw, 'Unknown') as {JobSilver.salary},
                COALESCE(seniority, 'Unknown') as {JobSilver.level},
                list_filter(
                    {ROLE_LIST}, 
                    x -> regexp_matches(lower(description), '\b' || x || '\b')
                ) as {JobSilver.role},
                COALESCE(CAST(number_applicants AS INTEGER), 0) as {JobSilver.number_applicants},
                CURRENT_TIMESTAMP as processed_at
            FROM read_json_auto('{bronze_path}', ignore_errors=True, format='newline_delimited')
        """
        jobs_arrow_table = con.execute(query_standardize_jobs).fetch_arrow_table()

        query_standardize_companies = f"""
            SELECT 
                COALESCE(
                    NULLIF(
                        REGEXP_REPLACE(
                            LOWER(TRIM(company_name)), 
                            '\s*(,?\s*(inc|co|ltd|llc|corp|tnhh|cổ phần|group|joint stock|ltd\.?|co\.?))\s*$', 
                            '', 
                            'g'
                        ), 
                        ''
                    ), 
                    'Unknown'
                ) AS {CompanySilver.id},
                COALESCE(organization, 'Unknown') as {CompanySilver.name},
                COALESCE(linkedin_org_industry, 'Unknown') as {CompanySilver.industry},
                COALESCE(linkedin_org_size, 'Unknown') as {CompanySilver.company_size},
                COALESCE(linkedin_org_headquarters, 'Unknown') as {CompanySilver.location}
            FROM read_json_auto('{bronze_path}', ignore_errors=True, format='newline_delimited')
        """
        companies_arrow_table = con.execute(query_standardize_companies).fetch_arrow_table()

    except Exception as e:
        print(f"Error processing data from API source: {e}")
    finally:
        con.close()
    return {
        "jobs": jobs_arrow_table,
        "companies": companies_arrow_table
    }