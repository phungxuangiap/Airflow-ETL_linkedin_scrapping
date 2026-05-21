"""
Path constants for the ETL pipeline
"""
import os

# S3/MinIO bucket
BUCKET = os.getenv("BUCKET", "airflow-bucket")

# Bronze layer paths
BRONZE_API_DATA_PATH = f"s3://{BUCKET}/BRONZE/api_data/jobs"
BRONZE_CRAWLER_DATA_PATH = f"s3://{BUCKET}/BRONZE/crawler_data/linkedin/jobs"

# Silver layer paths
SILVER_JOBS_PATH = f"s3://{BUCKET}/SILVER/jobs"
SILVER_COMPANIES_PATH = f"s3://{BUCKET}/SILVER/companies"

# Gold layer paths
GOLD_ANALYTICS_PATH = f"s3://{BUCKET}/GOLD/analytics"
GOLD_REPORTS_PATH = f"s3://{BUCKET}/GOLD/reports"

# Local paths (for development)
LOCAL_TMP_DIR = "/project_root/tmp"
LOCAL_API_SOURCE_DIR = f"{LOCAL_TMP_DIR}/api_sources"
LOCAL_SCRAPPED_DIR = f"{LOCAL_TMP_DIR}/scrapping_script"
