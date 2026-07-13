"""
Path constants for the ETL pipeline
"""
import os

# S3/MinIO bucket
BUCKET = os.getenv("BUCKET", "linkedin-jobs-prod")

# Bronze layer paths
BRONZE_API_DATA_PATH = f"s3://{BUCKET}/bronze/api_data/jobs"
BRONZE_CRAWLER_DATA_PATH = f"s3://{BUCKET}/bronze/crawler_data/linkedin/jobs"

# Staging layer paths
STAGING_BRONZE_API_DATA_PATH = f"s3://{BUCKET}/staging/bronze/api_data/jobs"
STAGING_BRONZE_CRAWLER_DATA_PATH = f"s3://{BUCKET}/staging/bronze/crawler_data/linkedin/jobs"

# Silver layer paths
SILVER_JOBS_PATH = f"s3://{BUCKET}/silver/jobs"
SILVER_COMPANIES_PATH = f"s3://{BUCKET}/silver/companies"

# Gold layer paths
GOLD_ANALYTICS_PATH = f"s3://{BUCKET}/gold/analytics"
GOLD_REPORTS_PATH = f"s3://{BUCKET}/gold/reports"

# Local paths (for development)
LOCAL_TMP_DIR = "/project_root/tmp"
LOCAL_API_SOURCE_DIR = f"{LOCAL_TMP_DIR}/api_sources"
LOCAL_SCRAPPED_DIR = f"{LOCAL_TMP_DIR}/scrapping_script"
