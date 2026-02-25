"""
Schema Enforcement Module for Silver Layer.

Orchestrates the Bronze to Silver ETL pipeline using DuckDB and MinIO.
Handles:
1. Company Upsert logic
2. Job transformation and mapping
3. Iceberg table writes
4. Data quality validation
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from src.jobs.silver.bronze_to_silver_etl import (
    run_etl_pipeline,
    MinIOConfig
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def enforce_script_schema(
    minio_endpoint: str = "http://localhost:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
    minio_bucket: str = "airflow-bucket",
    load_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Enforce and transform Bronze data to Silver schema through DuckDB ETL.
    
    Performs complete pipeline:
    1. Extract from Bronze (S3 JSONL via MinIO)
    2. Company Upsert with Iceberg:
       - Extract unique companies
       - Match with existing silver.companies
       - Update metadata if exists, Insert if new (UUID)
    3. Job transformation:
       - Map fields according to Silver schema
       - JOIN with company IDs
       - Extract techstacks
    4. Write to Silver Iceberg tables
    
    Args:
        minio_endpoint: MinIO S3 endpoint (default: http://localhost:9000)
        minio_access_key: MinIO access key (default: minioadmin)
        minio_secret_key: MinIO secret key (default: minioadmin)
        minio_bucket: S3 bucket name (default: airflow-bucket)
        load_date: Date in format YYYY-MM-DD (default: today)
        
    Returns:
        Dictionary with pipeline execution results:
            - status: 'SUCCESS' or 'FAILED'
            - bronze_records: Number of records extracted
            - unique_companies: Number of unique companies
            - new_companies: Number of new companies inserted
            - updated_companies: Number of existing companies updated
            - silver_jobs: Number of jobs transformed
            - duration_seconds: Pipeline execution time
            - errors: List of error messages if any
            
    Example:
        >>> result = enforce_script_schema(
        ...     minio_endpoint="http://minio:9000",
        ...     load_date="2026-02-25"
        ... )
        >>> if result['status'] == 'SUCCESS':
        ...     print(f"Jobs: {result['silver_jobs']}")
    """
    logger.info("Starting Bronze to Silver ETL pipeline (DuckDB + MinIO)...")
    
    # Create MinIO configuration
    minio_config = MinIOConfig(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        bucket=minio_bucket,
        use_ssl=False,
        path_style=True
    )
    
    # Run ETL pipeline
    pipeline_results = run_etl_pipeline(minio_config, load_date)
    
    return pipeline_results


def enfoce_api_schema(
    minio_endpoint: str = "http://localhost:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
    minio_bucket: str = "airflow-bucket",
    load_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Enforce schema for API-sourced data (currently uses same pipeline as scrapped data).
    
    Can be extended in future for API-specific schema variations.
    
    Args:
        minio_endpoint: MinIO S3 endpoint
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        minio_bucket: S3 bucket name
        load_date: Date in format YYYY-MM-DD
        
    Returns:
        Dictionary with pipeline execution results
    """
    logger.info("Starting API data enforcement (DuckDB + MinIO)...")
    
    # Currently delegates to same pipeline as scrapped data
    return enforce_script_schema(
        minio_endpoint=minio_endpoint,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        minio_bucket=minio_bucket,
        load_date=load_date
    )


def get_pipeline_config() -> Dict[str, Any]:
    """
    Get current ETL pipeline configuration.
    
    Returns:
        Dictionary with current configuration
    """
    return {
        'duckdb': {
            'memory_limit': '4GB',
            'threads': 4,
            'extensions': ['httpfs', 'iceberg', 'json']
        },
        'minio': {
            'default_endpoint': 'http://localhost:9000',
            'default_bucket': 'airflow-bucket',
            'path_style': True
        },
        'bronze': {
            'format': 'jsonl',
            'path_template': 's3://{bucket}/BRONZE/crawler_data/linkedin/jobs/load_date={load_date}/'
        },
        'silver': {
            'companies_path': 's3://{bucket}/silver/companies/',
            'jobs_path': 's3://{bucket}/silver/jobs/',
            'format': 'iceberg'
        }
    }


