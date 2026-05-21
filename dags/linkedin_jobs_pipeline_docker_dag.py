"""
LinkedIn Jobs ETL Pipeline - Docker Operator Version
Separates ETL execution from Airflow to avoid dependency conflicts
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Shared Docker configuration
docker_config = {
    'image': 'linkedin-etl:latest',
    'api_version': 'auto',
    'auto_remove': True,  # Auto-remove container after execution
    'docker_url': 'unix://var/run/docker.sock',
    'network_mode': 'data-pipeline-network',  # Connect to MinIO, PostgreSQL
    'mount_tmp_dir': False,
    'environment': {
        # S3/MinIO Configuration
        'AWS_ENDPOINT_URL': 'http://minio:9000',
        'AWS_ACCESS_KEY_ID': 'minioadmin',
        'AWS_SECRET_ACCESS_KEY': 'minioadmin',
        'AWS_S3_BUCKET': 'airflow-bucket',
        'AWS_REGION': 'us-east-1',
        'S3_ENDPOINT': 'http://minio:9000',
        'S3_ACCESS_KEY': 'minioadmin',
        'S3_SECRET_KEY': 'minioadmin',
        'S3_REGION': 'us-east-1',

        # Iceberg SQL Catalog Configuration
        'ICEBERG_CATALOG_TYPE': 'sql',
        'ICEBERG_CATALOG_URI': 'postgresql://iceberg:iceberg123@postgres-iceberg:5432/iceberg_catalog',
        'ICEBERG_CATALOG_NAME': 'lakehouse',
        'ICEBERG_WAREHOUSE_PATH': 's3://airflow-bucket/warehouse',

        # Paths
        'BUCKET': 'airflow-bucket',
        'BRONZE_PATH': 's3://airflow-bucket/BRONZE/crawler_data/linkedin/jobs',
        'SILVER_PATH': 's3://airflow-bucket/silver',
        'GOLD_PATH': 's3://airflow-bucket/gold/analytics',
    },
}

# Define DAG
with DAG(
    'linkedin_jobs_pipeline_docker_v03',
    default_args=default_args,
    description='LinkedIn Jobs ETL Pipeline using Docker Operator',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['etl', 'linkedin', 'docker', 'medallion'],
    max_active_runs=1,
) as dag:

    # Task 1: Bronze Layer - Extract and Load
    task_bronze = DockerOperator(
        task_id='bronze_extract_and_load',
        command=[
            '--layer', 'bronze',
            '--load-date', '{{ ds }}',  # Airflow execution date (YYYY-MM-DD)
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    # Task 2: Silver Layer - Transform and Clean
    task_silver = DockerOperator(
        task_id='silver_transform_and_clean',
        command=[
            '--layer', 'silver',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    # Task 3: Gold Layer - Build Dimensions
    task_gold_dimensions = DockerOperator(
        task_id='gold_build_dimensions',
        command=[
            '--layer', 'gold',
            '--step', 'dimensions',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    # Task 4: Gold Layer - Build Fact Table
    task_gold_fact = DockerOperator(
        task_id='gold_build_fact_table',
        command=[
            '--layer', 'gold',
            '--step', 'fact',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    # Task 5: Gold Layer - Load Star Schema
    task_gold_load = DockerOperator(
        task_id='gold_load_star_schema',
        command=[
            '--layer', 'gold',
            '--step', 'load',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    # Define task dependencies
    task_bronze >> task_silver >> task_gold_dimensions >> task_gold_fact >> task_gold_load
