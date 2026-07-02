"""
LinkedIn Jobs ETL Pipeline - Docker Operator Version
Separates ETL execution from Airflow to avoid dependency conflicts
"""
import os
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
    'execution_timeout': timedelta(minutes=59),
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

        # AI Selector Configuration
        'GROQ_API_KEY': os.getenv('GROQ_API_KEY', ''),
        'GROQ_MODEL': os.getenv('GROQ_MODEL', 'qwen/qwen3-32b'),

        # Crawler Configuration
        'CRAWLER_REQUEST_DELAY_MIN_SECONDS': os.getenv('CRAWLER_REQUEST_DELAY_MIN_SECONDS', '2'),
        'CRAWLER_REQUEST_DELAY_MAX_SECONDS': os.getenv('CRAWLER_REQUEST_DELAY_MAX_SECONDS', '7'),
        'CRAWLER_PROXY_SWITCH_INTERVAL': os.getenv('CRAWLER_PROXY_SWITCH_INTERVAL', '100'),
        'CRAWLER_PROXIES': os.getenv('CRAWLER_PROXIES', ''),

        # Paths
        'BUCKET': 'airflow-bucket',
        'BRONZE_PATH': 's3://airflow-bucket/bronze/crawler_data/linkedin/jobs',
        'SILVER_PATH': 's3://airflow-bucket/silver',
        'GOLD_PATH': 's3://airflow-bucket/gold/analytics',
    },
}

# Define DAG
with DAG(
    'linkedin_jobs_pipeline_docker_v08',
    default_args=default_args,
    description='LinkedIn Jobs ETL Pipeline using Docker Operator',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['etl', 'linkedin', 'docker', 'medallion'],
    max_active_runs=1,
) as dag:

    task_bronze_process_to_staging = DockerOperator(
        task_id='bronze_process_to_staging',
        command=[
            '--layer', 'bronze',
            '--step', 'process_to_staging',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_bronze_promote = DockerOperator(
        task_id='bronze_promote_staging_to_bronze',
        command=[
            '--layer', 'bronze',
            '--step', 'promote',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_silver_process_to_staging = DockerOperator(
        task_id='silver_process_to_staging',
        command=[
            '--layer', 'silver',
            '--step', 'process_to_staging',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_silver_promote = DockerOperator(
        task_id='silver_promote_staging_to_silver',
        command=[
            '--layer', 'silver',
            '--step', 'promote',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_gold_dimensions_process_to_staging = DockerOperator(
        task_id='gold_dimensions_process_to_staging',
        command=[
            '--layer', 'gold',
            '--step', 'dimensions_to_staging',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_gold_dimensions_promote = DockerOperator(
        task_id='gold_dimensions_promote_to_gold',
        command=[
            '--layer', 'gold',
            '--step', 'promote_dimensions',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_gold_fact_process_to_staging = DockerOperator(
        task_id='gold_fact_process_to_staging',
        command=[
            '--layer', 'gold',
            '--step', 'fact_to_staging',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_gold_fact_promote = DockerOperator(
        task_id='gold_fact_promote_to_gold',
        command=[
            '--layer', 'gold',
            '--step', 'promote_fact',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

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

    task_clean_staging = DockerOperator(
        task_id='clean_staging',
        command=[
            '--layer', 'staging',
            '--step', 'clean',
            '--load-date', '{{ ds }}',
            '--log-level', 'INFO',
        ],
        **docker_config
    )

    task_bronze_process_to_staging >> task_bronze_promote >> task_silver_process_to_staging >> task_silver_promote >> task_gold_dimensions_process_to_staging >> task_gold_dimensions_promote >> task_gold_fact_process_to_staging >> task_gold_fact_promote >> task_gold_load >> task_clean_staging
