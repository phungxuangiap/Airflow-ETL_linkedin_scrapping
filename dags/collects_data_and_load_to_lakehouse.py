from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add paths for imports
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/project_root')

try:
    from src.scripts.generate_api_data import generate_api_files
    from src.scripts.generate_scrapped_data import generate_scrapped_files
    # from src.scripts.extract.api_source.get_jobs_from_apis import fetch_jobs_from_api_and_store_in_local
    from src.jobs.bronze.validate_local_jobs import validate_local_jobs
    from src.configs.minio.minio_utils import upload_to_bronze_layer
except ImportError as e:
    import logging
    logging.error(f"Import error in DAG: {e}")
    raise

default_args = {
    'owner': 'alexgi',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='collects_data_and_load_to_lakehous_02',
    default_args=default_args,
    start_date=datetime(2026, 1, 15),
    schedule='@daily',
    catchup=False,
) as dag:
    # fetch_jobs_from_api_and_store_in_local = PythonOperator(
    #     task_id='fetch_jobs_from_api_and_store_in_local',
    #     python_callable=fetch_jobs_from_api_and_store_in_local,
    #     provide_context=True
    # )
    fetch_jobs_from_api_and_store_in_local = PythonOperator(
        task_id='fetch_jobs_from_api_and_store_in_local',
        python_callable=generate_api_files,
        provide_context=True
    )
    
    fetch_scrapped_jobs_and_store_in_local = PythonOperator(
        task_id='fetch_scrapped_jobs_and_store_in_local',
        python_callable=generate_scrapped_files,
        provide_context=True
    )
    
    validate_local_jobs = PythonOperator(
        task_id='validate_local_jobs',
        python_callable=validate_local_jobs,
        provide_context=True
    )
    
    upload_bronze_layer = PythonOperator(
        task_id='upload_to_bronze',
        python_callable=upload_to_bronze_layer,
        op_kwargs={
            'api_source_dir': '/project_root/tmp/api_sources',
            'scrapped_source_dir': '/project_root/tmp/scrapping_script',
            'source_web_name': 'linkedin',
            'entity': 'jobs'
        },
        provide_context=True
    )
    
    [fetch_jobs_from_api_and_store_in_local, fetch_scrapped_jobs_and_store_in_local] >> validate_local_jobs >> upload_bronze_layer