from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


import sys
sys.path.insert(0, '/opt/airflow')
sys.path.append('/mnt/Document/project/linkedin_scrapping/airflow')

from scripts.extract.api_source.get_jobs_from_apis import fetch_jobs_from_api_and_store_in_local
from scripts.extract.api_source.validate_local_jobs import validate_local_jobs


GCS_BUCKET = 'linkedin_scrapping_gcp_bucket'
GCS_CONN_ID = 'gcp_storage'
API_SOURCE = 'linkedin'
LOCAL_TEMP_DIR = '/tmp/linkedin_jobs'


default_args = {
    'owner': 'alexgi',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='collects_data_and_load_to_datalake',
    default_args=default_args,
    start_date=datetime(2026, 1, 15),
    schedule='@daily',
    catchup=False,
) as dag:
    fetch_jobs_from_api_and_store_in_local = PythonOperator(
        task_id='fetch_jobs_from_api_and_store_in_local',
        python_callable=fetch_jobs_from_api_and_store_in_local,
        provide_context=True
    )
    validate_local_jobs = PythonOperator(
        task_id='validate_local_jobs',
        python_callable=validate_local_jobs,  # Placeholder for the actual validation and loading function
        provide_context=True
    )
    upload_api_source_csv = LocalFilesystemToGCSOperator(
        task_id='upload_to_landing_zone_datalake',
        src="{{ task_instance.xcom_pull(task_ids='validate_local_jobs')['filepath'] }}",
        dst=f'landing/api_source/api_source_jobs_{datetime.now().strftime("%Y%m%d")}.csv',
        bucket='linkedin_scrapping_gcp_bucket',
        gcp_conn_id='gcp_storage'
    )
    fetch_jobs_from_api_and_store_in_local >> validate_local_jobs >> upload_api_source_csv