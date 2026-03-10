from datetime import datetime, timedelta
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add paths for imports
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/project_root')

from src.jobs.silver.use_duckdb_transfer_load_file_to_iceberg import load_bronze_to_silver_iceberg
default_args = {
    'owner': 'alexgi',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='transfer_data_to_silver_v33',
    default_args=default_args,
    start_date=datetime(2026, 3, 3),
    schedule='@daily',
    catchup=False,
) as dag:
    transfer_data_to_silver = PythonOperator(
        task_id='transfer_data_to_silver_v01',
        python_callable=load_bronze_to_silver_iceberg,
        op_kwargs={
            'load_date': datetime.now().strftime("%Y-%m-%d"),
            'bucket': 'airflow-bucket',
            's3_endpoint': 'http://minio:9000',
            's3_access_key': 'minioadmin',
            's3_secret_key': 'minioadmin'
        },
        provide_context=True
    )
    transfer_data_to_silver