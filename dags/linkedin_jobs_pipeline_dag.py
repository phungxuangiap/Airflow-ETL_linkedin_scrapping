from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.configs.airflow_config import airflow_config
from src.jobs.bronze.extract_and_load_bronze import run as extract_and_load_bronze
from src.jobs.silver.transform_and_load_silver import run as transform_and_load_silver
from src.jobs.gold.build_dimensions import run as build_dimensions
from src.jobs.gold.build_fact_table import run as build_fact_table
from src.jobs.gold.load_star_schema import run as load_star_schema


# DAG default arguments
default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='linkedin_jobs_pipeline',
    default_args=default_args,
    description='LinkedIn Jobs ETL Pipeline: Bronze -> Silver -> Gold',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['linkedin', 'etl', 'jobs', 'pipeline'],
) as dag:

    # Bronze Layer Task (Combined Extract & Load)
    extract_and_load_bronze_task = PythonOperator(
        task_id='extract_and_load_bronze',
        python_callable=extract_and_load_bronze,
        provide_context=True,
    )

    # Silver Layer Task (Combined Transform & Load)
    transform_and_load_silver_task = PythonOperator(
        task_id='transform_and_load_silver',
        python_callable=transform_and_load_silver,
        provide_context=True,
    )

    # Gold Layer Tasks - Star Schema
    build_dimensions_task = PythonOperator(
        task_id='build_dimensions',
        python_callable=build_dimensions,
        provide_context=True,
    )

    build_fact_table_task = PythonOperator(
        task_id='build_fact_table',
        python_callable=build_fact_table,
        provide_context=True,
    )

    load_star_schema_task = PythonOperator(
        task_id='load_star_schema',
        python_callable=load_star_schema,
        provide_context=True,
    )

    # Define task dependencies
    extract_and_load_bronze_task >> transform_and_load_silver_task >> [build_dimensions_task, build_fact_table_task] >> load_star_schema_task
