from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.jobs.ingestion.scrapper import run_ingestion_pipeline


def test_ingestion_logic():
    jobs = run_ingestion_pipeline()
    return {"total_jobs": len(jobs)}


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="test_ingestion_logic",
    default_args=default_args,
    description="Manual DAG to test LinkedIn job ingestion logic",
    schedule_interval=None,
    start_date=datetime(2026, 6, 10),
    catchup=False,
    tags=["test", "ingestion", "linkedin"],
) as dag:
    run_ingestion_test = PythonOperator(
        task_id="run_ingestion_test",
        python_callable=test_ingestion_logic,
    )
