from datetime import datetime, date, timedelta
import sys
sys.path.insert(0, '/opt/airflow')
sys.path.append('/mnt/Document/project/linkedin_scrapping/airflow')
from airflow import DAG
from pyspark.sql import SparkSession, Row
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from spark_jobs.transform_linkedin_data import add_timestamp_column
CLUSTER_NAME = 'linkedin-scrapping-cluster-{{ ds_nodash }}'  # Unique name with date
PROJECT_ID = 'linkedin-scrapping-etl-482610'
REGION = 'us-central1'

default_args = {
    'owner': 'alexgi',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}
    
with DAG(
    dag_id='linkedin_scrapping_v31',
    default_args=default_args,
    start_date=datetime(2026, 1, 3),
    schedule='@daily'
) as dag:
    
    upload_csv = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/opt/airflow/data/linkedin_jobs.csv',
        dst='data/raw/linkedin_jobs.csv',
        bucket='linkedin_scrapping_gcp_bucket',
        gcp_conn_id='gcp_storage'
    )
    upload_spark_script = LocalFilesystemToGCSOperator(
        task_id='upload_spark_script_to_gcs',
        src='/opt/airflow/spark_jobs/transform_linkedin_data.py',
        dst='spark_jobs/transform_linkedin_data.py',
        bucket='linkedin_scrapping_gcp_bucket',
        gcp_conn_id='gcp_storage'
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_config={
            'gce_cluster_config': {
                'service_account': 'linkedin-scrapping-etl@linkedin-scrapping-etl-482610.iam.gserviceaccount.com',  # ← Service account của bạn
                'service_account_scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            },
            'master_config': {
                'machine_type_uri': 'n1-standard-2',
                'disk_config': {'boot_disk_size_gb': 100}
            },
            'worker_config': {
                'machine_type_uri': 'n1-standard-2',
                'disk_config': {'boot_disk_size_gb': 100},
                'num_instances': 2
            }
        },
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id='gcp_storage'
    )
    migrate_csv_schema_task = DataprocSubmitJobOperator(
        task_id='migrate_csv_schema_with_spark',
        job={
            'reference': {'project_id': 'linkedin-scrapping-etl-482610'},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': 'gs://linkedin_scrapping_gcp_bucket/spark_jobs/transform_linkedin_data.py',
                'args': [
                    'gs://linkedin_scrapping_gcp_bucket/data/raw/linkedin_jobs.csv',
                    'gs://linkedin_scrapping_gcp_bucket/data/processed/linkedin_jobs_transformed'
                ]
            }
        },
        gcp_conn_id='gcp_storage',
        region=REGION
    )
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id='linkedin_scrapping_dataset',
        gcp_conn_id='gcp_storage'
    )
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        dataset_id='linkedin_scrapping_dataset',
        table_id='linkedin_jobs_table',
        schema_fields=[
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company_avatar_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company_location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location_working_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'working_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date_posted', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'number_applicants', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'job_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'scrapped_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ],
        gcp_conn_id='gcp_storage'
    )
    gcs_to_staging = GCSToBigQueryOperator(
        task_id='gcs_to_bq_staging',
        bucket='linkedin_scrapping_gcp_bucket',
        source_objects=['data/processed/linkedin_jobs_transformed/part-*'],
        destination_project_dataset_table='linkedin-scrapping-etl-482610.linkedin_scrapping_dataset.linkedin_jobs_table_staging',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='gcp_storage',
        autodetect=True
    )
    upsert_to_prod = BigQueryInsertJobOperator(
        task_id='upsert_to_prod',
        configuration={
            "query": {
                "query": f"""
                    MERGE `linkedin-scrapping-etl-482610.linkedin_scrapping_dataset.linkedin_jobs_table` T
                    USING `linkedin-scrapping-etl-482610.linkedin_scrapping_dataset.linkedin_jobs_table_staging` S
                    ON T.job_url = S.job_url
                    WHEN MATCHED THEN
                      UPDATE SET 
                        T.title = S.title,
                        T.company = S.company,
                        T.company_avatar_url = S.company_avatar_url,
                        T.company_location = S.company_location,
                        T.location_working_type = S.location_working_type,
                        T.working_type = S.working_type,
                        T.date_posted = CAST(S.date_posted AS STRING),
                        T.number_applicants = S.number_applicants,
                        T.description = S.description,
                        T.scrapped_at = S.scrapped_at
                    WHEN NOT MATCHED THEN
                      INSERT (title, company, company_avatar_url, company_location, location_working_type, working_type, date_posted, number_applicants, job_url, description, scrapped_at)
                      VALUES (S.title, S.company, S.company_avatar_url, S.company_location, S.location_working_type, S.working_type, CAST(S.date_posted AS STRING), S.number_applicants, S.job_url, S.description, S.scrapped_at)
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='gcp_storage'
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id='gcp_storage',
        trigger_rule='all_done'  
    )
    upload_csv >> upload_spark_script >> create_cluster >> migrate_csv_schema_task >>  create_bq_dataset >> create_bq_table >> gcs_to_staging >> upsert_to_prod >> delete_cluster