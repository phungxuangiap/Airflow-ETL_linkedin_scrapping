"""
Airflow-specific configuration
"""
import os
from datetime import timedelta


class AirflowConfig:
    """Airflow DAG and task configuration"""

    # Default DAG arguments
    DEFAULT_ARGS = {
        'owner': os.getenv('AIRFLOW_OWNER', 'data-engineering'),
        'retries': int(os.getenv('AIRFLOW_RETRIES', '3')),
        'retry_delay': timedelta(minutes=int(os.getenv('AIRFLOW_RETRY_DELAY_MINUTES', '5'))),
        'email_on_failure': os.getenv('AIRFLOW_EMAIL_ON_FAILURE', 'false').lower() == 'true',
        'email_on_retry': os.getenv('AIRFLOW_EMAIL_ON_RETRY', 'false').lower() == 'true',
    }

    # DAG schedule intervals
    BRONZE_SCHEDULE = os.getenv('BRONZE_SCHEDULE', '@daily')
    SILVER_SCHEDULE = os.getenv('SILVER_SCHEDULE', '@daily')
    GOLD_SCHEDULE = os.getenv('GOLD_SCHEDULE', '@daily')

    # Task execution settings
    TASK_TIMEOUT_SECONDS = int(os.getenv('TASK_TIMEOUT_SECONDS', '3600'))  # 1 hour
    EXECUTION_TIMEOUT = timedelta(seconds=TASK_TIMEOUT_SECONDS)

    # Catchup settings
    CATCHUP = os.getenv('AIRFLOW_CATCHUP', 'false').lower() == 'true'

    # Tags
    BRONZE_TAGS = ['linkedin', 'bronze', 'ingestion']
    SILVER_TAGS = ['linkedin', 'silver', 'transformation']
    GOLD_TAGS = ['linkedin', 'gold', 'analytics']


airflow_config = AirflowConfig()
