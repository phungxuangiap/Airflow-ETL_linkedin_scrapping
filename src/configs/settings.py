"""
Central configuration settings for the LinkedIn Jobs ETL pipeline
"""
import os
from pathlib import Path


class Settings:
    """Main settings class for the ETL pipeline"""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    LOGS_DIR = PROJECT_ROOT / "logs"
    TMP_DIR = PROJECT_ROOT / "tmp"

    # Environment
    ENV = os.getenv("ENV", "local")  # local, staging, production

    # Airflow settings
    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    AIRFLOW_DAGS_FOLDER = os.getenv("AIRFLOW_DAGS_FOLDER", f"{AIRFLOW_HOME}/dags")

    # Data generation paths (for local development)
    API_SOURCE_DIR = os.getenv("API_SOURCE_DIR", str(TMP_DIR / "api_sources"))
    SCRAPPED_SOURCE_DIR = os.getenv("SCRAPPED_SOURCE_DIR", str(TMP_DIR / "scrapping_script"))

    # Source configuration
    SOURCE_WEB_NAME = os.getenv("SOURCE_WEB_NAME", "linkedin")
    ENTITY_TYPE = os.getenv("ENTITY_TYPE", "jobs")

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_env_specific_config(cls):
        """Get environment-specific configuration"""
        return {
            "env": cls.ENV,
            "project_root": str(cls.PROJECT_ROOT),
            "data_dir": str(cls.DATA_DIR),
            "logs_dir": str(cls.LOGS_DIR),
        }


settings = Settings()
