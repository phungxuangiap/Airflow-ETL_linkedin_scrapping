"""
MinIO/S3 configuration for object storage
"""
import os


class MinIOConfig:
    """MinIO/S3 configuration settings"""

    # Connection settings
    ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
    ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    REGION = os.getenv("S3_REGION", "us-east-1")
    USE_SSL = os.getenv("S3_USE_SSL", "false").lower() == "true"
    PATH_STYLE = os.getenv("S3_PATH_STYLE", "true").lower() == "true"

    # Bucket configuration
    BUCKET_NAME = os.getenv("BUCKET", "airflow-bucket")

    # Layer paths
    BRONZE_PATH = f"bronze/"
    SILVER_PATH = f"silver/"
    GOLD_PATH = f"gold/"

    # Specific data paths
    API_SOURCE_PATH = f"{BRONZE_PATH}api_data/"
    SCRAPPED_DATA_PATH = f"{BRONZE_PATH}crawler_data/"
    PROCESSED_DATA_PATH = f"{SILVER_PATH}processed/"

    @classmethod
    def get_endpoint_host(cls):
        """Get endpoint without protocol"""
        return cls.ENDPOINT.replace('http://', '').replace('https://', '')

    @classmethod
    def get_connection_config(cls):
        """Get MinIO connection configuration"""
        return {
            "endpoint": cls.ENDPOINT,
            "access_key": cls.ACCESS_KEY,
            "secret_key": cls.SECRET_KEY,
            "region": cls.REGION,
            "secure": cls.USE_SSL,
        }

    @classmethod
    def get_s3_uri(cls, path: str = ""):
        """Get full S3 URI for a given path"""
        return f"s3://{cls.BUCKET_NAME}/{path}"


minio_config = MinIOConfig()
