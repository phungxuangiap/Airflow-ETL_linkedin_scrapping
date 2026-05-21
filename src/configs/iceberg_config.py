"""
Apache Iceberg configuration for lakehouse operations
"""
import os


class IcebergConfig:
    """Iceberg catalog and table configuration"""

    # Iceberg REST catalog settings
    CATALOG_TYPE = "rest"
    CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
    CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "lakehouse")

    # S3/MinIO settings
    BUCKET = os.getenv("BUCKET", "airflow-bucket")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    S3_REGION = os.getenv("S3_REGION", "us-east-1")

    # Warehouse location
    WAREHOUSE = f"s3://{BUCKET}/"

    # Iceberg table format settings
    ICEBERG_ENABLED = os.getenv("DUCKDB_ICEBERG_ENABLED", "true").lower() == "true"
    FORMAT_VERSION = int(os.getenv("DUCKDB_ICEBERG_FORMAT_VERSION", "2"))
    COMPRESSION = os.getenv("DUCKDB_ICEBERG_COMPRESSION", "snappy")
    PARTITION_MODE = os.getenv("DUCKDB_ICEBERG_PARTITION_MODE", "hive")
    FILE_FORMAT = os.getenv("DUCKDB_ICEBERG_FILE_FORMAT", "parquet")
    TARGET_FILE_SIZE = int(os.getenv("DUCKDB_ICEBERG_TARGET_FILE_SIZE", "134217728"))  # 128MB
    METADATA_COMPRESSION = os.getenv("DUCKDB_ICEBERG_METADATA_COMPRESSION", "gzip")
    DELETE_MODE = os.getenv("DUCKDB_ICEBERG_DELETE_MODE", "copy-on-write")

    # Layer paths
    BRONZE_PATH = os.getenv("BRONZE_PATH", f"s3://{BUCKET}/BRONZE/crawler_data/linkedin/jobs")
    SILVER_PATH = os.getenv("SILVER_PATH", f"s3://{BUCKET}/silver")
    GOLD_PATH = os.getenv("GOLD_PATH", f"s3://{BUCKET}/gold/analytics")

    @classmethod
    def get_catalog_config(cls):
        """Get Iceberg catalog configuration for pyiceberg"""
        return {
            "type": cls.CATALOG_TYPE,
            "uri": cls.CATALOG_URI,
            "warehouse": cls.WAREHOUSE,
            "s3.endpoint": cls.S3_ENDPOINT,
            "s3.access-key-id": cls.S3_ACCESS_KEY,
            "s3.secret-access-key": cls.S3_SECRET_KEY,
            "s3.region": cls.S3_REGION,
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "header.X-Iceberg-Access-Key": cls.S3_ACCESS_KEY,
            "header.X-Iceberg-Secret-Key": cls.S3_SECRET_KEY,
        }

    @classmethod
    def get_table_properties(cls):
        """Get default table properties for Iceberg tables"""
        return {
            "format-version": str(cls.FORMAT_VERSION),
            "write.parquet.compression-codec": cls.COMPRESSION,
            "write.metadata.compression-codec": cls.METADATA_COMPRESSION,
            "write.delete.mode": cls.DELETE_MODE,
            "write.target-file-size-bytes": str(cls.TARGET_FILE_SIZE),
        }


iceberg_config = IcebergConfig()
