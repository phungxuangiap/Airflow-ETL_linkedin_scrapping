"""
DuckDB configuration for ETL operations
"""
import os


class DuckDBConfig:
    """DuckDB configuration settings"""

    # Memory and performance settings
    MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", "8GB")
    THREADS = int(os.getenv("DUCKDB_THREADS", "6"))
    MAX_MEMORY = os.getenv("DUCKDB_MAX_MEMORY", "4GB")
    TEMP_DIR = os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb")

    # Logging and profiling
    ENABLE_LOGGING = os.getenv("DUCKDB_ENABLE_LOGGING", "false").lower() == "true"
    ENABLE_PROFILING = os.getenv("DUCKDB_ENABLE_PROFILING", "false").lower() == "true"

    # Database file (use in-memory for most operations)
    DATABASE_FILE = os.getenv("DUCKDB_DATABASE_FILE", ":memory:")

    @classmethod
    def get_connection_config(cls):
        """Get DuckDB connection configuration"""
        return {
            "database": cls.DATABASE_FILE,
            "config": {
                "memory_limit": cls.MEMORY_LIMIT,
                "threads": cls.THREADS,
                "max_memory": cls.MAX_MEMORY,
                "temp_directory": cls.TEMP_DIR,
            }
        }

    @classmethod
    def get_s3_config(cls):
        """Get S3 configuration for DuckDB"""
        endpoint_host = os.getenv("S3_ENDPOINT", "http://minio:9000").replace('http://', '').replace('https://', '')

        return {
            "s3_endpoint": endpoint_host,
            "s3_access_key_id": os.getenv("S3_ACCESS_KEY", "minioadmin"),
            "s3_secret_access_key": os.getenv("S3_SECRET_KEY", "minioadmin"),
            "s3_use_ssl": os.getenv("S3_USE_SSL", "false").lower() == "true",
            "s3_url_style": "path",
        }


duckdb_config = DuckDBConfig()
