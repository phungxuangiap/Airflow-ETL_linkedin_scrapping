from dataclasses import dataclass
import os
from typing import Any, Dict, Optional

@dataclass
class DuckDBConfig:
    memory_limit: str = '4GB'
    threads: int = 4
    max_memory: str = '2GB'
    threads_per_query: Optional[int] = None
    enable_profiling: bool = False
    
    # ========== Query Optimization ==========
    default_order: str = 'ASC'
    default_null_order: str = 'NULLS LAST'
    
    # ========== File System ==========
    temp_dir: str = '/tmp/duckdb'
    access_mode: str = 'automatic'
    
    # ========== Iceberg Settings (CRITICAL) ==========
    iceberg_enabled: bool = True
    iceberg_format_version: int = 2  # Version 1 or 2
    iceberg_compression: str = 'snappy'  # 'snappy', 'gzip', 'zstd'
    iceberg_partition_mode: str = 'hive'  # 'hive' or 'identity'
    iceberg_default_file_format: str = 'parquet'  # Iceberg data format
    iceberg_write_target_file_size: str = '134217728'  # 128MB (bytes)
    iceberg_metadata_compression: str = 'gzip'  # metadata compression
    iceberg_delete_mode: str = 'copy-on-write'  # 'copy-on-write' or 'merge-on-read'
    iceberg_partition_columns: Optional[str] = None  # e.g., "load_date"
    
    # ========== S3/MinIO Settings ==========
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_use_ssl: bool = False
    s3_path_style: bool = True  # MinIO requires true
    s3_region: str = 'us-east-1'
    
    # ========== Extension Settings ==========
    httpfs_enabled: bool = True
    json_enabled: bool = True
    parquet_enabled: bool = True
    csv_enabled: bool = True
    
    # ========== Logging & Debug ==========
    enable_query_logging: bool = False
    log_directory: str = './logs/duckdb'

    @classmethod
    def from_env(cls) -> 'DuckDBConfig':
        return cls(
            # Memory & Performance
            memory_limit=os.getenv('DUCKDB_MEMORY_LIMIT', '4GB'),
            threads=int(os.getenv('DUCKDB_THREADS', '4')),
            max_memory=os.getenv('DUCKDB_MAX_MEMORY', '2GB'),
            enable_profiling=os.getenv('DUCKDB_ENABLE_PROFILING', 'false').lower() == 'true',
            
            # File System
            temp_dir=os.getenv('DUCKDB_TEMP_DIR', '/tmp/duckdb'),
            
            # Iceberg Configuration
            iceberg_enabled=os.getenv('DUCKDB_ICEBERG_ENABLED', 'true').lower() == 'true',
            iceberg_format_version=int(os.getenv('DUCKDB_ICEBERG_FORMAT_VERSION', '2')),
            iceberg_compression=os.getenv('DUCKDB_ICEBERG_COMPRESSION', 'snappy'),
            iceberg_partition_mode=os.getenv('DUCKDB_ICEBERG_PARTITION_MODE', 'hive'),
            iceberg_default_file_format=os.getenv('DUCKDB_ICEBERG_FILE_FORMAT', 'parquet'),
            iceberg_write_target_file_size=os.getenv('DUCKDB_ICEBERG_TARGET_FILE_SIZE', '134217728'),
            iceberg_metadata_compression=os.getenv('DUCKDB_ICEBERG_METADATA_COMPRESSION', 'gzip'),
            iceberg_delete_mode=os.getenv('DUCKDB_ICEBERG_DELETE_MODE', 'copy-on-write'),
            iceberg_partition_columns=os.getenv('DUCKDB_ICEBERG_PARTITION_COLUMNS'),
            
            # S3/MinIO
            s3_endpoint=os.getenv('S3_ENDPOINT'),
            s3_access_key=os.getenv('S3_ACCESS_KEY'),
            s3_secret_key=os.getenv('S3_SECRET_KEY'),
            s3_region=os.getenv('S3_REGION', 'us-east-1'),
            s3_use_ssl=os.getenv('S3_USE_SSL', 'false').lower() == 'true',
            s3_path_style=os.getenv('S3_PATH_STYLE', 'true').lower() == 'true',
            
            # Logging
            enable_query_logging=os.getenv('DUCKDB_ENABLE_LOGGING', 'false').lower() == 'true'
        )
