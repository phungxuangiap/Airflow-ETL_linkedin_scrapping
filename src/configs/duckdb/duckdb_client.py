import duckdb
from typing import Any, Dict, List, Optional
from src.configs.duckdb.config import DuckDBConfig

class DuckDBClient:
    """DuckDB Client - All-in-one client with all services"""
    
    def __init__(self, config: Optional[DuckDBConfig] = None):
        self.config = config or DuckDBConfig.from_env()
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._initialize()
    
    def _initialize(self) -> None:
        """Initialize DuckDB connection with all settings"""
        self.conn = duckdb.connect(':memory:')
        self._apply_memory_settings()
        self._apply_query_settings()
        self._apply_filesystem_settings()
        self._load_extensions()
        
        if self.config.iceberg_enabled:
            self._setup_iceberg()
        
        if self.config.s3_endpoint:
            self._setup_s3()
    
    # ========== Initialization Methods ==========
    def _apply_memory_settings(self) -> None:
        """Apply memory and performance settings"""
        self.conn.execute(f"SET memory_limit = '{self.config.memory_limit}'")
        self.conn.execute(f"SET threads = {self.config.threads}")
        self.conn.execute(f"SET max_memory = '{self.config.max_memory}'")
        
        if self.config.threads_per_query:
            self.conn.execute(f"SET threads_per_query = {self.config.threads_per_query}")
        
        if self.config.enable_profiling:
            self.conn.execute("PRAGMA enable_profiling")
    
    def _apply_query_settings(self) -> None:
        """Apply query optimization settings"""
        self.conn.execute(f"SET default_order = '{self.config.default_order}'")
        self.conn.execute(f"SET default_null_order = '{self.config.default_null_order}'")
    
    def _apply_filesystem_settings(self) -> None:
        """Apply filesystem settings"""
        self.conn.execute(f"SET temp_directory = '{self.config.temp_dir}'")
        self.conn.execute(f"SET access_mode = '{self.config.access_mode}'")
    
    def _load_extensions(self) -> None:
        """Load required DuckDB extensions"""
        extensions = {
            'httpfs': self.config.httpfs_enabled,
            'json': self.config.json_enabled,
            'parquet': self.config.parquet_enabled,
            'csv': self.config.csv_enabled,
        }
        
        for ext_name, is_enabled in extensions.items():
            if is_enabled:
                try:
                    self.conn.execute(f"INSTALL {ext_name} IF NOT EXISTS")
                    self.conn.execute(f"LOAD {ext_name}")
                except Exception as e:
                    print(f"Warning: Failed to load {ext_name}: {e}")
    
    def _setup_iceberg(self) -> None:
        """Setup Iceberg extension"""
        try:
            self.conn.execute("INSTALL iceberg IF NOT EXISTS")
            self.conn.execute("LOAD iceberg")
            
            self.conn.execute(f"SET iceberg_format_version = {self.config.iceberg_format_version}")
            self.conn.execute(f"SET iceberg_compression = '{self.config.iceberg_compression}'")
            self.conn.execute(f"SET iceberg_metadata_compression = '{self.config.iceberg_metadata_compression}'")
            self.conn.execute(f"SET iceberg_default_file_format = '{self.config.iceberg_default_file_format}'")
        except Exception as e:
            print(f"Error setting up Iceberg: {e}")
    
    def _setup_s3(self) -> None:
        """Setup S3/MinIO configuration"""
        try:
            self.conn.execute("INSTALL httpfs IF NOT EXISTS")
            self.conn.execute("LOAD httpfs")
            
            self.conn.execute(f"SET s3_endpoint = '{self.config.s3_endpoint}'")
            self.conn.execute(f"SET s3_access_key_id = '{self.config.s3_access_key}'")
            self.conn.execute(f"SET s3_secret_access_key = '{self.config.s3_secret_key}'")
            self.conn.execute(f"SET s3_use_ssl = {str(self.config.s3_use_ssl).lower()}")
            self.conn.execute(f"SET s3_url_style = '{'path' if self.config.s3_path_style else 'vhost'}'")
            self.conn.execute(f"SET s3_region = '{self.config.s3_region}'")
        except Exception as e:
            print(f"Error setting up S3: {e}")
    
    # ========== Query Execution Services ==========
    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> duckdb.DuckDBPyRelation:
        """Execute SQL query"""
        try:
            if params:
                return self.conn.execute(sql, params)
            return self.conn.execute(sql)
        except Exception as e:
            print(f"Error executing SQL: {e}")
            raise
    
    def execute_sql_fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[tuple]:
        """Execute SQL and fetch all results"""
        result = self.execute_sql(sql, params)
        return result.fetchall()
    
    def execute_sql_fetch_df(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """Execute SQL and return pandas DataFrame"""
        result = self.execute_sql(sql, params)
        return result.df()
    
    def execute_sql_fetch_arrow(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """Execute SQL and return Arrow table"""
        result = self.execute_sql(sql, params)
        return result.arrow()
    
    # ========== Table Operations ==========
    def create_table_from_df(self, table_name: str, df) -> None:
        """Create table from pandas DataFrame"""
        self.conn.from_df(df).create_view(table_name)
    
    def create_table_from_parquet(self, table_name: str, file_path: str) -> None:
        """Create table from Parquet file"""
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
    
    def create_table_from_csv(self, table_name: str, file_path: str) -> None:
        """Create table from CSV file"""
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{file_path}')")
    
    def create_table_from_json(self, table_name: str, file_path: str) -> None:
        """Create table from JSON file"""
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json('{file_path}')")
    
    def drop_table(self, table_name: str) -> None:
        """Drop table"""
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        result = self.conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
        return [row[0] for row in result.fetchall()]
    
    # ========== Iceberg Services ==========
    def create_iceberg_table(self, table_name: str, sql: str, catalog: str = 'memory') -> None:
        """Create Iceberg table from SQL"""
        create_sql = f"CREATE TABLE iceberg_{catalog}.{table_name} AS {sql}"
        self.execute_sql(create_sql)
    
    def drop_iceberg_table(self, table_name: str, catalog: str = 'memory') -> None:
        """Drop Iceberg table"""
        drop_sql = f"DROP TABLE IF EXISTS iceberg_{catalog}.{table_name}"
        self.execute_sql(drop_sql)
    
    def list_iceberg_tables(self, catalog: str = 'memory') -> List[str]:
        """List all Iceberg tables"""
        try:
            sql = f"SELECT table_name FROM information_schema.tables WHERE table_catalog = 'iceberg_{catalog}'"
            result = self.execute_sql(sql)
            return [row[0] for row in result.fetchall()]
        except:
            return []
    
    def get_iceberg_table_metadata(self, table_name: str, catalog: str = 'memory') -> Dict[str, Any]:
        """Get Iceberg table metadata"""
        sql = f"SELECT * FROM iceberg_{catalog}.{table_name}$metadata"
        result = self.execute_sql(sql).df()
        return result.to_dict()
    
    def compact_iceberg_table(self, table_name: str, catalog: str = 'memory') -> None:
        """Compact Iceberg table"""
        sql = f"ALTER TABLE iceberg_{catalog}.{table_name} COMPACT"
        self.execute_sql(sql)
    
    # ========== S3/MinIO Services ==========
    def read_from_s3(self, s3_path: str, format: str = 'parquet'):
        """Read data from S3"""
        if format == 'parquet':
            sql = f"SELECT * FROM read_parquet('s3://{s3_path}')"
        elif format == 'csv':
            sql = f"SELECT * FROM read_csv('s3://{s3_path}')"
        elif format == 'json':
            sql = f"SELECT * FROM read_json('s3://{s3_path}')"
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return self.execute_sql(sql)
    
    def read_from_s3_to_df(self, s3_path: str, format: str = 'parquet'):
        """Read data from S3 and return as DataFrame"""
        result = self.read_from_s3(s3_path, format)
        return result.df()
    
    def write_to_s3(self, table_name: str, s3_path: str, format: str = 'parquet') -> None:
        """Write table to S3"""
        if format == 'parquet':
            sql = f"COPY {table_name} TO 's3://{s3_path}' (FORMAT PARQUET)"
        elif format == 'csv':
            sql = f"COPY {table_name} TO 's3://{s3_path}' (FORMAT CSV)"
        elif format == 'json':
            sql = f"COPY {table_name} TO 's3://{s3_path}' (FORMAT JSON)"
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        self.execute_sql(sql)
    
    def write_dataframe_to_s3(self, df, s3_path: str, format: str = 'parquet') -> None:
        """Write DataFrame directly to S3"""
        table_name = f"temp_table_{id(df)}"
        self.create_table_from_df(table_name, df)
        self.write_to_s3(table_name, s3_path, format)
        self.drop_table(table_name)
    
    def list_s3_files(self, s3_path: str) -> List[str]:
        """List files in S3 path"""
        try:
            sql = f"SELECT * FROM glob('s3://{s3_path}')"
            result = self.execute_sql(sql)
            return [row[0] for row in result.fetchall()]
        except:
            return []
    
    # ========== Validation & Utility ==========
    def validate_connection(self) -> bool:
        """Validate DuckDB connection is working"""
        try:
            self.execute_sql("SELECT 1")
            return True
        except Exception as e:
            print(f"Connection validation failed: {e}")
            return False
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get raw DuckDB connection"""
        return self.conn
    
    def close(self) -> None:
        """Close connection"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()