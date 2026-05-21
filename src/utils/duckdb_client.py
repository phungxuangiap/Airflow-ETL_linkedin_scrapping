"""
DuckDB client for ETL operations
"""
import duckdb
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

from src.configs.duckdb_config import duckdb_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DuckDBClient:
    """DuckDB client for data processing operations"""

    def __init__(self, database: str = ":memory:"):
        """
        Initialize DuckDB client

        Args:
            database: Database file path or ":memory:" for in-memory database
        """
        self.database = database
        self.connection: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Create and configure DuckDB connection

        Returns:
            DuckDB connection object
        """
        if self.connection is None:
            logger.info(f"Connecting to DuckDB: {self.database}")
            self.connection = duckdb.connect(database=self.database)

            # Configure DuckDB settings
            self.connection.execute(f"SET memory_limit = '{duckdb_config.MEMORY_LIMIT}'")
            self.connection.execute(f"SET threads = {duckdb_config.THREADS}")
            self.connection.execute(f"SET temp_directory = '{duckdb_config.TEMP_DIR}'")

            # Install and load extensions
            self.connection.execute("INSTALL httpfs; LOAD httpfs;")
            self.connection.execute("INSTALL json; LOAD json;")
            self.connection.execute("INSTALL iceberg; LOAD iceberg;")

            # Configure S3 settings
            s3_config = duckdb_config.get_s3_config()
            self.connection.execute(f"SET s3_endpoint = '{s3_config['s3_endpoint']}'")
            self.connection.execute(f"SET s3_access_key_id = '{s3_config['s3_access_key_id']}'")
            self.connection.execute(f"SET s3_secret_access_key = '{s3_config['s3_secret_access_key']}'")
            self.connection.execute(f"SET s3_use_ssl = {str(s3_config['s3_use_ssl']).lower()}")
            self.connection.execute(f"SET s3_url_style = '{s3_config['s3_url_style']}'")

            logger.info("DuckDB connection configured successfully")

        return self.connection

    def close(self):
        """Close DuckDB connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("DuckDB connection closed")

    def execute_query(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query

        Args:
            query: SQL query string

        Returns:
            Query result as DuckDB relation
        """
        conn = self.connect()
        logger.debug(f"Executing query: {query[:100]}...")
        return conn.execute(query)

    def fetch_arrow_table(self, query: str):
        """
        Execute query and return results as PyArrow table

        Args:
            query: SQL query string

        Returns:
            PyArrow table with query results
        """
        result = self.execute_query(query)
        return result.fetch_arrow_table()

    def fetch_df(self, query: str):
        """
        Execute query and return results as pandas DataFrame

        Args:
            query: SQL query string

        Returns:
            Pandas DataFrame with query results
        """
        result = self.execute_query(query)
        return result.df()

    def read_json_from_s3(
        self,
        s3_path: str,
        format: str = "newline_delimited",
        ignore_errors: bool = True
    ):
        """
        Read JSON/JSONL files from S3

        Args:
            s3_path: S3 path pattern (e.g., 's3://bucket/path/*.jsonl')
            format: JSON format ('newline_delimited' or 'array')
            ignore_errors: Whether to ignore parsing errors

        Returns:
            DuckDB relation with loaded data
        """
        query = f"""
            SELECT * FROM read_json_auto(
                '{s3_path}',
                format='{format}',
                ignore_errors={str(ignore_errors).lower()}
            )
        """
        return self.execute_query(query)

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


@contextmanager
def get_duckdb_connection(database: str = ":memory:"):
    """
    Context manager for DuckDB connections

    Args:
        database: Database file path

    Yields:
        DuckDB connection

    Example:
        with get_duckdb_connection() as conn:
            result = conn.execute("SELECT * FROM table").fetchall()
    """
    client = DuckDBClient(database)
    try:
        yield client.connect()
    finally:
        client.close()
