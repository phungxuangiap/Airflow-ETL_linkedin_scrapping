"""
Iceberg client for lakehouse operations
"""
from typing import Optional, Dict, Any, List
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
import pyarrow as pa

from src.configs.iceberg_config import iceberg_config
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)


class IcebergClient:
    """Client for Apache Iceberg operations"""

    def __init__(self):
        """Initialize Iceberg client"""
        self.catalog = None
        self._connect()

    @retry(max_attempts=3, delay=2, exceptions=(Exception,))
    def _connect(self):
        """Connect to Iceberg catalog"""
        try:
            catalog_config_dict = iceberg_config.get_catalog_config()
            self.catalog = load_catalog(
                iceberg_config.CATALOG_NAME,
                **catalog_config_dict
            )
            logger.info(f"Connected to Iceberg catalog: {iceberg_config.CATALOG_NAME}")
        except Exception as e:
            logger.error(f"Failed to connect to Iceberg catalog: {e}")
            raise

    def create_namespace(self, namespace: str) -> bool:
        """
        Create a namespace (database) if it doesn't exist

        Args:
            namespace: Namespace name (e.g., 'BRONZE', 'SILVER', 'GOLD')

        Returns:
            True if created or already exists
        """
        try:
            self.catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
            return True
        except Exception as e:
            logger.warning(f"Namespace {namespace} might already exist: {e}")
            return True

    def table_exists(self, identifier: str) -> bool:
        """
        Check if a table exists

        Args:
            identifier: Table identifier (e.g., 'SILVER.jobs')

        Returns:
            True if table exists
        """
        try:
            self.catalog.load_table(identifier)
            return True
        except Exception:
            return False

    def load_table(self, identifier: str) -> Optional[Table]:
        """
        Load an existing Iceberg table

        Args:
            identifier: Table identifier (e.g., 'SILVER.jobs')

        Returns:
            Iceberg Table object or None if not found
        """
        try:
            table = self.catalog.load_table(identifier)
            logger.info(f"Loaded table: {identifier}")
            return table
        except Exception as e:
            logger.warning(f"Table {identifier} not found: {e}")
            return None

    def create_table(
        self,
        identifier: str,
        schema: pa.Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> Table:
        """
        Create a new Iceberg table

        Args:
            identifier: Table identifier (e.g., 'SILVER.jobs')
            schema: PyArrow schema for the table
            partition_spec: Partition specification
            properties: Table properties

        Returns:
            Created Iceberg Table object
        """
        # Ensure namespace exists
        namespace = identifier.split('.')[0]
        self.create_namespace(namespace)

        # Use default properties if not provided
        if properties is None:
            properties = iceberg_config.get_table_properties()

        try:
            if partition_spec is None:
                partition_spec = PartitionSpec()

            table = self.catalog.create_table(
                identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            logger.info(f"Created table: {identifier}")
            return table
        except Exception as e:
            logger.error(f"Failed to create table {identifier}: {e}")
            raise

    def append_data(self, identifier: str, data: pa.Table) -> int:
        """
        Append data to an Iceberg table

        Args:
            identifier: Table identifier
            data: PyArrow table with data to append

        Returns:
            Number of rows appended
        """
        table = self.load_table(identifier)
        if table is None:
            raise ValueError(f"Table {identifier} does not exist")

        try:
            table.append(data)
            row_count = data.num_rows
            logger.info(f"Appended {row_count} rows to {identifier}")
            return row_count
        except Exception as e:
            logger.error(f"Failed to append data to {identifier}: {e}")
            raise

    def overwrite_data(self, identifier: str, data: pa.Table) -> int:
        """
        Overwrite data in an Iceberg table

        Args:
            identifier: Table identifier
            data: PyArrow table with data to write

        Returns:
            Number of rows written
        """
        table = self.load_table(identifier)
        if table is None:
            raise ValueError(f"Table {identifier} does not exist")

        try:
            table.overwrite(data)
            row_count = data.num_rows
            logger.info(f"Overwrote {identifier} with {row_count} rows")
            return row_count
        except Exception as e:
            logger.error(f"Failed to overwrite data in {identifier}: {e}")
            raise

    def get_or_create_table(
        self,
        identifier: str,
        schema: pa.Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> Table:
        """
        Get existing table or create if it doesn't exist

        Args:
            identifier: Table identifier
            schema: PyArrow schema
            partition_spec: Partition specification
            properties: Table properties

        Returns:
            Iceberg Table object
        """
        table = self.load_table(identifier)
        if table is None:
            table = self.create_table(identifier, schema, partition_spec, properties)
        return table

    def upsert_data(self, identifier: str, data: pa.Table, unique_key: str) -> int:
        """
        Upsert data to an Iceberg table (update if exists, insert if new)

        Args:
            identifier: Table identifier
            data: PyArrow table with data to upsert
            unique_key: Column name to use for deduplication (e.g., 'job_url', 'id')

        Returns:
            Number of rows upserted
        """
        table = self.load_table(identifier)
        if table is None:
            raise ValueError(f"Table {identifier} does not exist")

        try:
            # Get unique values from new data
            unique_values = data.column(unique_key).to_pylist()
            unique_values_str = ", ".join([f"'{v}'" for v in unique_values])

            # Delete existing records with matching unique keys
            delete_filter = f"{unique_key} IN ({unique_values_str})"
            table.delete(delete_filter)
            logger.info(f"Deleted existing records matching {unique_key} from {identifier}")

            # Append new data
            table.append(data)
            row_count = data.num_rows
            logger.info(f"Upserted {row_count} rows to {identifier}")
            return row_count
        except Exception as e:
            logger.error(f"Failed to upsert data to {identifier}: {e}")
            raise

    def list_tables(self, namespace: str) -> List[str]:
        """
        List all tables in a namespace

        Args:
            namespace: Namespace name

        Returns:
            List of table names
        """
        try:
            tables = self.catalog.list_tables(namespace)
            logger.info(f"Found {len(tables)} tables in namespace {namespace}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables in {namespace}: {e}")
            return []


def get_iceberg_client() -> IcebergClient:
    """
    Get Iceberg client instance

    Returns:
        IcebergClient instance
    """
    return IcebergClient()
