"""
MinIO client for object storage operations
"""
from typing import List, Optional
from pathlib import Path
from minio import Minio
from minio.error import S3Error

from src.configs.minio_config import minio_config
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)


class MinIOClient:
    """Client for MinIO/S3 operations"""

    def __init__(self):
        """Initialize MinIO client"""
        self.client = self._create_client()
        self.bucket_name = minio_config.BUCKET_NAME
        self._ensure_bucket_exists()

    def _create_client(self) -> Minio:
        """Create MinIO client instance"""
        endpoint = minio_config.get_endpoint_host()
        logger.info(f"Connecting to MinIO at {endpoint}")

        return Minio(
            endpoint,
            access_key=minio_config.ACCESS_KEY,
            secret_key=minio_config.SECRET_KEY,
            secure=minio_config.USE_SSL
        )

    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if not"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    @retry(max_attempts=3, delay=2, exceptions=(S3Error,))
    def upload_file(self, local_path: str, object_name: str) -> bool:
        """
        Upload a file to MinIO

        Args:
            local_path: Local file path
            object_name: Object name in MinIO (key)

        Returns:
            True if successful
        """
        try:
            self.client.fput_object(
                self.bucket_name,
                object_name,
                local_path
            )
            logger.info(f"Uploaded {local_path} to s3://{self.bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    @retry(max_attempts=3, delay=2, exceptions=(S3Error,))
    def download_file(self, object_name: str, local_path: str) -> bool:
        """
        Download a file from MinIO

        Args:
            object_name: Object name in MinIO
            local_path: Local file path to save

        Returns:
            True if successful
        """
        try:
            # Ensure parent directory exists
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)

            self.client.fget_object(
                self.bucket_name,
                object_name,
                local_path
            )
            logger.info(f"Downloaded s3://{self.bucket_name}/{object_name} to {local_path}")
            return True
        except S3Error as e:
            logger.error(f"Failed to download {object_name}: {e}")
            raise

    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[str]:
        """
        List objects in MinIO bucket

        Args:
            prefix: Filter by prefix
            recursive: List recursively

        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            object_names = [obj.object_name for obj in objects]
            logger.info(f"Found {len(object_names)} objects with prefix '{prefix}'")
            return object_names
        except S3Error as e:
            logger.error(f"Failed to list objects: {e}")
            return []

    def delete_object(self, object_name: str) -> bool:
        """
        Delete an object from MinIO

        Args:
            object_name: Object name to delete

        Returns:
            True if successful
        """
        try:
            self.client.remove_object(self.bucket_name, object_name)
            logger.info(f"Deleted s3://{self.bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Failed to delete {object_name}: {e}")
            return False

    def object_exists(self, object_name: str) -> bool:
        """
        Check if an object exists

        Args:
            object_name: Object name to check

        Returns:
            True if exists
        """
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error:
            return False


def get_minio_client() -> MinIOClient:
    """
    Get MinIO client instance

    Returns:
        MinIOClient instance
    """
    return MinIOClient()
