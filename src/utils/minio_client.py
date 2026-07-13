"""
MinIO client for object storage operations
"""
from io import BytesIO
from typing import Any, List, Optional
from pathlib import Path

from src.configs.minio_config import minio_config
from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)


def _get_minio_classes():
    try:
        from minio import Minio
        from minio.error import S3Error
    except ModuleNotFoundError as exc:
        raise RuntimeError("The 'minio' package is required for MinIO operations. Install project dependencies.") from exc
    return Minio, S3Error


class MinIOClient:
    """Client for MinIO/S3 operations"""

    def __init__(self):
        """Initialize MinIO client"""
        self.client = self._create_client()
        self.bucket_name = minio_config.BUCKET_NAME
        self._ensure_bucket_exists()

    def _create_client(self) -> Any:
        """Create MinIO client instance"""
        Minio, _ = _get_minio_classes()
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
        _, S3Error = _get_minio_classes()
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    @retry(max_attempts=3, delay=2, exceptions=(Exception,))
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
        except _get_minio_classes()[1] as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            raise

    @retry(max_attempts=3, delay=2, exceptions=(Exception,))
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
        except _get_minio_classes()[1] as e:
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
        except _get_minio_classes()[1] as e:
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
        except _get_minio_classes()[1] as e:
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
        except _get_minio_classes()[1]:
            return False

    @retry(max_attempts=3, delay=2, exceptions=(Exception,))
    def read_text(self, object_name: str, encoding: str = "utf-8") -> Optional[str]:
        """
        Read a text object from MinIO.

        Args:
            object_name: Object name in MinIO
            encoding: Text encoding

        Returns:
            Object contents, or None when the object does not exist
        """
        response = None
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            content = response.read().decode(encoding)
            logger.info(f"Read s3://{self.bucket_name}/{object_name}")
            return content
        except _get_minio_classes()[1] as e:
            if e.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                logger.warning(f"Object not found: s3://{self.bucket_name}/{object_name}")
                return None
            logger.error(f"Failed to read {object_name}: {e}")
            raise
        finally:
            if response:
                response.close()
                response.release_conn()

    @retry(max_attempts=3, delay=2, exceptions=(Exception,))
    def write_text(self, object_name: str, content: str, encoding: str = "utf-8") -> bool:
        """
        Write a text object to MinIO.

        Args:
            object_name: Object name in MinIO
            content: Text content
            encoding: Text encoding

        Returns:
            True if successful
        """
        data = content.encode(encoding)
        try:
            self.client.put_object(
                self.bucket_name,
                object_name,
                BytesIO(data),
                length=len(data),
                content_type="application/json",
            )
            logger.info(f"Wrote s3://{self.bucket_name}/{object_name}")
            return True
        except _get_minio_classes()[1] as e:
            logger.error(f"Failed to write {object_name}: {e}")
            raise


def get_minio_client() -> MinIOClient:
    """
    Get MinIO client instance

    Returns:
        MinIOClient instance
    """
    return MinIOClient()
