"""
MinIO/S3 configuration and utilities for Airflow
"""

import os
from typing import Optional, List
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


class MinIOConfig:
    """MinIO configuration settings"""
    
    ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000')
    ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'airflow-bucket')
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # Create output directories based on bucket
    API_SOURCE_PATH = 'api_source/'
    SCRAPPED_DATA_PATH = 'scrapped/'
    PROCESSED_DATA_PATH = 'processed/'
    

class MinIOClient:
    """MinIO S3-compatible client wrapper"""
    
    def __init__(self, config: MinIOConfig = None):
        """Initialize MinIO client"""
        self.config = config or MinIOConfig()
        
        # Create S3 client pointing to MinIO
        self.client = boto3.client(
            's3',
            endpoint_url=self.config.ENDPOINT_URL,
            aws_access_key_id=self.config.ACCESS_KEY,
            aws_secret_access_key=self.config.SECRET_KEY,
            region_name=self.config.REGION,
            config=Config(signature_version='s3v4')
        )
        
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            self.client.head_bucket(Bucket=self.config.BUCKET_NAME)
            logger.info(f"Bucket '{self.config.BUCKET_NAME}' exists")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.client.create_bucket(Bucket=self.config.BUCKET_NAME)
                logger.info(f"Created bucket '{self.config.BUCKET_NAME}'")
            else:
                raise
    
    def upload_file(self, file_path: str, object_name: str) -> bool:
        """Upload file to MinIO"""
        try:
            self.client.upload_file(
                file_path,
                self.config.BUCKET_NAME,
                object_name
            )
            logger.info(f"Uploaded {file_path} to {object_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {file_path}: {e}")
            return False
    
    def download_file(self, object_name: str, file_path: str) -> bool:
        """Download file from MinIO"""
        try:
            self.client.download_file(
                self.config.BUCKET_NAME,
                object_name,
                file_path
            )
            logger.info(f"Downloaded {object_name} to {file_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download {object_name}: {e}")
            return False
    
    def list_objects(self, prefix: str = '') -> List[str]:
        """List objects in bucket with optional prefix"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.config.BUCKET_NAME,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
        except ClientError as e:
            logger.error(f"Failed to list objects: {e}")
            return []
    
    def delete_object(self, object_name: str) -> bool:
        """Delete object from MinIO"""
        try:
            self.client.delete_object(
                Bucket=self.config.BUCKET_NAME,
                Key=object_name
            )
            logger.info(f"Deleted {object_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete {object_name}: {e}")
            return False
    
    def put_object(self, object_name: str, data: bytes) -> bool:
        """Put object directly to MinIO"""
        try:
            self.client.put_object(
                Bucket=self.config.BUCKET_NAME,
                Key=object_name,
                Body=data
            )
            logger.info(f"Put object {object_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to put object {object_name}: {e}")
            return False
    
    def get_object(self, object_name: str) -> Optional[bytes]:
        """Get object from MinIO"""
        try:
            response = self.client.get_object(
                Bucket=self.config.BUCKET_NAME,
                Key=object_name
            )
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Failed to get object {object_name}: {e}")
            return None
