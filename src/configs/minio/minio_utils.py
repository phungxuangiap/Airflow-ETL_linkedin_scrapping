"""
MinIO utilities for ETL operations
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from src.configs.minio.minio_client import MinIOClient, MinIOConfig

logger = logging.getLogger(__name__)


def upload_api_data_to_minio(api_data_path: str, **context) -> str:
    """
    Upload API source data to MinIO
    
    Args:
        api_data_path: Path to API data file
        
    Returns:
        S3 object path where data was uploaded
    """
    client = MinIOClient()
    config = MinIOConfig()
    
    file_name = Path(api_data_path).name
    object_name = f"{config.API_SOURCE_PATH}{file_name}"
    
    success = client.upload_file(api_data_path, object_name)
    
    if success:
        logger.info(f"API data uploaded to: s3://{config.BUCKET_NAME}/{object_name}")
        return object_name
    else:
        raise Exception(f"Failed to upload API data: {api_data_path}")


def upload_scrapped_data_to_minio(scrapped_data_path: str, **context) -> str:
    """
    Upload scrapped data to MinIO
    
    Args:
        scrapped_data_path: Path to scrapped data file
        
    Returns:
        S3 object path where data was uploaded
    """
    client = MinIOClient()
    config = MinIOConfig()
    
    file_name = Path(scrapped_data_path).name
    object_name = f"{config.SCRAPPED_DATA_PATH}{file_name}"
    
    success = client.upload_file(scrapped_data_path, object_name)
    
    if success:
        logger.info(f"Scrapped data uploaded to: s3://{config.BUCKET_NAME}/{object_name}")
        return object_name
    else:
        raise Exception(f"Failed to upload scrapped data: {scrapped_data_path}")


def list_minio_objects(prefix: str = '', **context) -> List[str]:
    """
    List objects in MinIO bucket
    
    Args:
        prefix: Filter by prefix (e.g., 'api_source/', 'scrapped/')
        
    Returns:
        List of object keys
    """
    client = MinIOClient()
    objects = client.list_objects(prefix)
    logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
    return objects


def download_from_minio(object_name: str, local_path: str, **context) -> str:
    """
    Download object from MinIO to local storage
    
    Args:
        object_name: S3 object key
        local_path: Local file path to save
        
    Returns:
        Local file path
    """
    client = MinIOClient()
    success = client.download_file(object_name, local_path)
    
    if success:
        logger.info(f"Downloaded {object_name} to {local_path}")
        return local_path
    else:
        raise Exception(f"Failed to download {object_name}")


def upload_processed_results(processed_data_path: str, data_type: str, **context) -> str:
    """
    Upload processed results to MinIO
    
    Args:
        processed_data_path: Path to processed data file
        data_type: Type of data (e.g., 'validated', 'transformed')
        
    Returns:
        S3 object path
    """
    client = MinIOClient()
    config = MinIOConfig()
    
    file_name = Path(processed_data_path).name
    date_str = datetime.now().strftime("%Y-%m-%d")
    object_name = f"{config.PROCESSED_DATA_PATH}{data_type}/{date_str}/{file_name}"
    
    success = client.upload_file(processed_data_path, object_name)
    
    if success:
        logger.info(f"Processed data uploaded to: s3://{config.BUCKET_NAME}/{object_name}")
        return object_name
    else:
        raise Exception(f"Failed to upload processed data: {processed_data_path}")


def sync_api_data_to_minio(local_api_dir: str = '/tmp/api_sources', **context) -> int:
    """
    Sync all API data files from local directory to MinIO
    
    Args:
        local_api_dir: Local directory containing API data files
        
    Returns:
        Number of files uploaded
    """
    client = MinIOClient()
    config = MinIOConfig()
    
    api_dir = Path(local_api_dir)
    uploaded_count = 0
    
    if not api_dir.exists():
        logger.warning(f"API directory not found: {local_api_dir}")
        return 0
    
    for file_path in api_dir.glob('*.jsonl'):
        object_name = f"{config.API_SOURCE_PATH}{file_path.name}"
        if client.upload_file(str(file_path), object_name):
            uploaded_count += 1
            logger.info(f"Uploaded {file_path.name}")
    
    logger.info(f"Total API files uploaded to MinIO: {uploaded_count}")
    return uploaded_count


def sync_scrapped_data_to_minio(local_scrapped_dir: str = '/project_root/tmp/scrapping_script', **context) -> int:
    """
    Sync all scrapped data files from local directory to MinIO
    
    Args:
        local_scrapped_dir: Local directory containing scrapped data files
        
    Returns:
        Number of files uploaded
    """
    client = MinIOClient()
    config = MinIOConfig()
    
    scrapped_dir = Path(local_scrapped_dir)
    uploaded_count = 0
    
    if not scrapped_dir.exists():
        logger.warning(f"Scrapped directory not found: {local_scrapped_dir}")
        return 0
    
    for file_path in scrapped_dir.glob('*.jsonl'):
        object_name = f"{config.SCRAPPED_DATA_PATH}{file_path.name}"
        if client.upload_file(str(file_path), object_name):
            uploaded_count += 1
            logger.info(f"Uploaded {file_path.name}")
    
    logger.info(f"Total scrapped files uploaded to MinIO: {uploaded_count}")
    return uploaded_count


def upload_to_bronze_layer(
    api_source_dir: str = '/project_root/tmp/api_sources',
    scrapped_source_dir: str = '/project_root/tmp/scrapping_script',
    source_web_name: str = 'linkedin',
    entity: str = 'jobs',
    **context
) -> Dict[str, int]:
    """
    Upload data files to MinIO bronze layer with structured paths and auto-delete.
    
    Structure:
    - API data:     BRONZE/api_data/[entity]/load_date=YYYY-MM-DD/data_file.json
    - Scrapped:     BRONZE/crawler_data/[source_web_name]/[entity]/load_date=YYYY-MM-DD/data_file.json
    
    Args:
        api_source_dir: Local directory containing API data files
        scrapped_source_dir: Local directory containing scrapped data files
        source_web_name: Source website name (default: 'linkedin')
        entity: Entity type (default: 'jobs')
        **context: Airflow context containing execution_date
        
    Returns:
        Dict with counts of files uploaded from each source
    """
    client = MinIOClient()
    
    # Get execution date from Airflow context
    execution_date = context.get('execution_date')
    load_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Uploading data to bronze layer with load_date={load_date}")
    
    api_count = 0
    scrapped_count = 0
    
    # Upload API data to: BRONZE/api_data/[entity]/load_date=YYYY-MM-DD/
    api_dir = Path(api_source_dir)
    if api_dir.exists():
        for file_path in api_dir.glob('*.jsonl'):
            # Create bronze layer path for API data
            object_name = f"BRONZE/api_data/{entity}/load_date={load_date}/{file_path.name}"
            
            if client.upload_file(str(file_path), object_name):
                try:
                    file_path.unlink()  # Delete local file after upload
                    api_count += 1
                    logger.info(f"Uploaded and deleted API file: {file_path.name} -> {object_name}")
                except Exception as e:
                    logger.error(f"Failed to delete API file {file_path.name}: {e}")
            else:
                logger.error(f"Failed to upload API file: {file_path.name}")
    else:
        logger.warning(f"API source directory not found: {api_source_dir}")
    
    # Upload scrapped data to: BRONZE/crawler_data/[source_web_name]/[entity]/load_date=YYYY-MM-DD/
    scrapped_dir = Path(scrapped_source_dir)
    if scrapped_dir.exists():
        for file_path in scrapped_dir.glob('*.jsonl'):
            # Create bronze layer path for scrapped data
            object_name = f"BRONZE/crawler_data/{source_web_name}/{entity}/load_date={load_date}/{file_path.name}"
            
            if client.upload_file(str(file_path), object_name):
                try:
                    file_path.unlink()  # Delete local file after upload
                    scrapped_count += 1
                    logger.info(f"Uploaded and deleted scrapped file: {file_path.name} -> {object_name}")
                except Exception as e:
                    logger.error(f"Failed to delete scrapped file {file_path.name}: {e}")
            else:
                logger.error(f"Failed to upload scrapped file: {file_path.name}")
    else:
        logger.warning(f"Scrapped source directory not found: {scrapped_source_dir}")
    
    result = {
        'api_files_uploaded': api_count,
        'scrapped_files_uploaded': scrapped_count,
        'total_files_uploaded': api_count + scrapped_count,
        'load_date': load_date,
        'bronze_layer': 'BRONZE'
    }
    
    logger.info(f"Bronze layer upload completed: {result}")
    return result
