from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from src.utils.logger import get_logger
from src.utils.minio_client import get_minio_client
from src.configs.settings import settings

logger = get_logger(__name__)


def extract_and_load_api_jobs(load_date: str, **context) -> Dict[str, Any]:
    try:
        # Step 1: Generate/Extract API data
        from data_generation.generate_api_data import generate_api_files

        logger.info("Extracting API jobs...")
        extract_result = generate_api_files(base_path=settings.API_SOURCE_DIR)

        logger.info(f"API jobs extracted: {extract_result['job_count']} jobs")
        logger.info(f"File created: {extract_result['filepath']}")

        # Step 2: Upload to Bronze layer immediately
        client = get_minio_client()

        file_path = Path(extract_result['filepath'])
        object_name = f"BRONZE/api_data/{settings.ENTITY_TYPE}/load_date={load_date}/{file_path.name}"

        logger.info(f"Uploading to Bronze: {object_name}")

        if client.upload_file(str(file_path), object_name):
            # Delete local file after successful upload
            file_path.unlink()
            logger.info(f"Uploaded and deleted: {file_path.name}")

            return {
                'source': 'api',
                'job_count': extract_result['job_count'],
                'file_size': extract_result['file_size'],
                'bronze_path': object_name,
                'load_date': load_date,
                'success': True
            }
        else:
            raise Exception(f"Failed to upload {file_path.name} to Bronze")

    except Exception as e:
        logger.error(f"Failed to extract and load API jobs: {e}")
        raise


def extract_and_load_scrapped_jobs(load_date: str, **context) -> Dict[str, Any]:
    try:
        # Step 1: Generate/Extract scrapped data
        from data_generation.generate_scrapped_data import generate_scrapped_files

        logger.info("Extracting scrapped jobs...")
        extract_result = generate_scrapped_files(base_path=settings.SCRAPPED_SOURCE_DIR)

        logger.info(f"Scrapped jobs extracted: {extract_result['job_count']} jobs")
        logger.info(f"File created: {extract_result['filepath']}")

        # Step 2: Upload to Bronze layer immediately
        client = get_minio_client()

        file_path = Path(extract_result['filepath'])
        object_name = f"BRONZE/crawler_data/{settings.SOURCE_WEB_NAME}/{settings.ENTITY_TYPE}/load_date={load_date}/{file_path.name}"

        logger.info(f"Uploading to Bronze: {object_name}")

        if client.upload_file(str(file_path), object_name):
            # Delete local file after successful upload
            file_path.unlink()
            logger.info(f"Uploaded and deleted: {file_path.name}")

            return {
                'source': 'scrapped',
                'job_count': extract_result['job_count'],
                'file_size': extract_result['file_size'],
                'bronze_path': object_name,
                'load_date': load_date,
                'success': True
            }
        else:
            raise Exception(f"Failed to upload {file_path.name} to Bronze")

    except Exception as e:
        logger.error(f"Failed to extract and load scrapped jobs: {e}")
        raise


def run(load_date: str = None, **context):
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Starting Bronze layer: Extract and Load for load_date={load_date}")

    # Extract and load API data
    api_result = extract_and_load_api_jobs(load_date=load_date, **context)
    logger.info(f"API data loaded to Bronze: {api_result['job_count']} jobs")

    # Extract and load scrapped data
    scrapped_result = extract_and_load_scrapped_jobs(load_date=load_date, **context)
    logger.info(f"Scrapped data loaded to Bronze: {scrapped_result['job_count']} jobs")

    # Combined result
    combined_result = {
        'api_jobs': api_result['job_count'],
        'scrapped_jobs': scrapped_result['job_count'],
        'total_jobs': api_result['job_count'] + scrapped_result['job_count'],
        'api_bronze_path': api_result['bronze_path'],
        'scrapped_bronze_path': scrapped_result['bronze_path'],
        'load_date': api_result['load_date'],
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Bronze layer completed: {combined_result['total_jobs']} total jobs loaded")

    return combined_result
