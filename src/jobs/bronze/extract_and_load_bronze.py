from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from minio.commonconfig import CopySource

from src.utils.logger import get_logger
from src.utils.minio_client import get_minio_client
from src.configs.settings import settings

logger = get_logger(__name__)


def _upload_generated_file_to_staging(extract_result: Dict[str, Any], object_name: str, source: str, load_date: str) -> Dict[str, Any]:
    client = get_minio_client()
    file_path = Path(extract_result['filepath'])

    logger.info(f"Uploading to Bronze staging: {object_name}")

    if not client.upload_file(str(file_path), object_name):
        raise Exception(f"Failed to upload {file_path.name} to Bronze staging")

    file_path.unlink()
    logger.info(f"Uploaded and deleted: {file_path.name}")

    return {
        'source': source,
        'job_count': extract_result['job_count'],
        'file_size': extract_result['file_size'],
        'staging_path': object_name,
        'load_date': load_date,
        'success': True
    }


def extract_and_stage_api_jobs(load_date: str, **context) -> Dict[str, Any]:
    try:
        from data_generation.generate_api_data import generate_api_files

        logger.info("Extracting API jobs...")
        extract_result = generate_api_files(base_path=settings.API_SOURCE_DIR)

        logger.info(f"API jobs extracted: {extract_result['job_count']} jobs")
        logger.info(f"File created: {extract_result['filepath']}")

        file_path = Path(extract_result['filepath'])
        object_name = f"STAGING/BRONZE/api_data/{settings.ENTITY_TYPE}/load_date={load_date}/{file_path.name}"

        return _upload_generated_file_to_staging(extract_result, object_name, 'api', load_date)

    except Exception as e:
        logger.error(f"Failed to extract and stage API jobs: {e}")
        raise


def extract_and_stage_scrapped_jobs(load_date: str, **context) -> Dict[str, Any]:
    try:
        from data_generation.generate_scrapped_data import generate_scrapped_files

        logger.info("Extracting scrapped jobs...")
        extract_result = generate_scrapped_files(base_path=settings.SCRAPPED_SOURCE_DIR)

        logger.info(f"Scrapped jobs extracted: {extract_result['job_count']} jobs")
        logger.info(f"File created: {extract_result['filepath']}")

        file_path = Path(extract_result['filepath'])
        object_name = f"STAGING/BRONZE/crawler_data/{settings.SOURCE_WEB_NAME}/{settings.ENTITY_TYPE}/load_date={load_date}/{file_path.name}"

        return _upload_generated_file_to_staging(extract_result, object_name, 'scrapped', load_date)

    except Exception as e:
        logger.error(f"Failed to extract and stage scrapped jobs: {e}")
        raise


def _copy_objects(client, source_prefix: str, target_prefix: str) -> List[str]:
    objects = client.list_objects(prefix=source_prefix, recursive=True)
    if not objects:
        raise ValueError(f"No staged objects found with prefix {source_prefix}")

    promoted = []
    for source_object in objects:
        target_object = source_object.replace(source_prefix, target_prefix, 1)
        client.client.copy_object(
            client.bucket_name,
            target_object,
            CopySource(client.bucket_name, source_object)
        )
        promoted.append(target_object)
        logger.info(f"Promoted s3://{client.bucket_name}/{source_object} to s3://{client.bucket_name}/{target_object}")

    return promoted


def process_to_staging(load_date: str = None, **context) -> Dict[str, Any]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Starting Bronze process to staging for load_date={load_date}")

    api_result = extract_and_stage_api_jobs(load_date=load_date, **context)
    scrapped_result = extract_and_stage_scrapped_jobs(load_date=load_date, **context)

    result = {
        'api_jobs': api_result['job_count'],
        'scrapped_jobs': scrapped_result['job_count'],
        'total_jobs': api_result['job_count'] + scrapped_result['job_count'],
        'api_staging_path': api_result['staging_path'],
        'scrapped_staging_path': scrapped_result['staging_path'],
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Bronze staging completed: {result}")
    return result


def promote_staging_to_bronze(load_date: str = None, **context) -> Dict[str, Any]:
    if load_date is None:
        load_date = context.get('ds') or datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Promoting Bronze staging to Bronze for load_date={load_date}")

    client = get_minio_client()
    api_staging_prefix = f"STAGING/BRONZE/api_data/{settings.ENTITY_TYPE}/load_date={load_date}/"
    api_target_prefix = f"BRONZE/api_data/{settings.ENTITY_TYPE}/load_date={load_date}/"
    scrapped_staging_prefix = f"STAGING/BRONZE/crawler_data/{settings.SOURCE_WEB_NAME}/{settings.ENTITY_TYPE}/load_date={load_date}/"
    scrapped_target_prefix = f"BRONZE/crawler_data/{settings.SOURCE_WEB_NAME}/{settings.ENTITY_TYPE}/load_date={load_date}/"

    api_promoted = _copy_objects(client, api_staging_prefix, api_target_prefix)
    scrapped_promoted = _copy_objects(client, scrapped_staging_prefix, scrapped_target_prefix)

    result = {
        'api_files_promoted': len(api_promoted),
        'scrapped_files_promoted': len(scrapped_promoted),
        'total_files_promoted': len(api_promoted) + len(scrapped_promoted),
        'api_bronze_paths': api_promoted,
        'scrapped_bronze_paths': scrapped_promoted,
        'load_date': load_date,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Bronze promotion completed: {result}")
    return result


def run(load_date: str = None, **context):
    staging_result = process_to_staging(load_date=load_date, **context)
    promotion_result = promote_staging_to_bronze(load_date=staging_result['load_date'], **context)

    return {
        'staging': staging_result,
        'promotion': promotion_result,
        'load_date': staging_result['load_date'],
        'timestamp': datetime.now().isoformat()
    }
