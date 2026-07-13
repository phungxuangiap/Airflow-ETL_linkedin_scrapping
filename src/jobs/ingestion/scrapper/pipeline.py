from typing import Any, Dict, List, Optional

from src.jobs.ingestion.scrapper.ai_config import generate_source_config_from_ai
from src.jobs.ingestion.scrapper.http_client import fetch_html
from src.jobs.ingestion.scrapper.job_extractor import extract_job_items_html, extract_jobs_for_source
from src.jobs.ingestion.scrapper.selector_config import (
    is_source_config_complete,
    load_source_configs,
    write_source_configs,
)
from src.jobs.ingestion.scrapper.sources import load_job_sources, source_entry_url
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def collect_jobs_html_by_source() -> Dict[str, List[str]]:
    """Crawl all configured sources and group extracted job item HTML by source name."""
    jobs_html: Dict[str, List[str]] = {}
    for source in load_job_sources():
        source_name = source["source_name"]
        entry_url = source_entry_url(source)
        LOGGER.info("Crawling source %s from %s", source_name, entry_url)
        html = fetch_html(entry_url)
        jobs_html.setdefault(source_name, []).extend(extract_job_items_html(source, html))
    return jobs_html


def get_source_config_for_crawled_source(
    source: Dict[str, Any],
    html_items: List[str],
    config_by_source: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return an existing complete config or generate one from crawled source HTML."""
    source_name = source["source_name"]
    source_config = config_by_source.get(source_name)
    if source_config and is_source_config_complete(source_config):
        return source_config

    first_job_html = next(iter(html_items), None)
    if not first_job_html:
        LOGGER.warning("Cannot generate config for %s because no job HTML was crawled", source_name)
        return source_config

    source_config = generate_source_config_from_ai(source_name, first_job_html, source_entry_url(source))
    if not is_source_config_complete(source_config):
        LOGGER.warning("Generated selector config for %s is incomplete; skip writing it", source_name)
        return config_by_source.get(source_name)

    config_by_source[source_name] = source_config
    write_source_configs(list(config_by_source.values()))
    return source_config


def run_ingestion_pipeline() -> List[Dict[str, Optional[str]]]:
    """Run the end-to-end ingestion flow and return crawled raw jobs."""
    LOGGER.info("Start job ingestion pipeline")
    jobs: List[Dict[str, Optional[str]]] = []
    config_by_source = {config.get("source_name"): config for config in load_source_configs()}

    for source in load_job_sources():
        source_name = source["source_name"]
        entry_url = source_entry_url(source)
        LOGGER.info("Crawling source %s from %s", source_name, entry_url)
        html = fetch_html(entry_url)
        html_items = extract_job_items_html(source, html)

        source_config = get_source_config_for_crawled_source(source, html_items, config_by_source)
        if not source_config or not is_source_config_complete(source_config):
            LOGGER.warning("Skip source %s because selector config is missing", source_name)
            continue

        jobs.extend(extract_jobs_for_source(source, html_items, source_config))

    LOGGER.info("Finished job ingestion pipeline with %s jobs", len(jobs))
    return jobs
