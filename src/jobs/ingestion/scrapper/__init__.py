from src.jobs.ingestion.scrapper.ai_config import (
    ask_ai_for_source_config,
    complete_source_config_from_detail_page,
    ensure_source_configs,
    generate_source_config_from_ai,
)
from src.jobs.ingestion.scrapper.field_model import SCRAPER_FIELD_MODEL, ScraperFieldModel
from src.jobs.ingestion.scrapper.http_client import fetch_html, fetch_source_html
from src.jobs.ingestion.scrapper.html_parser import extract_value_by_selector
from src.jobs.ingestion.scrapper.job_extractor import (
    extract_description_html,
    extract_job_from_html,
    extract_job_items_html,
    extract_jobs_for_source,
    fetch_job_detail_html,
    normalize_job_url,
)
from src.jobs.ingestion.scrapper.pipeline import collect_jobs_html_by_source, run_ingestion_pipeline
from src.jobs.ingestion.scrapper.selector_config import (
    get_missing_source_configs,
    is_source_config_complete,
    load_source_configs,
    write_source_configs,
)
from src.jobs.ingestion.scrapper.sources import load_job_sources

__all__ = [
    "ask_ai_for_source_config",
    "collect_jobs_html_by_source",
    "complete_source_config_from_detail_page",
    "ensure_source_configs",
    "extract_description_html",
    "extract_job_from_html",
    "extract_job_items_html",
    "extract_jobs_for_source",
    "extract_value_by_selector",
    "fetch_html",
    "fetch_source_html",
    "fetch_job_detail_html",
    "generate_source_config_from_ai",
    "get_missing_source_configs",
    "is_source_config_complete",
    "load_job_sources",
    "load_source_configs",
    "normalize_job_url",
    "run_ingestion_pipeline",
    "SCRAPER_FIELD_MODEL",
    "ScraperFieldModel",
    "write_source_configs",
]
