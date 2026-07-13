from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from src.jobs.ingestion.scrapper.field_model import SCRAPER_FIELD_MODEL
from src.jobs.ingestion.scrapper.html_parser import BeautifulSoup, extract_value_by_selector, select_elements
from src.jobs.ingestion.scrapper.http_client import fetch_html
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def extract_job_items_html(source: Dict[str, Any], html: Optional[str]) -> List[str]:
    """Extract individual job listing HTML blocks for a configured source."""
    source_name = source.get("source_name", "unknown")
    entry_point = source.get("entry_point")
    if not html:
        LOGGER.warning("No HTML to extract job items for source %s", source_name)
        return []

    try:
        job_items = [str(element) for element in select_elements(html, entry_point)]
    except Exception as exc:
        LOGGER.error("Cannot extract job items for source %s: %s", source_name, exc)
        return []

    LOGGER.info("Extracted %s job item HTML blocks for source %s", len(job_items), source_name)
    return job_items


def fetch_job_detail_html(source: Dict[str, Any], job_url: Optional[str], prefix_job_url: str = "") -> Optional[str]:
    """Fetch a job detail page and optionally reduce it to the configured detail section."""
    if not job_url:
        LOGGER.warning("Cannot fetch detail page for %s because job_url is empty", source.get("source_name"))
        return None

    detail_url = urljoin(prefix_job_url or source.get("base_url", ""), job_url)
    detail_html = fetch_html(detail_url)
    if not detail_html:
        return None

    detail_entry_point = source.get("detail_entry_point")
    if not detail_entry_point:
        return detail_html

    detail_blocks = select_elements(detail_html, detail_entry_point)
    if not detail_blocks:
        LOGGER.warning("Detail entry point not found for source %s at %s", source.get("source_name"), detail_url)
        return BeautifulSoup(detail_html, "html.parser").get_text(" ", strip=True)
    return detail_blocks[0].get_text(" ", strip=True)


def normalize_job_url(job_url: Optional[str], prefix_job_url: str) -> Optional[str]:
    """Resolve a job URL against its prefix when the scraped value is relative."""
    if not job_url:
        return None
    return urljoin(prefix_job_url, job_url) if prefix_job_url else job_url


def extract_description_html(job_html: str, detail_entry_point: Optional[str]) -> Optional[str]:
    """Extract the configured description/detail HTML block from a job listing."""
    if not detail_entry_point:
        return None

    detail_blocks = select_elements(job_html, detail_entry_point)
    return str(detail_blocks[0]) if detail_blocks else None


def extract_job_from_html(
    source_name: str,
    job_html: str,
    source_config: Dict[str, Any],
    source: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[str]]:
    """Convert one job listing HTML block into a normalized raw job dictionary."""
    job: Dict[str, Optional[str]] = {"source_name": source_name}
    for field in SCRAPER_FIELD_MODEL.listing_extract_fields:
        value = extract_value_by_selector(job_html, source_config.get(field))
        job[field] = value if value else None

    prefix_job_url = source_config.get(SCRAPER_FIELD_MODEL.prefix_job_url) or ""
    job[SCRAPER_FIELD_MODEL.job_url] = normalize_job_url(job.get(SCRAPER_FIELD_MODEL.job_url), prefix_job_url)
    if source:
        LOGGER.info(
            "Fetching job detail for source=%s title=%s url=%s",
            source_name,
            job.get(SCRAPER_FIELD_MODEL.title),
            job.get(SCRAPER_FIELD_MODEL.job_url),
        )
    job[SCRAPER_FIELD_MODEL.description] = (
        fetch_job_detail_html(source, job.get(SCRAPER_FIELD_MODEL.job_url), prefix_job_url) if source else None
    )
    return job


def extract_jobs_for_source(
    source: Dict[str, Any],
    html_items: List[str],
    source_config: Dict[str, Any],
) -> List[Dict[str, Optional[str]]]:
    """Extract raw jobs for one source from its job listing HTML blocks."""
    source_name = source["source_name"]
    source_jobs: List[Dict[str, Optional[str]]] = []

    for job_html in html_items:
        try:
            source_jobs.append(extract_job_from_html(source_name, job_html, source_config, source))
        except Exception as exc:
            LOGGER.error("Cannot extract job for source %s: %s", source_name, exc)

    LOGGER.info("Extracted %s jobs for source %s", len(source_jobs), source_name)
    return source_jobs
