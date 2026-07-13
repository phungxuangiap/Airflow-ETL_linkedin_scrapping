from typing import Any, Dict, List
from urllib.parse import urljoin

from src.configs import jobs_source
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def load_job_sources() -> List[Dict[str, Any]]:
    """Load configured job sources from the jobs_source config module."""
    configured_sources = getattr(jobs_source, "JOBS_SOURCE", None) or getattr(jobs_source, "jobs_source", None)
    if configured_sources is None:
        configured_sources = getattr(jobs_source, "JOB_SOURCES", None) or getattr(jobs_source, "job_sources", None)

    if configured_sources is None:
        LOGGER.warning("No job sources configured in src.configs.jobs_source")
        return []

    sources = list(configured_sources.values()) if isinstance(configured_sources, dict) else list(configured_sources)
    valid_sources = [source for source in sources if source.get("source_name")]
    LOGGER.info("Loaded %s job sources", len(valid_sources))
    return valid_sources


def source_entry_url(source: Dict[str, Any]) -> str:
    """Build the absolute entry URL used to crawl a job source."""
    base_url = source.get("base_url", "")
    return urljoin(base_url, source.get("url") or base_url)
