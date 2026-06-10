import importlib
import json
import os
from pathlib import Path
from pprint import pformat
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests

from src.configs import jobs_source
from src.utils.logger import get_logger

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - runtime dependency guard
    BeautifulSoup = None


LOGGER = get_logger(__name__)
REQUEST_TIMEOUT_SECONDS = 20
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = os.getenv("GROQ_MODEL", "qwen/qwen3-32b")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SOURCE_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "source_config.py"
SOURCE_CONFIG_FIELDS = [
    "source_name",
    "title",
    "company",
    "company_location",
    "location_working_type",
    "working_type",
    "date_posted",
    "number_applicants",
    "prefix_job_url",
    "job_url",
    "description",
    "salary",
    "role",
    "level",
]
DETAIL_ENRICHMENT_FIELDS = [
    "description",
    "salary",
    "role",
    "level",
    "working_type",
    "location_working_type",
    "date_posted",
    "number_applicants",
]
NULLABLE_FIELDS = set(SOURCE_CONFIG_FIELDS) - {"source_name", "title", "prefix_job_url", "job_url"}


def load_job_sources() -> List[Dict[str, Any]]:
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


def fetch_html(url: str) -> Optional[str]:
    if not url:
        LOGGER.warning("Skip fetching empty URL")
        return None

    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.text
    except requests.Timeout:
        LOGGER.error("Request timeout for URL: %s", url)
    except requests.RequestException as exc:
        LOGGER.error("Request failed for URL %s: %s", url, exc)
    return None


def _require_html_parser() -> None:
    if BeautifulSoup is None:
        raise RuntimeError("BeautifulSoup is required for HTML parsing. Install beautifulsoup4 to run scrapper.py")


def _select_elements(html: str, selector: Optional[str]) -> List[Any]:
    _require_html_parser()
    if not html or not selector:
        return []
    return BeautifulSoup(html, "html.parser").select(selector)


def extract_job_items_html(source: Dict[str, Any], html: Optional[str]) -> List[str]:
    source_name = source.get("source_name", "unknown")
    entry_point = source.get("entry_point")
    if not html:
        LOGGER.warning("No HTML to extract job items for source %s", source_name)
        return []

    try:
        job_items = [str(element) for element in _select_elements(html, entry_point)]
    except Exception as exc:
        LOGGER.error("Cannot extract job items for source %s: %s", source_name, exc)
        return []

    LOGGER.info("Extracted %s job item HTML blocks for source %s", len(job_items), source_name)
    return job_items


def _source_entry_url(source: Dict[str, Any]) -> str:
    base_url = source.get("base_url", "")
    return urljoin(base_url, source.get("url") or base_url)


def collect_jobs_html_by_source() -> Dict[str, List[str]]:
    jobs_html: Dict[str, List[str]] = {}
    for source in load_job_sources():
        source_name = source["source_name"]
        entry_url = _source_entry_url(source)
        LOGGER.info("Crawling source %s from %s", source_name, entry_url)
        html = fetch_html(entry_url)
        jobs_html[source_name] = extract_job_items_html(source, html)
    return jobs_html


def load_source_configs() -> List[Dict[str, Any]]:
    if not SOURCE_CONFIG_PATH.exists():
        return []

    try:
        module = importlib.import_module("src.configs.source_config")
        importlib.reload(module)
        configs = getattr(module, "SOURCE_CONFIGS", None) or getattr(module, "source_configs", None)
        if configs is None:
            LOGGER.warning("source_config.py exists but SOURCE_CONFIGS is not defined")
            return []
        return list(configs.values()) if isinstance(configs, dict) else list(configs)
    except Exception as exc:
        LOGGER.error("Cannot load source_config.py: %s", exc)
        return []


def _is_config_field_complete(config: Dict[str, Any], field: str) -> bool:
    if field in NULLABLE_FIELDS:
        return field in config
    return bool(config.get(field))


def is_source_config_complete(source_config: Dict[str, Any]) -> bool:
    return all(_is_config_field_complete(source_config, field) for field in SOURCE_CONFIG_FIELDS)


def get_missing_source_configs(
    sources: Iterable[Dict[str, Any]],
    source_configs: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    config_by_source = {config.get("source_name"): config for config in source_configs}
    missing_sources = []

    for source in sources:
        source_name = source["source_name"]
        config = config_by_source.get(source_name)
        if not config or not is_source_config_complete(config):
            LOGGER.info("Source %s is missing selector config", source_name)
            missing_sources.append(source)

    return missing_sources


def _normalize_ai_config(source_name: str, ai_config: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {field: ai_config.get(field) for field in SOURCE_CONFIG_FIELDS}
    normalized["source_name"] = source_name
    normalized["prefix_job_url"] = normalized.get("prefix_job_url") or ""

    for field in SOURCE_CONFIG_FIELDS:
        if field not in normalized:
            normalized[field] = None if field in NULLABLE_FIELDS else ""

    missing_required = [field for field in SOURCE_CONFIG_FIELDS if not _is_config_field_complete(normalized, field)]
    if missing_required:
        LOGGER.warning("AI config for source %s is missing required fields: %s", source_name, missing_required)
    return normalized


def _load_env_file() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _get_groq_api_key() -> Optional[str]:
    _load_env_file()
    return os.getenv("GROQ_API_KEY")


def _extract_json_object(raw_response: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw_response)
    except json.JSONDecodeError:
        start = raw_response.find("{")
        end = raw_response.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        try:
            parsed = json.loads(raw_response[start : end + 1])
        except json.JSONDecodeError:
            return {}

    if isinstance(parsed, list):
        return parsed[0] if parsed else {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def ask_ai_for_source_config(
    source_name: str,
    html: str,
    fields: List[str],
    source_url: str = "",
) -> Dict[str, Any]:
    api_key = _get_groq_api_key()
    if not api_key:
        LOGGER.warning("GROQ_API_KEY is not configured; cannot auto-generate selectors for %s", source_name)
        return {}

    prompt = {
        "task": "Extract CSS selectors or attribute names for job fields from this HTML.",
        "source_name": source_name,
        "source_url": source_url,
        "fields": fields,
        "html": html,
        "rules": [
            "Return JSON object only, no markdown.",
            "Use null for nullable fields that are not present.",
            "For prefix_job_url, return the absolute scheme and host used to resolve relative job_url values, for example 'https://careerviet.vn'.",
            "Do not return null or an empty string for prefix_job_url when job_url is relative.",
            "For text extraction, use the format 'css_selector@text()', for example 'h2 a.job_link@text()'.",
            "For attribute extraction, use the format 'css_selector@attribute', for example 'a@href' or 'h2 a.job_link@title'.",
            "Do not invent selectors that are not supported by the HTML, except prefix_job_url may be inferred from absolute URLs or source context.",
        ],
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You extract reliable CSS selectors from job listing HTML and return strict JSON only.",
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        response = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        return _extract_json_object(content)
    except requests.Timeout:
        LOGGER.error("Groq request timeout while generating config for source %s", source_name)
    except requests.RequestException as exc:
        LOGGER.error("Groq request failed while generating config for source %s: %s", source_name, exc)
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        LOGGER.error("Groq response is invalid for source %s: %s", source_name, exc)
    return {}


def generate_source_config_from_ai(source_name: str, job_html: str, source_url: str = "") -> Dict[str, Any]:
    ai_config = ask_ai_for_source_config(source_name, job_html, SOURCE_CONFIG_FIELDS, source_url)
    return _normalize_ai_config(source_name, ai_config)


def extract_value_by_selector(job_html: str, selector_config: Optional[str]) -> Optional[str]:
    if not selector_config:
        return None

    _require_html_parser()
    soup = BeautifulSoup(job_html, "html.parser")
    selector = selector_config.strip()

    if selector.startswith("@"):
        attribute = selector[1:].strip()
        if attribute == "text()":
            return soup.get_text(" ", strip=True)
        return soup.get(attribute)

    if "@" in selector and not selector.startswith("["):
        css_selector, attribute = selector.rsplit("@", 1)
        element = soup.select_one(css_selector.strip()) if css_selector.strip() else soup
        if not element:
            return None

        attribute = attribute.strip()
        if attribute == "text()":
            return element.get_text(" ", strip=True)
        return element.get(attribute)

    element = soup.select_one(selector)
    if element:
        return element.get_text(" ", strip=True)

    if soup.has_attr(selector):
        return soup.get(selector)

    LOGGER.debug("Selector not found: %s", selector_config)
    return None


def fetch_job_detail_html(source: Dict[str, Any], job_url: Optional[str], prefix_job_url: str = "") -> Optional[str]:
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

    detail_blocks = _select_elements(detail_html, detail_entry_point)
    if not detail_blocks:
        LOGGER.warning("Detail entry point not found for source %s at %s", source.get("source_name"), detail_url)
        return BeautifulSoup(detail_html, "html.parser").get_text(" ", strip=True)
    return detail_blocks[0].get_text(" ", strip=True)


def complete_source_config_from_detail_page(
    source: Dict[str, Any],
    source_config: Dict[str, Any],
    job_html: str,
) -> Dict[str, Any]:
    job_url = extract_value_by_selector(job_html, source_config.get("job_url"))
    detail_html = fetch_job_detail_html(source, job_url, source_config.get("prefix_job_url") or source.get("base_url", ""))
    if not detail_html:
        return source_config

    missing_fields = [field for field in DETAIL_ENRICHMENT_FIELDS if not source_config.get(field)]
    if not missing_fields:
        return source_config

    detail_config = ask_ai_for_source_config(source["source_name"], detail_html, missing_fields, _source_entry_url(source))
    for field in missing_fields:
        if detail_config.get(field):
            source_config[field] = detail_config[field]

    return _normalize_ai_config(source["source_name"], source_config)


def write_source_config_py(source_configs: List[Dict[str, Any]]) -> None:
    ordered_configs = sorted(source_configs, key=lambda config: config.get("source_name", ""))
    content = "SOURCE_CONFIGS = " + pformat(ordered_configs, sort_dicts=False, width=120) + "\n"
    SOURCE_CONFIG_PATH.write_text(content, encoding="utf-8")
    LOGGER.info("Wrote %s source configs to %s", len(ordered_configs), SOURCE_CONFIG_PATH)


def ensure_source_configs(jobs_html: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    sources = load_job_sources()
    source_configs = load_source_configs()
    missing_sources = get_missing_source_configs(sources, source_configs)

    if not missing_sources:
        LOGGER.info("All source configs are complete; skip AI config generation")
        return source_configs

    config_by_source = {config.get("source_name"): config for config in source_configs}
    for source in missing_sources:
        source_name = source["source_name"]
        first_job_html = next(iter(jobs_html.get(source_name, [])), None)
        if not first_job_html:
            LOGGER.warning("Cannot generate config for %s because no job HTML was crawled", source_name)
            continue

        generated_config = generate_source_config_from_ai(source_name, first_job_html, _source_entry_url(source))
        config_by_source[source_name] = generated_config

    final_configs = list(config_by_source.values())
    write_source_config_py(final_configs)
    return final_configs


def normalize_job_url(job_url: Optional[str], prefix_job_url: str) -> Optional[str]:
    if not job_url:
        return None
    return urljoin(prefix_job_url, job_url) if prefix_job_url else job_url


def extract_description_html(job_html: str, detail_entry_point: Optional[str]) -> Optional[str]:
    if not detail_entry_point:
        return None

    detail_blocks = _select_elements(job_html, detail_entry_point)
    return str(detail_blocks[0]) if detail_blocks else None


def extract_job_from_html(
    source_name: str,
    job_html: str,
    source_config: Dict[str, Any],
    source: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[str]]:
    job: Dict[str, Optional[str]] = {"source_name": source_name}
    for field in SOURCE_CONFIG_FIELDS:
        if field in {"source_name", "prefix_job_url", "description"}:
            continue
        value = extract_value_by_selector(job_html, source_config.get(field))
        job[field] = value if value else None

    prefix_job_url = source_config.get("prefix_job_url") or ""
    job["job_url"] = normalize_job_url(job.get("job_url"), prefix_job_url)
    job["description"] = fetch_job_detail_html(source, job.get("job_url"), prefix_job_url) if source else None
    return job


def extract_jobs_by_source(
    jobs_html: Dict[str, List[str]],
    source_configs: List[Dict[str, Any]],
) -> List[Dict[str, Optional[str]]]:
    jobs: List[Dict[str, Optional[str]]] = []
    config_by_source = {config.get("source_name"): config for config in source_configs}
    source_by_name = {source.get("source_name"): source for source in load_job_sources()}

    for source_name, html_items in jobs_html.items():
        source_config = config_by_source.get(source_name)
        source = source_by_name.get(source_name)
        if not source_config:
            LOGGER.warning("Skip source %s because selector config is missing", source_name)
            continue

        extracted_count = 0
        for job_html in html_items:
            try:
                jobs.append(extract_job_from_html(source_name, job_html, source_config, source))
                extracted_count += 1
            except Exception as exc:
                LOGGER.error("Cannot extract job for source %s: %s", source_name, exc)

        LOGGER.info("Extracted %s jobs for source %s", extracted_count, source_name)

    LOGGER.info("Extracted %s jobs in total", len(jobs))
    return jobs


def run_ingestion_pipeline() -> List[Dict[str, Optional[str]]]:
    LOGGER.info("Start job ingestion pipeline")
    jobs_html = collect_jobs_html_by_source()
    source_configs = ensure_source_configs(jobs_html)
    jobs = extract_jobs_by_source(jobs_html, source_configs)
    LOGGER.info("Finished job ingestion pipeline with %s jobs", jobs)
    return jobs


if __name__ == "__main__":
    run_ingestion_pipeline()
