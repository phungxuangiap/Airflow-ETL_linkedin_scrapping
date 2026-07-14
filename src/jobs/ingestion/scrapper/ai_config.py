import json
import time
from typing import Any, Dict, List

import requests

from src.jobs.ingestion.scrapper.constants import (
    AI_API_URL,
    AI_MODEL,
    AI_REQUEST_MAX_ATTEMPTS,
    AI_REQUEST_RETRY_DELAY_SECONDS,
    AI_REQUEST_TIMEOUT_SECONDS,
)
from src.jobs.ingestion.scrapper.env import get_ai_api_key
from src.jobs.ingestion.scrapper.field_model import SCRAPER_FIELD_MODEL
from src.jobs.ingestion.scrapper.html_parser import extract_value_by_selector
from src.jobs.ingestion.scrapper.job_extractor import fetch_job_detail_html
from src.jobs.ingestion.scrapper.selector_config import (
    get_missing_source_configs,
    is_source_config_complete,
    load_source_configs,
    normalize_ai_config,
    write_source_configs,
)
from src.jobs.ingestion.scrapper.sources import load_job_sources, source_entry_url
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def extract_json_object(raw_response: str) -> Dict[str, Any]:
    """Parse a JSON object from an AI response, tolerating wrapped JSON text."""
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
    """Ask the configured AI model to infer selector config values from sample HTML."""
    api_key = get_ai_api_key()
    if not api_key:
        LOGGER.warning("DEEPSEEK_API_KEY is not configured; cannot auto-generate selectors for %s", source_name)
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
            "CSS attribute selector operators must include '='. Correct examples: 'a[href^=\"/vi/tim-viec-lam/\"]@href', 'a[href*=\"tim-viec-lam\"]@href', 'a[href$=\".html\"]@href'. Incorrect examples: 'a[href^\"/vi/tim-viec-lam/\"]@href', 'a[href*\"job\"]@href', 'a[href$\".html\"]@href'.",
            "Before returning JSON, validate selector syntax mentally and never omit '=' after CSS attribute operators such as ^=, *=, $=, ~=, |=.",
            "Never return a bare attribute-existence selector as the CSS selector part, such as '[data-search--job-selection-job-url-value]@data-search--job-selection-job-url-value'. It is invalid for this scraper. If an attribute selector is unavoidable, qualify it with a tag and prefer a value match, for example 'a[data-search--job-selection-job-url-value]@data-search--job-selection-job-url-value' or 'div[data-testid=\"job-card\"]@text()'.",
            "Avoid custom data attributes with repeated hyphens in the CSS selector part when a normal tag/href selector exists. For job_url, prefer anchor href selectors and avoid data-search--* attributes.",
            "Prefer stable semantic selectors over styling classes: href patterns, meaningful attributes, tag structure, itemprop/aria/data attributes, and domain classes such as job-item, job-title, company-name, salary, job-link.",
            "For salary, return a selector only when the extracted text is an actual salary value or accepted salary text such as numeric ranges, currency symbols, usd/vnd/vnđ/trieu/triệu/million, negotiable, competitive, thoa thuan, or thỏa thuận. Do not select login-gated placeholders.",
            "For salary, return null if the only available salary-like text says the user must sign in or log in to view salary, for example 'Sign in to view salary', 'Login to view salary', 'Đăng nhập để xem lương', 'Dang nhap de xem luong', 'Ứng tuyển để xem lương', or similar hidden-salary prompts.",
            "Do not use Tailwind utility classes or generated UI classes such as flex, gap-*, p-*, m-*, w-*, h-*, rounded-*, shadow, border, text-*, bg-*, line-clamp-*, md:*, lg:*, hover:*, or classes containing '/', '[', ']', ':'.",
            "For job_url, prefer href-pattern selectors such as 'a[href^=\"/viec-lam/\"]@href', 'a[href^=\"/detail-jobs/\"]@href', or 'a[href*=\"job\"]@href' instead of class-based selectors.",
            "For ITviec job_url specifically, prefer an anchor selector like 'a[href^=\"/it-jobs/\"]@href', 'a[href*=\"/it-jobs/\"]@href', or the closest valid 'a@href'. Do not use '[data-search--job-selection-job-url-value]@data-search--job-selection-job-url-value'.",
            "For topdev job_url specifically, prefer 'a[href^=\"/viec-lam/\"]@href'.",
            "Use short selectors rooted inside the supplied job card HTML. Avoid long brittle chains with many nth-child levels unless no semantic selector exists.",
            "Do not invent selectors that are not supported by the HTML, except prefix_job_url may be inferred from absolute URLs or source context.",
        ],
    }
    payload = {
        "model": AI_MODEL,
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

    for attempt in range(1, AI_REQUEST_MAX_ATTEMPTS + 1):
        try:
            LOGGER.info(
                "Requesting AI selector config for %s, attempt %s/%s",
                source_name,
                attempt,
                AI_REQUEST_MAX_ATTEMPTS,
            )
            response = requests.post(AI_API_URL, headers=headers, json=payload, timeout=AI_REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return extract_json_object(content)
        except requests.Timeout:
            LOGGER.warning(
                "AI request timeout while generating config for source %s on attempt %s/%s",
                source_name,
                attempt,
                AI_REQUEST_MAX_ATTEMPTS,
            )
        except requests.RequestException as exc:
            LOGGER.warning(
                "AI request failed while generating config for source %s on attempt %s/%s: %s",
                source_name,
                attempt,
                AI_REQUEST_MAX_ATTEMPTS,
                exc,
            )
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            LOGGER.error("AI response is invalid for source %s: %s", source_name, exc)
            return {}

        if attempt < AI_REQUEST_MAX_ATTEMPTS:
            LOGGER.info("Waiting %.2fs before retrying AI request", AI_REQUEST_RETRY_DELAY_SECONDS)
            time.sleep(AI_REQUEST_RETRY_DELAY_SECONDS)

    LOGGER.error("AI request failed after %s attempts for source %s", AI_REQUEST_MAX_ATTEMPTS, source_name)
    return {}


def generate_source_config_from_ai(source_name: str, job_html: str, source_url: str = "") -> Dict[str, Any]:
    """Generate and normalize a complete source selector config from one job HTML block."""
    ai_config = ask_ai_for_source_config(source_name, job_html, list(SCRAPER_FIELD_MODEL.source_config_fields), source_url)
    return normalize_ai_config(source_name, ai_config)


def complete_source_config_from_detail_page(
    source: Dict[str, Any],
    source_config: Dict[str, Any],
    job_html: str,
) -> Dict[str, Any]:
    """Use a job detail page to fill missing enrichment selectors in a source config."""
    job_url = extract_value_by_selector(job_html, source_config.get("job_url"))
    detail_html = fetch_job_detail_html(source, job_url, source_config.get("prefix_job_url") or source.get("base_url", ""))
    if not detail_html:
        return source_config

    missing_fields = [field for field in SCRAPER_FIELD_MODEL.detail_enrichment_fields if not source_config.get(field)]
    if not missing_fields:
        return source_config

    detail_config = ask_ai_for_source_config(source["source_name"], detail_html, missing_fields, source_entry_url(source))
    for field in missing_fields:
        if detail_config.get(field):
            source_config[field] = detail_config[field]

    return normalize_ai_config(source["source_name"], source_config)


def ensure_source_configs(jobs_html: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """Ensure every configured source has selectors, generating missing configs with AI."""
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

        generated_config = generate_source_config_from_ai(source_name, first_job_html, source_entry_url(source))
        if not is_source_config_complete(generated_config):
            LOGGER.warning("Generated selector config for %s is incomplete; skip writing it", source_name)
            continue
        config_by_source[source_name] = generated_config

    final_configs = list(config_by_source.values())
    write_source_configs(final_configs)
    return final_configs
