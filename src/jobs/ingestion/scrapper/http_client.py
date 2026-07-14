import random
import time
from typing import Any, Dict, Optional

import requests

from src.jobs.ingestion.scrapper.constants import (
    BYPASS_HTML_REQUEST_TIMEOUT_SECONDS,
    PROXY_SWITCH_INTERVAL,
    REQUEST_DELAY_MAX_SECONDS,
    REQUEST_DELAY_MIN_SECONDS,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT_SECONDS,
)
from src.jobs.ingestion.scrapper.env import (
    get_bypass_html_server_url,
    get_crawler_proxies,
    get_float_env,
    get_int_env,
)
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)
_CRAWLER_REQUEST_COUNT = 0
_LAST_FETCH_AT: Optional[float] = None
_MISSING_PROXY_CONFIG_LOGGED = False


def fetch_html(url: str) -> Optional[str]:
    """Fetch HTML content from a URL with human-like pacing and proxy rotation."""
    if not url:
        LOGGER.warning("Skip fetching empty URL")
        return None

    _wait_between_fetches()
    request_number = _next_crawler_request_number()
    proxy = _proxy_for_request(request_number)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS, proxies=proxies)
        response.raise_for_status()
        return response.text
    except requests.Timeout:
        LOGGER.error("Request timeout for URL: %s", url)
    except requests.RequestException as exc:
        LOGGER.error("Request failed for URL %s: %s", url, exc)
    return None


def fetch_source_html(source: Dict[str, Any], url: str) -> Optional[str]:
    """Fetch HTML for a source using its configured scraper strategy."""
    if source.get("scraper") == "bypass":
        return fetch_html_via_bypass_server(source, url)
    return fetch_html(url)


def fetch_html_via_bypass_server(source: Dict[str, Any], url: str) -> Optional[str]:
    """Fetch target HTML through the configured internal HTML server."""
    source_name = source.get("source_name", "unknown")
    server_url = get_bypass_html_server_url()
    if not server_url:
        LOGGER.error("BYPASS_HTML_SERVER_URL is not configured; cannot fetch source %s via bypass server", source_name)
        return None
    if not url:
        LOGGER.warning("Skip bypass fetching empty URL for source %s", source_name)
        return None

    _wait_between_fetches()
    _next_crawler_request_number()
    timeout = get_int_env("BYPASS_HTML_REQUEST_TIMEOUT_SECONDS", BYPASS_HTML_REQUEST_TIMEOUT_SECONDS)

    try:
        LOGGER.info("Fetching HTML via bypass server source=%s url=%s server=%s", source_name, url, server_url)
        response = requests.get(server_url, params={"url": url}, timeout=timeout)
        response.raise_for_status()
        if not response.text.strip():
            LOGGER.warning("Bypass server returned empty HTML for source=%s url=%s", source_name, url)
            return None
        LOGGER.info(
            "Bypass server returned %s chars for source=%s url=%s",
            len(response.text),
            source_name,
            url,
        )
        return response.text
    except requests.Timeout:
        LOGGER.error("Bypass server timeout for source=%s url=%s", source_name, url)
    except requests.RequestException as exc:
        LOGGER.error("Bypass server request failed for source=%s url=%s: %s", source_name, url, exc)
    return None


def _next_crawler_request_number() -> int:
    """Increment and return the crawler request count used for proxy rotation."""
    global _CRAWLER_REQUEST_COUNT
    _CRAWLER_REQUEST_COUNT += 1
    return _CRAWLER_REQUEST_COUNT


def _wait_between_fetches() -> None:
    """Sleep a random amount between crawler requests to avoid a fixed request cadence."""
    global _LAST_FETCH_AT

    if _LAST_FETCH_AT is None:
        _LAST_FETCH_AT = time.monotonic()
        return

    min_delay = get_float_env("CRAWLER_REQUEST_DELAY_MIN_SECONDS", REQUEST_DELAY_MIN_SECONDS)
    max_delay = get_float_env("CRAWLER_REQUEST_DELAY_MAX_SECONDS", REQUEST_DELAY_MAX_SECONDS)
    if max_delay < min_delay:
        LOGGER.warning("CRAWLER_REQUEST_DELAY_MAX_SECONDS is lower than min; using %.2fs", min_delay)
        max_delay = min_delay

    delay = random.uniform(min_delay, max_delay)
    LOGGER.info("Waiting %.2fs before next crawler request", delay)
    time.sleep(delay)
    _LAST_FETCH_AT = time.monotonic()


def _proxy_for_request(request_number: int) -> Optional[str]:
    """Return the proxy assigned to a request number, rotating every configured interval."""
    global _MISSING_PROXY_CONFIG_LOGGED

    proxies = get_crawler_proxies()
    if not proxies:
        if not _MISSING_PROXY_CONFIG_LOGGED:
            LOGGER.info("CRAWLER_PROXIES is not configured; crawler IP rotation is disabled")
            _MISSING_PROXY_CONFIG_LOGGED = True
        return None

    switch_interval = max(1, get_int_env("CRAWLER_PROXY_SWITCH_INTERVAL", PROXY_SWITCH_INTERVAL))
    proxy_index = ((request_number - 1) // switch_interval) % len(proxies)
    if (request_number - 1) % switch_interval == 0:
        LOGGER.info(
            "Using crawler proxy %s/%s for requests %s-%s",
            proxy_index + 1,
            len(proxies),
            request_number,
            request_number + switch_interval - 1,
        )
    return proxies[proxy_index]
