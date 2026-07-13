from typing import Any, List, Optional

from src.utils.logger import get_logger

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - runtime dependency guard
    BeautifulSoup = None

LOGGER = get_logger(__name__)


def require_html_parser() -> None:
    """Ensure BeautifulSoup is available before HTML parsing is attempted."""
    if BeautifulSoup is None:
        raise RuntimeError("BeautifulSoup is required for HTML parsing. Install beautifulsoup4 to run scrapper.py")


def select_elements(html: str, selector: Optional[str]) -> List[Any]:
    """Select HTML elements matching a CSS selector from a raw HTML string."""
    require_html_parser()
    if not html or not selector:
        return []
    return BeautifulSoup(html, "html.parser").select(selector)


def extract_value_by_selector(job_html: str, selector_config: Optional[str]) -> Optional[str]:
    """Extract text or an attribute from job HTML using the configured selector syntax."""
    if not selector_config:
        return None

    require_html_parser()
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
