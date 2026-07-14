import os
from typing import List, Optional

from src.jobs.ingestion.scrapper.constants import PROJECT_ROOT
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def load_env_file() -> None:
    """Load key-value pairs from the project .env file into the process environment."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_float_env(name: str, default: float) -> float:
    """Read a float environment variable, falling back when it is missing or invalid."""
    load_env_file()
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return float(raw_value)
    except ValueError:
        LOGGER.warning("Invalid %s=%r; using %.2f", name, raw_value, default)
        return default


def get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable, falling back when it is missing or invalid."""
    load_env_file()
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        LOGGER.warning("Invalid %s=%r; using %s", name, raw_value, default)
        return default


def get_ai_api_key() -> Optional[str]:
    """Load and return the API key used for AI selector generation."""
    load_env_file()
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key or api_key.strip() in {"CHANGE_ME", "deepseek_api_key"}:
        return None
    return api_key


def get_crawler_proxies() -> List[str]:
    """Load comma-separated crawler proxies from the environment."""
    load_env_file()
    raw_proxies = os.getenv("CRAWLER_PROXIES", "")
    return [proxy.strip() for proxy in raw_proxies.split(",") if proxy.strip()]


def get_bypass_html_server_url() -> Optional[str]:
    """Load the internal HTML fetch server endpoint used by scraper='bypass' sources."""
    load_env_file()
    server_url = os.getenv("BYPASS_HTML_SERVER_URL")
    return server_url.strip() if server_url and server_url.strip() else None
