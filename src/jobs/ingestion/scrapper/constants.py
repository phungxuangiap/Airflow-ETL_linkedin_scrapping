import os
from pathlib import Path

from src.jobs.ingestion.scrapper.field_model import SCRAPER_FIELD_MODEL

REQUEST_TIMEOUT_SECONDS = 20
AI_REQUEST_TIMEOUT_SECONDS = float(os.getenv("AI_REQUEST_TIMEOUT_SECONDS", "90"))
AI_REQUEST_MAX_ATTEMPTS = int(os.getenv("AI_REQUEST_MAX_ATTEMPTS", "3"))
AI_REQUEST_RETRY_DELAY_SECONDS = float(os.getenv("AI_REQUEST_RETRY_DELAY_SECONDS", "5"))
REQUEST_DELAY_MIN_SECONDS = 2.0
REQUEST_DELAY_MAX_SECONDS = 7.0
PROXY_SWITCH_INTERVAL = 100
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}
AI_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/chat/completions")
AI_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/app"))
SOURCE_CONFIG_OBJECT_KEY = os.getenv("SOURCE_CONFIG_OBJECT_KEY", "configs/ingestion/source_config.json")

# Backward-compatible aliases for modules that still import the old names.
SOURCE_CONFIG_FIELDS = SCRAPER_FIELD_MODEL.source_config_fields
DETAIL_ENRICHMENT_FIELDS = SCRAPER_FIELD_MODEL.detail_enrichment_fields
NULLABLE_FIELDS = SCRAPER_FIELD_MODEL.nullable_fields
