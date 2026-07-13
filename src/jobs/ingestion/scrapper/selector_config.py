import json
from typing import Any, Dict, Iterable, List

from src.jobs.ingestion.scrapper.constants import SOURCE_CONFIG_OBJECT_KEY
from src.jobs.ingestion.scrapper.field_model import SCRAPER_FIELD_MODEL
from src.utils.minio_client import get_minio_client
from src.utils.logger import get_logger

LOGGER = get_logger(__name__)


def load_source_configs() -> List[Dict[str, Any]]:
    """Load persisted selector configs from the shared MinIO source config object."""
    try:
        raw_config = get_minio_client().read_text(SOURCE_CONFIG_OBJECT_KEY)
    except Exception as exc:
        LOGGER.warning("Cannot load source config from MinIO: %s", exc)
        return []

    if not raw_config:
        LOGGER.info("Source config object is not available in MinIO; start with empty config")
        return []

    try:
        parsed_config = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        LOGGER.warning("Source config JSON in MinIO is invalid: %s", exc)
        return []

    configs = parsed_config.get("source_configs") if isinstance(parsed_config, dict) else parsed_config
    if configs is None:
        LOGGER.warning("Source config JSON does not contain source_configs")
        return []
    if isinstance(configs, dict):
        return list(configs.values())
    if isinstance(configs, list):
        return configs

    LOGGER.warning("Source config JSON has unsupported shape: %s", type(configs).__name__)
    return []


def is_config_field_complete(config: Dict[str, Any], field: str) -> bool:
    """Check whether a selector config field satisfies required or nullable rules."""
    if field in SCRAPER_FIELD_MODEL.nullable_fields:
        return field in config
    return bool(config.get(field))


def is_source_config_complete(source_config: Dict[str, Any]) -> bool:
    """Return whether a source selector config contains all expected fields."""
    return all(is_config_field_complete(source_config, field) for field in SCRAPER_FIELD_MODEL.source_config_fields)


def get_missing_source_configs(
    sources: Iterable[Dict[str, Any]],
    source_configs: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Find sources that do not have a complete selector configuration."""
    config_by_source = {config.get("source_name"): config for config in source_configs}
    missing_sources = []

    for source in sources:
        source_name = source["source_name"]
        config = config_by_source.get(source_name)
        if not config or not is_source_config_complete(config):
            LOGGER.info("Source %s is missing selector config", source_name)
            missing_sources.append(source)

    return missing_sources


def normalize_ai_config(source_name: str, ai_config: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize an AI-generated selector config to the expected schema."""
    normalized = {field: ai_config.get(field) for field in SCRAPER_FIELD_MODEL.source_config_fields}
    normalized[SCRAPER_FIELD_MODEL.source_name] = source_name
    normalized[SCRAPER_FIELD_MODEL.prefix_job_url] = normalized.get(SCRAPER_FIELD_MODEL.prefix_job_url) or ""

    for field in SCRAPER_FIELD_MODEL.source_config_fields:
        if field not in normalized:
            normalized[field] = None if field in SCRAPER_FIELD_MODEL.nullable_fields else ""

    missing_required = [
        field for field in SCRAPER_FIELD_MODEL.source_config_fields if not is_config_field_complete(normalized, field)
    ]
    if missing_required:
        LOGGER.warning("AI config for source %s is missing required fields: %s", source_name, missing_required)
    return normalized


def write_source_configs(source_configs: List[Dict[str, Any]]) -> None:
    """Persist source selector configs to the shared MinIO source config object."""
    ordered_configs = sorted(source_configs, key=lambda config: config.get("source_name", ""))
    content = json.dumps({"source_configs": ordered_configs}, ensure_ascii=False, indent=2) + "\n"
    get_minio_client().write_text(SOURCE_CONFIG_OBJECT_KEY, content)
    LOGGER.info("Wrote %s source configs to MinIO object %s", len(ordered_configs), SOURCE_CONFIG_OBJECT_KEY)
