import os


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-in-production")

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.getenv('SUPERSET_DB_USER', 'superset')}:"
    f"{os.getenv('SUPERSET_DB_PASSWORD', 'superset_password')}@"
    f"{os.getenv('SUPERSET_DB_HOST', 'superset-db')}:"
    f"{os.getenv('SUPERSET_DB_PORT', '5432')}/"
    f"{os.getenv('SUPERSET_DB_NAME', 'superset')}"
)

REDIS_HOST = os.getenv("SUPERSET_REDIS_HOST", "superset-redis")
REDIS_PORT = os.getenv("SUPERSET_REDIS_PORT", "6379")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"


CELERY_CONFIG = CeleryConfig
