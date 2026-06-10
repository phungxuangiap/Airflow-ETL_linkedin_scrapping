import os
from urllib.parse import quote_plus


INGESTION_POSTGRES_HOST = os.getenv("INGESTION_POSTGRES_HOST", "postgres-ingestion")
INGESTION_POSTGRES_PORT = os.getenv("INGESTION_POSTGRES_PORT", "5432")
INGESTION_POSTGRES_DB = os.getenv("INGESTION_POSTGRES_DB", "ingestion")
INGESTION_POSTGRES_USER = os.getenv("INGESTION_POSTGRES_USER", "ingestion")
INGESTION_POSTGRES_PASSWORD = os.getenv("INGESTION_POSTGRES_PASSWORD", "ingestion123")


def get_ingestion_postgres_dsn() -> str:
    user = quote_plus(INGESTION_POSTGRES_USER)
    password = quote_plus(INGESTION_POSTGRES_PASSWORD)
    host = INGESTION_POSTGRES_HOST
    port = INGESTION_POSTGRES_PORT
    database = quote_plus(INGESTION_POSTGRES_DB)
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
