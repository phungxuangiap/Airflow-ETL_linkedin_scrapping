import os
from pyiceberg.catalog import load_catalog

# Path tới file SQLite cục bộ
CATALOG_DB_PATH = os.path.abspath("linkedin_catalog.db")

# Khởi tạo Catalog JDBC
catalog = load_catalog(
    "linkedin_warehouse",
    **{
        "type": "jdbc",
        "uri": f"sqlite:///{CATALOG_DB_PATH}",
        "warehouse": "s3://airflow_bucket/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
    }
)