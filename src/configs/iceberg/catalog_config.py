import os
from pyiceberg.catalog import load_catalog

# Path tới file SQLite cục bộ
CATALOG_DB_PATH = os.path.abspath("linkedin_catalog.db")

# S3/MinIO Configuration from environment variables
bucket = os.getenv("AWS_S3_BUCKET", "airflow-bucket")
s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
target_s3_endpoint = s3_endpoint

catalog_config = {
    "type": "rest",
    "uri": "http://iceberg-rest:8181",
    "warehouse": f"s3://{bucket}/",
    "s3.endpoint": target_s3_endpoint,
    "s3.access-key-id": s3_access_key,
    "s3.secret-access-key": s3_secret_key,
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    "header.X-Iceberg-Access-Key": s3_access_key,
    "header.X-Iceberg-Secret-Key": s3_secret_key,
}