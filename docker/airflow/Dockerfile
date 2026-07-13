FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Minimal dependencies for lightweight ingestion test DAGs that run in Airflow.
# Full ETL dependencies remain in the linkedin-etl container.
RUN pip install --no-cache-dir \
    minio==7.2.0 \
    requests==2.31.0 \
    beautifulsoup4==4.12.3

WORKDIR /opt/airflow
