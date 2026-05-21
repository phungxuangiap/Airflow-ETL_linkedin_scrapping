FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ETL dependencies are now in linkedin-etl container (requirements-etl.txt)
# Airflow only needs base image + docker provider (already included)

WORKDIR /opt/airflow
