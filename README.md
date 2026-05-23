# LinkedIn Jobs ETL Pipeline

A Dockerized ETL pipeline that generates LinkedIn jobs data, loads raw files to a Bronze object-storage layer, transforms the data into Silver Apache Iceberg tables, and builds Gold dimensional/fact tables for analytics.

## Architecture

```text
Generated LinkedIn API + crawler data
    ↓
Bronze: JSONL files in MinIO/S3
    ↓
Silver: cleaned/deduplicated Apache Iceberg tables
    ↓
Gold: dimensional + fact Apache Iceberg tables
    ↓
BI / analytics consumers
```

The project separates orchestration dependencies from ETL dependencies:

- Airflow runs in its own Docker image.
- ETL jobs run in `linkedin-etl:latest` through Airflow `DockerOperator`.
- MinIO provides S3-compatible storage.
- PostgreSQL stores Airflow metadata and the Iceberg SQL catalog metadata.

## Tech Stack

- **Orchestration**: Apache Airflow
- **Task runtime**: DockerOperator + `linkedin-etl:latest`
- **Processing**: Python, DuckDB, PyArrow
- **Lakehouse tables**: Apache Iceberg via PyIceberg
- **Object storage**: MinIO / S3-compatible storage
- **Metadata stores**: PostgreSQL
- **Deployment**: Docker Compose, GitHub Actions, EC2

## Project Structure

```text
.
├── .github/workflows/          # GitHub Actions deployment workflows
├── dags/                       # Airflow DAG definitions
├── data_generation/            # Mock API/crawler data generators
├── docker/
│   ├── airflow/                # Airflow compose + image
│   └── infrastructure/         # MinIO + Iceberg catalog Postgres
├── scripts/                    # Local/prod deploy and health scripts
├── src/
│   ├── configs/                # Runtime config
│   ├── constants/              # Table names and paths
│   ├── jobs/
│   │   ├── bronze/             # Extract and load raw JSONL files
│   │   ├── silver/             # Clean, deduplicate, write Silver Iceberg
│   │   └── gold/               # Build and write Gold Iceberg tables
│   ├── models/                 # Schemas
│   └── utils/                  # DuckDB, Iceberg, MinIO helpers
├── Dockerfile.etl              # ETL runtime image
├── main.py                     # ETL CLI entrypoint
└── requirements.txt
```

## Local Development

### 1. Build the ETL image

```bash
docker build -f Dockerfile.etl -t linkedin-etl:latest .
```

### 2. Start services

```bash
./scripts/local-up.sh
```

Or with Compose directly:

```bash
docker compose -f docker/infrastructure/docker-compose.yml up -d
docker compose -f docker/airflow/docker-compose.yml up -d
```

### 3. Access services

- Airflow UI: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- MinIO API: <http://localhost:9000>

Default local credentials are defined in the compose files.

## Running ETL Jobs Manually

The ETL image exposes `main.py` as the container entrypoint.

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer bronze \
  --load-date 2026-05-21 \
  --log-level INFO
```

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer silver \
  --load-date 2026-05-21 \
  --log-level INFO
```

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer gold \
  --step dimensions \
  --load-date 2026-05-21 \
  --log-level INFO
```

Gold steps:

- `dimensions`
- `fact`
- `load`
- `all`

## Airflow DAG

The Docker DAG is in:

```text
dags/linkedin_jobs_pipeline_docker_dag.py
```

The DAG tasks are:

```text
bronze_extract_and_load
    ↓
silver_transform_and_clean
    ↓
gold_build_dimensions
    ↓
gold_build_fact_table
    ↓
gold_load_star_schema
```

Each task runs `linkedin-etl:latest` through `DockerOperator`.

## Data Layout

### Bronze

Bronze data is written as JSONL files partitioned by `load_date`:

```text
s3://airflow-bucket/BRONZE/api_data/jobs/load_date=YYYY-MM-DD/*.jsonl
s3://airflow-bucket/BRONZE/crawler_data/linkedin/jobs/load_date=YYYY-MM-DD/*.jsonl
```

### Silver

Silver is written as Iceberg tables:

```text
SILVER.jobs
SILVER.companies
```

With the current default warehouse config, table files are stored under:

```text
s3://airflow-bucket/SILVER/jobs/
├── data/
└── metadata/

s3://airflow-bucket/SILVER/companies/
├── data/
└── metadata/
```

The Iceberg SQL catalog metadata is stored in PostgreSQL, while Iceberg table data and table metadata JSON/Avro files are stored in MinIO/S3.

### Gold

Gold tables are written as Iceberg tables:

```text
GOLD.dim_company
GOLD.dim_location
GOLD.dim_date
GOLD.dim_source
GOLD.dim_role
GOLD.dim_level
GOLD.dim_working_model
GOLD.dim_techstack
GOLD.fact_hiring
GOLD.bridge_tech_fact
```

Gold transformations use DuckDB as the SQL engine over Arrow tables loaded from PyIceberg. PyIceberg is used for catalog access and Iceberg writes.

## Important Runtime Notes

### DockerOperator socket access

Airflow containers need access to the host Docker socket:

```text
/var/run/docker.sock
```

The Airflow compose file mounts the socket and adds the Docker socket group:

```yaml
user: "${AIRFLOW_UID:-50000}:0"
group_add:
  - "${DOCKER_GID:-989}"
```

On EC2, `scripts/prod-deploy.sh` detects the socket group dynamically:

```bash
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
export DOCKER_GID
```

If `DockerOperator` fails with `PermissionError: [Errno 13] Permission denied`, recreate the Airflow containers after setting the correct `DOCKER_GID`.

### Docker network

Both compose stacks use the external network:

```text
data-pipeline-network
```

The production deploy script creates it automatically if missing.

## Production Deployment on EC2

Production deploy is handled by:

```text
.github/workflows/deploy-production.yml
scripts/prod-deploy.sh
scripts/health-check.sh
```

Required GitHub repository secrets:

```text
EC2_HOST                 # EC2 public IP or public DNS
EC2_USER                 # EC2 SSH user, usually ec2-user for Amazon Linux
EC2_SSH_PRIVATE_KEY      # Private key for SSH access
```

The workflow SSHs into EC2, syncs `/opt/linkedin-jobs-etl` with the GitHub `main` branch, and runs:

```bash
./scripts/prod-deploy.sh
```

The deploy script:

1. Installs Git/Docker/Compose when needed.
2. Builds `linkedin-etl:latest`.
3. Creates `data-pipeline-network` when missing.
4. Starts MinIO and PostgreSQL infrastructure.
5. Starts Airflow with recreated containers.
6. Runs health checks.

## Useful Commands

### Check containers

```bash
docker ps
```

### Check Airflow logs

```bash
docker logs airflow-scheduler --tail 200
docker logs airflow-webserver --tail 200
```

### Check task state

```bash
docker exec airflow-scheduler airflow tasks states-for-dag-run \
  linkedin_jobs_pipeline_docker_v08 \
  <run_id>
```

### Test one Airflow task

```bash
docker exec airflow-scheduler airflow tasks test \
  linkedin_jobs_pipeline_docker_v08 \
  bronze_extract_and_load \
  2026-05-21
```

### List MinIO objects

```bash
docker run --rm --network data-pipeline-network --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc ls -r local/airflow-bucket | head -200"
```

## Troubleshooting

### `docker: command not found`

Install and start Docker on EC2:

```bash
sudo dnf install -y docker
sudo systemctl enable docker
sudo systemctl start docker
```

The production deploy script performs this automatically.

### `network data-pipeline-network declared as external, but could not be found`

Create the network:

```bash
sudo docker network create data-pipeline-network
```

The production deploy script performs this automatically.

### DockerOperator `PermissionError: [Errno 13] Permission denied`

Check Airflow group membership and Docker socket group:

```bash
sudo docker exec airflow-scheduler id
sudo docker exec airflow-scheduler ls -l /var/run/docker.sock
```

The socket group ID must appear in the Airflow container's `groups=...` output.

### `No files found ... BRONZE/.../load_date=...`

Bronze and Silver must use the same `load_date`. The Bronze code writes to the DAG-provided load date, and Silver reads the same partition.

### `Cannot open file "SILVER.jobs/metadata/version-hint.text"`

Do not use DuckDB `iceberg_scan('SILVER.jobs')` with the SQL catalog table name. Load the table via PyIceberg, register the Arrow result in DuckDB, then query the registered view.

### `.env.prod` values with spaces

`prod-deploy.sh` parses `.env.prod` without shell-sourcing it, so values such as this are supported:

```env
_PIP_ADDITIONAL_REQUIREMENTS=-r /project_root/requirements.txt
```

## License

MIT License
