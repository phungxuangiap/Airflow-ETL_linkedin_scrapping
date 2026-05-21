# LinkedIn Jobs ETL Pipeline

A production-ready ETL pipeline for scraping LinkedIn job postings, transforming data through Bronze → Silver → Gold layers, and serving analytics to Power BI.

## Architecture

```
Data Sources (LinkedIn)
    ↓
Bronze Layer (Raw Data - MinIO/S3)
    ↓
Silver Layer (Cleaned & Standardized - Iceberg)
    ↓
Gold Layer (Analytics & Aggregations - Iceberg)
    ↓
Power BI Reports
```

## Tech Stack

- **Orchestration**: Apache Airflow
- **Data Processing**: DuckDB, PyArrow
- **Lakehouse**: Apache Iceberg
- **Storage**: MinIO (S3-compatible)
- **Containerization**: Docker, Docker Compose

## Project Structure

```
linkedin-jobs-etl/
├── .github/workflows/          # CI/CD pipelines
├── dags/                       # Airflow DAGs
├── src/
│   ├── jobs/                   # ETL job modules
│   │   ├── bronze/            # Data extraction & loading
│   │   ├── silver/            # Cleaning & transformation
│   │   └── gold/              # Aggregation & analytics
│   ├── configs/               # Configuration modules
│   ├── utils/                 # Utility functions
│   ├── models/                # Data models & schemas
│   └── constants/             # Constants & mappings
├── scripts/                   # Deployment & utility scripts
├── docker/                    # Docker configurations
├── config/                    # Service configurations
└── data_generation/           # Mock data generators
```

## Quick Start

### Local Development

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd linkedin-jobs-etl
```

2. **Configure environment**
```bash
cp .env.example .env.local
# Edit .env.local with your settings
```

3. **Start services**
```bash
make local-up
# or
./scripts/local-up.sh
```

4. **Access services**
- Airflow UI: http://localhost:8080 (airflow/airflow)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Iceberg REST: http://localhost:8181

### Production Deployment

1. **Setup EC2 instance**
```bash
# SSH into EC2
ssh ec2-user@your-ec2-ip

# Install Docker & Docker Compose
sudo yum update -y
sudo yum install -y docker
sudo systemctl start docker
sudo usermod -aG docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

2. **Clone and configure**
```bash
sudo mkdir -p /opt/linkedin-jobs-etl
sudo chown ec2-user:ec2-user /opt/linkedin-jobs-etl
cd /opt/linkedin-jobs-etl
git clone <your-repo-url> .

cp .env.example .env.prod
# Edit .env.prod with production settings
```

3. **Deploy**
```bash
make prod-deploy
```

4. **Setup GitHub Actions**
- Add secrets to GitHub repository:
  - `EC2_SSH_PRIVATE_KEY`: Your EC2 SSH private key
  - `EC2_HOST`: EC2 instance IP/hostname
  - `EC2_USER`: SSH user (e.g., ec2-user)

## Data Pipeline

### Bronze Layer
- Extracts raw data from LinkedIn (API & scraping)
- Stores in MinIO as JSONL files
- Partitioned by `load_date`

### Silver Layer
- Cleans and standardizes data
- Deduplicates records
- Stores in Iceberg tables
- Partitioned by `processed_at` (jobs) and `source_name` (companies)

### Gold Layer
- Aggregates data for analytics
- Builds reporting tables for Power BI
- Includes:
  - Job analytics by company, title, location
  - Technology trends
  - Salary insights

## DAG Schedule

- **Bronze**: Daily @ midnight
- **Silver**: Triggered after Bronze completion
- **Gold**: Triggered after Silver completion

## Development

### Available Commands
```bash
make help              # Show all available commands
make local-up          # Start local environment
make local-down        # Stop local environment
make health-check      # Run health checks
make logs              # View service logs
make ps                # Show service status
make clean             # Clean up data and volumes
```

## Monitoring

### Health Checks
```bash
make health-check
```

### View Logs
```bash
make logs
# or specific service
docker-compose logs -f airflow-webserver
```

### Service Status
```bash
make ps
```

## Troubleshooting

### Services not starting
```bash
# Check logs
docker-compose logs

# Restart services
make prod-restart
```

### Airflow DAG not appearing
- Check DAG file syntax
- Verify imports are correct
- Check Airflow logs: `docker-compose logs airflow-scheduler`

### MinIO connection issues
- Verify S3_ENDPOINT in .env file
- Check MinIO is running: `docker-compose ps minio`
- Test connection: `curl http://localhost:9000/minio/health/live`

## Power BI Integration

1. Install Power BI Desktop
2. Connect to Iceberg tables via:
   - Direct Query to PostgreSQL (metadata)
   - Import from Parquet files in MinIO
3. Use Gold layer tables for reporting

## Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create pull request

## License

MIT License
