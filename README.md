# LinkedIn Jobs ETL Lakehouse Pipeline

Dự án này xây dựng một pipeline Data Engineering mô phỏng quy trình thu thập, xử lý và phục vụ dữ liệu tuyển dụng LinkedIn theo kiến trúc Lakehouse. Pipeline được orchestration bằng Apache Airflow, chạy từng bước ETL trong Docker container riêng, lưu dữ liệu raw trên MinIO/S3-compatible storage, quản lý bảng bằng Apache Iceberg, xử lý bằng Python/DuckDB/PyArrow và phục vụ truy vấn phân tích qua Trino.

Mục tiêu chính của dự án:

- Mô phỏng một hệ thống dữ liệu end-to-end từ ingestion đến analytics-ready layer.
- Áp dụng mô hình Medallion Architecture: Bronze → Silver → Gold.
- Tách biệt orchestration runtime và ETL runtime để dễ triển khai, debug và scale.
- Có DataOps workflow cho local development, health check, CI/CD và deployment lên EC2.
- Chuẩn bị dữ liệu dạng star schema phục vụ BI, dashboard hoặc phân tích thị trường tuyển dụng.

---

## 1. Tổng quan kiến trúc

```text
LinkedIn API mock data + LinkedIn crawler mock data
        |
        v
Bronze staging trên MinIO
        |
        v
Bronze raw zone trên MinIO/S3
        |
        v
Silver staging Iceberg tables
        |
        v
Silver curated Iceberg tables
        |
        v
Gold staging Iceberg tables
        |
        v
Gold star schema Iceberg tables
        |
        v
Trino / BI / Analytics consumers
```

Các thành phần chính:

| Thành phần | Vai trò |
| --- | --- |
| Apache Airflow | Orchestrate DAG chạy hằng ngày, retry, dependency và scheduling. |
| DockerOperator | Airflow gọi từng ETL step bằng container `linkedin-etl:latest`. |
| Python ETL | Sinh dữ liệu, clean, deduplicate, transform và load Iceberg. |
| DuckDB | SQL engine cục bộ cho transformation trên Arrow table. |
| PyArrow | Định dạng dữ liệu in-memory giữa các bước xử lý. |
| Apache Iceberg | Table format cho Silver/Gold, hỗ trợ metadata, schema và lakehouse table. |
| MinIO | Object storage tương thích S3 để lưu raw files, Iceberg data files và metadata. |
| PostgreSQL | Metadata database cho Airflow và Iceberg SQL/JDBC catalog. |
| Trino | Query engine đọc Iceberg table để phục vụ phân tích. |
| GitHub Actions | CI/CD deploy production lên EC2 khi push vào `main`. |
| EC2 + Docker Compose | Môi trường production/self-hosted chạy toàn bộ stack. |

---

## 2. Luồng dữ liệu Data Engineering

### 2.1 Data sources

Dự án hiện dùng dữ liệu mô phỏng từ hai nguồn:

- `data_generation/generate_api_data.py`: sinh dữ liệu job từ nguồn API.
- `data_generation/generate_scrapped_data.py`: sinh dữ liệu job từ nguồn crawler/scraper.

Hai nguồn này đại diện cho hai kiểu ingestion thường gặp trong hệ thống dữ liệu thực tế:

- API ingestion: dữ liệu có cấu trúc hơn, thường đến từ đối tác hoặc service nội bộ.
- Web scraping/crawling ingestion: dữ liệu nhiều nhiễu hơn, cần normalize và deduplicate kỹ hơn.

### 2.2 Bronze layer

Bronze chịu trách nhiệm extract dữ liệu nguồn và lưu nguyên bản vào object storage.

File chính:

```text
src/jobs/bronze/extract_and_load_bronze.py
```

Bronze được chia thành hai phase:

1. `process_to_staging`
   - Sinh dữ liệu API và crawler.
   - Ghi file tạm vào thư mục local `tmp/`.
   - Upload JSONL lên vùng staging trong MinIO.
   - Xóa file local sau khi upload thành công.

2. `promote`
   - Copy object từ `staging/bronze/...` sang `bronze/...`.
   - Chỉ promote sau khi staging hoàn tất.

Ví dụ path trên MinIO:

```text
s3://linkedin-jobs-prod/staging/bronze/api_data/jobs/load_date=YYYY-MM-DD/*.jsonl
s3://linkedin-jobs-prod/staging/bronze/crawler_data/linkedin/jobs/load_date=YYYY-MM-DD/*.jsonl

s3://linkedin-jobs-prod/bronze/api_data/jobs/load_date=YYYY-MM-DD/*.jsonl
s3://linkedin-jobs-prod/bronze/crawler_data/linkedin/jobs/load_date=YYYY-MM-DD/*.jsonl
```

Ý nghĩa Data Engineering:

- Lưu raw data theo `load_date` để hỗ trợ replay/backfill.
- Giữ dữ liệu gần nguồn nhất có thể ở Bronze.
- Dùng staging → promote để giảm rủi ro ghi dữ liệu lỗi trực tiếp vào production zone.

### 2.3 Silver layer

Silver chịu trách nhiệm chuẩn hóa dữ liệu, merge nhiều nguồn và loại bỏ trùng lặp.

File chính:

```text
src/jobs/silver/transform_and_load_silver.py
src/jobs/silver/clean_jobs.py
src/jobs/silver/deduplicate_jobs.py
src/jobs/silver/load_silver.py
```

Silver flow:

1. Đọc Bronze API jobs.
2. Đọc Bronze crawler jobs.
3. Clean từng nguồn riêng biệt.
4. Tách dữ liệu thành jobs và companies.
5. Merge hai nguồn.
6. Deduplicate jobs và companies.
7. Ghi vào staging Iceberg tables.
8. Promote staging tables sang Silver tables.

Silver tables:

```text
silver.jobs
silver.companies
```

Staging tables:

```text
staging.silver_jobs
staging.silver_companies
```

Các chỉ số được log trong step Silver:

- Số job/company từ API.
- Số job/company từ crawler.
- Số record sau merge.
- Số duplicate bị loại bỏ.
- Số record cuối cùng được load.

Ý nghĩa Data Engineering:

- Silver là curated layer, dữ liệu đã sạch hơn và có thể dùng cho downstream transformations.
- Deduplication giúp giải quyết bài toán cùng một job xuất hiện ở nhiều nguồn.
- Iceberg giúp quản lý table metadata, data file, schema và hỗ trợ truy vấn lakehouse.

### 2.4 Gold layer

Gold chịu trách nhiệm tạo mô hình phân tích dạng star schema.

File chính:

```text
src/jobs/gold/build_dimensions.py
src/jobs/gold/build_fact_table.py
src/jobs/gold/load_star_schema.py
```

Gold flow:

1. Build dimension tables từ Silver.
2. Ghi dimension tables vào staging.
3. Promote dimension tables sang Gold.
4. Build fact và bridge tables từ Silver.
5. Ghi fact/bridge tables vào staging.
6. Promote fact/bridge tables sang Gold.
7. Load/ensure star schema hoàn chỉnh.

Gold dimension tables:

```text
gold.dim_company
gold.dim_location
gold.dim_date
gold.dim_source
gold.dim_role
gold.dim_level
gold.dim_working_model
gold.dim_techstack
```

Gold fact/bridge tables:

```text
gold.fact_hiring
gold.bridge_tech_fact
```

Mô hình phân tích:

```text
                   dim_company
                        |
dim_date --- fact_hiring --- dim_source
                        |
                 dim_location
                        |
                   dim_role
                        |
                   dim_level
                        |
             dim_working_model
                        |
              bridge_tech_fact --- dim_techstack
```

Một số use case phân tích có thể thực hiện trên Gold:

- Xu hướng tuyển dụng theo ngày/tháng/quý.
- Công ty nào tuyển nhiều nhất.
- Nhu cầu tech stack theo thị trường.
- Tỷ lệ remote/hybrid/onsite.
- Phân bổ job theo role và seniority level.
- Nguồn dữ liệu nào đóng góp nhiều job hơn.

---

## 3. Airflow orchestration

DAG chính:

```text
dags/linkedin_jobs_pipeline_docker_dag.py
```

DAG ID:

```text
linkedin_jobs_pipeline_docker_v08
```

Lịch chạy:

```text
0 2 * * *
```

Tức là chạy hằng ngày lúc 02:00.

Task dependency:

```text
bronze_process_to_staging
    -> bronze_promote_staging_to_bronze
    -> silver_process_to_staging
    -> silver_promote_staging_to_silver
    -> gold_dimensions_process_to_staging
    -> gold_dimensions_promote_to_gold
    -> gold_fact_process_to_staging
    -> gold_fact_promote_to_gold
    -> gold_load_star_schema
    -> clean_staging
```

Airflow không chạy logic ETL trực tiếp trong process của scheduler/worker. Thay vào đó, mỗi task dùng `DockerOperator` để chạy image:

```text
linkedin-etl:latest
```

Cách tách này giúp:

- Tránh conflict dependencies giữa Airflow và ETL libraries.
- Rebuild ETL image độc lập khi code xử lý thay đổi.
- Dễ debug từng layer bằng `docker run`.
- Giảm coupling giữa orchestration và transformation runtime.

Một số cấu hình DAG đáng chú ý:

```text
owner: data-engineering
retries: 2
retry_delay: 5 minutes
execution_timeout: 30 minutes
catchup: false
max_active_runs: 1
```

---

## 4. Lakehouse, Iceberg và object storage

### 4.1 MinIO/S3 layout

MinIO đóng vai trò object storage tương thích S3.

Các bucket được khởi tạo bởi `minio-init`:

```text
linkedin-jobs-prod
iceberg-warehouse
```

`linkedin-jobs-prod` lưu dữ liệu pipeline như Bronze, Silver, Gold hoặc warehouse path tùy cấu hình runtime.

`iceberg-warehouse` được dùng bởi Trino catalog config làm default warehouse dir.

### 4.2 Iceberg catalog

Dự án có PostgreSQL riêng cho Iceberg catalog metadata:

```text
container: postgres-iceberg
database: iceberg_catalog
user: iceberg
port host: 5433
port container: 5432
```

Trong ETL runtime, catalog được cấu hình qua environment variables như:

```text
ICEBERG_CATALOG_TYPE=sql
ICEBERG_CATALOG_URI=postgresql://iceberg:iceberg123@postgres-iceberg:5432/iceberg_catalog
ICEBERG_CATALOG_NAME=lakehouse
ICEBERG_WAREHOUSE_PATH=s3://linkedin-jobs-prod/warehouse
```

Trong Trino, catalog `iceberg` được cấu hình tại:

```text
trino/etc/catalog/iceberg.properties
```

Với JDBC catalog:

```text
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=lakehouse
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://postgres-iceberg:5432/iceberg_catalog
iceberg.jdbc-catalog.default-warehouse-dir=s3://iceberg-warehouse
```

### 4.3 Trino analytics

Trino chạy tại:

```text
http://localhost:8081
```

Trino đọc Iceberg tables thông qua catalog `iceberg`, kết nối PostgreSQL catalog và MinIO object storage.

Ví dụ truy vấn sau khi pipeline chạy:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.gold;

SELECT *
FROM iceberg.gold.fact_hiring
LIMIT 10;

SELECT
  d.year,
  d.month,
  COUNT(*) AS total_jobs
FROM iceberg.gold.fact_hiring f
JOIN iceberg.gold.dim_date d
  ON f.date_id = d.id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## 5. Cấu trúc thư mục

```text
.
├── .github/workflows/
│   └── deploy-production.yml          # GitHub Actions deploy production lên EC2
├── dags/
│   └── linkedin_jobs_pipeline_docker_dag.py
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/                          # Local placeholder directories
├── data_generation/
│   ├── generate_api_data.py            # Mock API source
│   └── generate_scrapped_data.py       # Mock crawler source
├── docker/
│   ├── airflow/
│   │   ├── docker-compose.yml          # Airflow webserver/scheduler/worker/postgres/redis
│   │   └── Dockerfile                  # Airflow image
│   └── infrastructure/
│       └── docker-compose.yml          # MinIO + PostgreSQL Iceberg catalog
├── scripts/
│   ├── local-up.sh                     # Start local stack
│   ├── local-down.sh                   # Stop local stack
│   ├── prod-deploy.sh                  # Production deployment script on EC2
│   ├── prod-restart.sh
│   ├── prod-down.sh
│   ├── rebuild-etl.sh
│   └── health-check.sh
├── src/
│   ├── configs/                        # Settings for Airflow, DuckDB, Iceberg, MinIO
│   ├── constants/                      # Field names, table names, paths
│   ├── jobs/
│   │   ├── bronze/                     # Extract and load Bronze
│   │   ├── silver/                     # Clean, deduplicate, load Silver
│   │   ├── gold/                       # Build star schema Gold
│   │   └── staging/                    # Staging utilities and cleanup
│   ├── models/                         # Schemas
│   └── utils/                          # Clients, logger, retry, validation
├── trino/
│   ├── docker-compose.yml
│   └── etc/catalog/iceberg.properties
├── Dockerfile.etl                      # ETL runtime image
├── main.py                             # CLI entrypoint cho ETL container
├── Makefile                            # Local/DataOps commands
├── requirements.txt
└── README.md
```

---

## 6. Tech stack

### Data Engineering

- Python
- PyArrow
- DuckDB
- Apache Iceberg / PyIceberg
- MinIO / S3-compatible object storage
- PostgreSQL catalog metadata
- Trino SQL query engine

### Orchestration

- Apache Airflow 2.10.3
- DockerOperator
- CeleryExecutor
- Redis broker
- PostgreSQL Airflow metadata DB

### DataOps / Platform

- Docker
- Docker Compose
- Makefile automation
- Shell scripts for local/prod operations
- Health check scripts
- GitHub Actions
- EC2 deployment

---

## 7. Cài đặt local

### 7.1 Yêu cầu

- Docker
- Docker Compose v2 hoặc `docker-compose`
- Make
- Bash shell
- Port local trống:
  - `8080`: Airflow UI
  - `8081`: Trino
  - `9000`: MinIO API
  - `9001`: MinIO Console
  - `5433`: PostgreSQL Iceberg catalog

### 7.2 Chuẩn bị environment

Copy file mẫu:

```bash
cp .env.example .env.local
```

Các biến quan trọng:

```text
ENV=local
AIRFLOW_UID=50000
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
BUCKET=linkedin-jobs-prod
ICEBERG_CATALOG_NAME=lakehouse
```

### 7.3 Build ETL image

```bash
docker build -f Dockerfile.etl -t linkedin-etl:latest .
```

Hoặc:

```bash
make rebuild-etl
```

### 7.4 Start toàn bộ stack

```bash
make up
```

Hoặc chạy trực tiếp:

```bash
./scripts/local-up.sh
```

Các service chính:

| Service | URL |
| --- | --- |
| Airflow UI | http://localhost:8080 |
| Trino | http://localhost:8081 |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |

Thông tin đăng nhập local mặc định thường nằm trong compose/env:

```text
Airflow: airflow / airflow
MinIO: minioadmin / minioadmin
```

### 7.5 Kiểm tra health

```bash
make health-check
```

Script kiểm tra:

- MinIO live endpoint.
- PostgreSQL Iceberg catalog bằng `pg_isready`.
- Trino `/v1/info`.
- Airflow `/health`.
- PostgreSQL Airflow metadata bằng `pg_isready`.

---

## 8. Chạy pipeline

### 8.1 Chạy qua Airflow UI

1. Mở Airflow UI tại `http://localhost:8080`.
2. Tìm DAG `linkedin_jobs_pipeline_docker_v08`.
3. Unpause DAG nếu đang paused.
4. Trigger DAG manual hoặc đợi lịch chạy hằng ngày.

### 8.2 Trigger bằng CLI

Nếu có script hỗ trợ trong môi trường hiện tại:

```bash
make trigger
```

Hoặc dùng Airflow CLI trong container tùy compose setup.

### 8.3 Chạy từng layer thủ công bằng Docker

Bronze:

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer bronze \
  --step all \
  --load-date 2026-05-21 \
  --log-level INFO
```

Silver:

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer silver \
  --step all \
  --load-date 2026-05-21 \
  --log-level INFO
```

Gold dimensions:

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer gold \
  --step dimensions \
  --load-date 2026-05-21 \
  --log-level INFO
```

Gold fact:

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer gold \
  --step fact \
  --load-date 2026-05-21 \
  --log-level INFO
```

Clean staging:

```bash
docker run --rm --network data-pipeline-network linkedin-etl:latest \
  --layer staging \
  --step clean \
  --load-date 2026-05-21 \
  --log-level INFO
```

Các `--layer` hợp lệ:

```text
bronze
silver
gold
staging
```

Các `--step` hợp lệ:

```text
process_to_staging
promote
dimensions
dimensions_to_staging
promote_dimensions
fact
fact_to_staging
promote_fact
load
clean
all
```

---

## 9. DataOps workflow

### 9.1 Makefile commands

```bash
make help
```

Các lệnh thường dùng:

| Lệnh | Mục đích |
| --- | --- |
| `make up` | Start infrastructure + Airflow. |
| `make down` | Stop toàn bộ stack. |
| `make restart` | Restart stack. |
| `make rebuild-etl` | Rebuild image `linkedin-etl:latest`. |
| `make health-check` | Kiểm tra trạng thái service. |
| `make logs` | Xem log gần nhất của infrastructure và Airflow. |
| `make ps` | Xem container status. |
| `make prod-deploy` | Chạy deploy production từ server. |
| `make prod-restart` | Restart production services. |
| `make prod-down` | Stop production services. |
| `make clean` | Xóa volumes/data tạm local. |

Lưu ý: `make clean` có thể xóa dữ liệu local và Docker volumes, chỉ dùng khi muốn reset môi trường local.

### 9.2 Staging → promote pattern

Pipeline dùng pattern staging trước khi promote ở nhiều layer:

```text
process_to_staging -> promote
```

Lợi ích:

- Giảm rủi ro ghi dữ liệu lỗi vào curated/serving zone.
- Dễ tách validation trước khi promote.
- Có thể mở rộng thành atomic publish pattern trong môi trường production.
- Dễ retry từng phase hơn thay vì chạy lại toàn bộ pipeline.

### 9.3 Logging và observability

ETL entrypoint `main.py` log:

- Layer đang chạy.
- Step đang chạy.
- `load_date`.
- Số lượng record input/output.
- Thống kê duplicate.
- Kết quả staging/promotion.
- Exception stack trace khi thất bại.

Airflow cung cấp:

- Task logs.
- Retry history.
- DAG run status.
- Duration từng task.
- Manual trigger/backfill theo execution date.

Health check script cung cấp kiểm tra nhanh sau deploy hoặc khi debug local.

### 9.4 Idempotency và backfill

Pipeline nhận tham số:

```text
--load-date YYYY-MM-DD
```

Điều này giúp:

- Chạy lại dữ liệu theo ngày.
- Debug một partition cụ thể.
- Backfill lịch sử.
- Tách dữ liệu theo batch date.

Bronze lưu object theo `load_date`, còn Silver/Gold được build từ dữ liệu Bronze/Silver tương ứng.

---

## 10. Cloud và production deployment

### 10.1 Mô hình production

Production hiện được thiết kế để chạy self-hosted trên EC2 bằng Docker Compose.

```text
GitHub main branch
        |
        v
GitHub Actions
        |
        v
SSH vào EC2
        |
        v
Sync /opt/linkedin-jobs-etl
        |
        v
Build ETL image
        |
        v
Start infrastructure + Trino + Airflow
        |
        v
Run health check
```

Workflow deploy:

```text
.github/workflows/deploy-production.yml
```

Script chạy trên EC2:

```text
scripts/prod-deploy.sh
```

### 10.2 GitHub Actions

Workflow trigger khi:

```text
push vào main
workflow_dispatch manual
```

GitHub repository secrets cần có:

```text
EC2_HOST
EC2_USER
EC2_SSH_PRIVATE_KEY
```

Deploy workflow thực hiện:

1. Checkout code.
2. Setup SSH agent.
3. Add EC2 vào known hosts.
4. SSH vào EC2.
5. Clone repo nếu chưa có ở `/opt/linkedin-jobs-etl`.
6. Nếu đã có repo, fetch và reset về `origin/main`.
7. Chạy `scripts/prod-deploy.sh`.
8. Verify bằng `scripts/health-check.sh`.

### 10.3 Production deploy script

`scripts/prod-deploy.sh` thực hiện:

1. Load `.env.prod`.
2. Cài Docker/Docker Compose nếu thiếu.
3. Detect Docker socket group id:

```bash
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
```

4. Build ETL image:

```bash
docker build -f Dockerfile.etl -t linkedin-etl:latest .
```

5. Tạo Docker network nếu chưa có:

```text
data-pipeline-network
```

6. Stop stack cũ.
7. Pull base images.
8. Start infrastructure:
   - MinIO
   - PostgreSQL Iceberg catalog
9. Start Trino.
10. Start Airflow.
11. Kiểm tra Docker socket access trong Airflow scheduler.
12. Chạy health check.

### 10.4 Docker network

Các compose stack dùng chung external network:

```text
data-pipeline-network
```

Network này cho phép:

- Airflow task container gọi `minio:9000`.
- ETL container gọi `postgres-iceberg:5432`.
- Trino kết nối MinIO và PostgreSQL catalog.
- Airflow DockerOperator launch ETL container cùng network.

### 10.5 Docker socket cho Airflow DockerOperator

Airflow cần truy cập Docker socket của host:

```text
/var/run/docker.sock
```

Compose Airflow mount socket và set group phù hợp. Nếu trên EC2 gặp lỗi:

```text
PermissionError: [Errno 13] Permission denied: '/var/run/docker.sock'
```

Cách xử lý:

1. Kiểm tra Docker socket group id trên host:

```bash
stat -c '%g' /var/run/docker.sock
```

2. Export `DOCKER_GID` đúng giá trị.
3. Recreate Airflow containers.

Production script đã tự động detect `DOCKER_GID` và truyền vào Docker Compose.

---

## 11. Security và secrets

Dự án có các file env mẫu và env theo môi trường:

```text
.env.example
.env.local
.env.prod
```

Nguyên tắc vận hành:

- Không commit credentials production thật.
- GitHub Actions dùng repository secrets cho SSH credentials.
- Với real LinkedIn scraper, `USER_NAME` và `PASSWORD` phải được quản lý bằng secret store hoặc environment variables an toàn.
- Không hardcode API keys như `GEMINI_API_KEY` trong code.
- Với production thật, nên thay default credentials của MinIO/PostgreSQL/Airflow.
- Nên giới hạn inbound security group trên EC2, đặc biệt các port Airflow, MinIO, Trino và PostgreSQL.

Các credential local mặc định chỉ phù hợp cho demo/local development:

```text
minioadmin / minioadmin
airflow / airflow
iceberg / iceberg123
```

---

## 12. Troubleshooting

### 12.1 DockerOperator không chạy được container

Triệu chứng:

```text
PermissionError: /var/run/docker.sock
```

Nguyên nhân thường gặp:

- Airflow container không có quyền truy cập Docker socket.
- `DOCKER_GID` không khớp với group id của socket trên host.

Cách xử lý:

```bash
stat -c '%g' /var/run/docker.sock
export DOCKER_GID=<gid>
docker compose -f docker/airflow/docker-compose.yml up -d --force-recreate
```

### 12.2 ETL container không kết nối được MinIO/PostgreSQL

Kiểm tra container có chạy trong network đúng không:

```bash
docker network inspect data-pipeline-network
```

Các hostname nội bộ cần resolve được:

```text
minio
postgres-iceberg
```

### 12.3 Trino không thấy Iceberg tables

Kiểm tra:

- `trino/etc/catalog/iceberg.properties` đúng thông tin PostgreSQL và MinIO.
- PostgreSQL Iceberg catalog đang chạy.
- MinIO bucket tồn tại.
- ETL đã tạo tables thành công.
- Warehouse path giữa ETL và Trino có nhất quán với catalog đang dùng.

### 12.4 Airflow UI không lên

Chạy:

```bash
make ps
make logs
make health-check
```

Kiểm tra port `8080` có bị service khác chiếm không.

### 12.5 Muốn reset local environment

```bash
make down
make clean
make up
```

Lưu ý: `make clean` xóa volumes và dữ liệu local, không dùng trên production nếu không chắc chắn.

---

