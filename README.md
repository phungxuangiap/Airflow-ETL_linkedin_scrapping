# AI-Powered LinkedIn Job Intelligence Lakehouse

## 🧠 Project Description
This project builds a fully automated data engineering pipeline to collect, process, and analyze job market data from platforms such as LinkedIn, Glassdoor, and others. It addresses the limitations of traditional scraping approaches by leveraging Large Language Models (LLMs) to intelligently extract and standardize information from unstructured HTML sources.

Instead of relying on fragile CSS selectors or hardcoded parsing logic, the system uses AI-driven agents to understand and transform raw web content into structured, analytics-ready datasets.

---

## 🎯 Objectives
- Automate job data collection across multiple platforms
- Eliminate dependency on brittle scraping techniques
- Standardize heterogeneous job data into a unified schema
- Enable scalable analytics and insights on job market trends
- Integrate AI as a core component in the data processing pipeline

---

## 🏗️ System Architecture (Medallion Architecture)

### 🔹 Bronze Layer (Raw Data)
- Stores raw HTML content and metadata from scraping jobs
- Data is ingested without transformation
- Storage: MinIO (object storage)
- Purpose: Preserve original data for traceability and reprocessing

### 🔹 Silver Layer (Cleaned & Structured)
- LLM agents process raw HTML and extract structured information
- Data is converted into JSON or Parquet format
- Schema is standardized across all sources
- Storage: Apache Iceberg
- Purpose: Provide clean, queryable datasets

### 🔹 Gold Layer (Curated & Analytics)
- Refined datasets optimized for analytics and reporting
- Aggregations and business logic applied
- Query engines: Trino / DuckDB
- Purpose: Support dashboards, BI tools, and market analysis

---

## 🤖 AI Processing Workflow

### 1. HTML Understanding & Extraction
- Raw HTML is passed to LLMs
- Extracts key fields such as:
  - job_title
  - company_name
  - job_location
  - salary_range
  - technical_requirements

### 2. Semantic Schema Mapping
- Aligns different field names across platforms
- Example:
  - "Location" → job_location
  - "Workplace" → job_location
- Ensures a unified schema regardless of source

### 3. Data Validation (Quality Gate)
- AI verifies logical consistency:
  - Salary vs experience level
  - Missing or abnormal values
- Filters or flags low-quality records

---

## 🛠️ Technology Stack

### Orchestration
- Apache Airflow

### Data Storage & Lakehouse
- MinIO (object storage)
- Apache Iceberg (table format)

### Data Processing
- PySpark
- DuckDB

### Infrastructure
- Terraform (Infrastructure as Code)
- Docker (Debian 13 environment)

### AI Integration
- OpenAI API / MiMo
- LangChain or custom AI agent framework

---

## ⚙️ Pipeline Flow

1. Scraper collects raw HTML from job platforms
2. Data is stored in Bronze layer (MinIO)
3. Airflow triggers LLM processing tasks
4. LLM extracts and standardizes data
5. Structured data is written to Iceberg tables (Silver)
6. Aggregations and analytics transformations create Gold datasets
7. Data is queried via Trino/DuckDB for insights

---
