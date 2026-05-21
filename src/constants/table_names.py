"""
Table name constants for Iceberg tables
"""

# Bronze layer tables
BRONZE_API_JOBS = "BRONZE.api_jobs"
BRONZE_SCRAPPED_JOBS = "BRONZE.scrapped_jobs"

# Silver layer tables
SILVER_JOBS = "SILVER.jobs"
SILVER_COMPANIES = "SILVER.companies"

# Gold layer tables - Star Schema
GOLD_DIM_COMPANY = "GOLD.dim_company"
GOLD_DIM_LOCATION = "GOLD.dim_location"
GOLD_DIM_DATE = "GOLD.dim_date"
GOLD_DIM_SOURCE = "GOLD.dim_source"
GOLD_DIM_ROLE = "GOLD.dim_role"
GOLD_DIM_LEVEL = "GOLD.dim_level"
GOLD_DIM_WORKING_MODEL = "GOLD.dim_working_model"
GOLD_DIM_TECHSTACK = "GOLD.dim_techstack"
GOLD_FACT_HIRING = "GOLD.fact_hiring"
GOLD_BRIDGE_TECH_FACT = "GOLD.bridge_tech_fact"

# Gold layer tables - Legacy (deprecated)
GOLD_JOB_ANALYTICS = "GOLD.job_analytics"
GOLD_COMPANY_ANALYTICS = "GOLD.company_analytics"
GOLD_TECH_TRENDS = "GOLD.tech_trends"
GOLD_SALARY_INSIGHTS = "GOLD.salary_insights"
