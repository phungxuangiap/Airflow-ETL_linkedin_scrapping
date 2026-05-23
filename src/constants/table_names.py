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

# Staging layer tables
STAGING_SILVER_JOBS = "STAGING.silver_jobs"
STAGING_SILVER_COMPANIES = "STAGING.silver_companies"
STAGING_GOLD_DIM_COMPANY = "STAGING.gold_dim_company"
STAGING_GOLD_DIM_LOCATION = "STAGING.gold_dim_location"
STAGING_GOLD_DIM_DATE = "STAGING.gold_dim_date"
STAGING_GOLD_DIM_SOURCE = "STAGING.gold_dim_source"
STAGING_GOLD_DIM_ROLE = "STAGING.gold_dim_role"
STAGING_GOLD_DIM_LEVEL = "STAGING.gold_dim_level"
STAGING_GOLD_DIM_WORKING_MODEL = "STAGING.gold_dim_working_model"
STAGING_GOLD_DIM_TECHSTACK = "STAGING.gold_dim_techstack"
STAGING_GOLD_FACT_HIRING = "STAGING.gold_fact_hiring"
STAGING_GOLD_BRIDGE_TECH_FACT = "STAGING.gold_bridge_tech_fact"

# Gold layer tables - Legacy (deprecated)
GOLD_JOB_ANALYTICS = "GOLD.job_analytics"
GOLD_COMPANY_ANALYTICS = "GOLD.company_analytics"
GOLD_TECH_TRENDS = "GOLD.tech_trends"
GOLD_SALARY_INSIGHTS = "GOLD.salary_insights"
