"""
Table name constants for Iceberg tables
"""

# Bronze layer tables
BRONZE_API_JOBS = "bronze.api_jobs"
BRONZE_SCRAPPED_JOBS = "bronze.scrapped_jobs"

# Silver layer tables
SILVER_JOBS = "silver.jobs"
SILVER_COMPANIES = "silver.companies"

# Gold layer tables - Star Schema
GOLD_DIM_COMPANY = "gold.dim_company"
GOLD_DIM_LOCATION = "gold.dim_location"
GOLD_DIM_DATE = "gold.dim_date"
GOLD_DIM_SOURCE = "gold.dim_source"
GOLD_DIM_ROLE = "gold.dim_role"
GOLD_DIM_LEVEL = "gold.dim_level"
GOLD_DIM_WORKING_MODEL = "gold.dim_working_model"
GOLD_DIM_TECHSTACK = "gold.dim_techstack"
GOLD_FACT_HIRING = "gold.fact_hiring"
GOLD_BRIDGE_TECH_FACT = "gold.bridge_tech_fact"

# Staging layer tables
STAGING_SILVER_JOBS = "staging.silver_jobs"
STAGING_SILVER_COMPANIES = "staging.silver_companies"
STAGING_GOLD_DIM_COMPANY = "staging.gold_dim_company"
STAGING_GOLD_DIM_LOCATION = "staging.gold_dim_location"
STAGING_GOLD_DIM_DATE = "staging.gold_dim_date"
STAGING_GOLD_DIM_SOURCE = "staging.gold_dim_source"
STAGING_GOLD_DIM_ROLE = "staging.gold_dim_role"
STAGING_GOLD_DIM_LEVEL = "staging.gold_dim_level"
STAGING_GOLD_DIM_WORKING_MODEL = "staging.gold_dim_working_model"
STAGING_GOLD_DIM_TECHSTACK = "staging.gold_dim_techstack"
STAGING_GOLD_FACT_HIRING = "staging.gold_fact_hiring"
STAGING_GOLD_BRIDGE_TECH_FACT = "staging.gold_bridge_tech_fact"

# Gold layer tables - Legacy (deprecated)
GOLD_JOB_ANALYTICS = "gold.job_analytics"
GOLD_COMPANY_ANALYTICS = "gold.company_analytics"
GOLD_TECH_TRENDS = "gold.tech_trends"
GOLD_SALARY_INSIGHTS = "gold.salary_insights"
