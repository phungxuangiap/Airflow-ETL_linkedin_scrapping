"""
Schema definitions for Iceberg tables
"""
import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform


# Bronze layer schemas
BRONZE_API_JOBS_SCHEMA = pa.schema([
    pa.field("id", pa.string()),
    pa.field("date_posted", pa.string()),
    pa.field("date_created", pa.string()),
    pa.field("title", pa.string()),
    pa.field("organization", pa.string()),
    pa.field("organization_url", pa.string()),
    pa.field("date_validthrough", pa.string()),
    pa.field("location_type", pa.string()),
    pa.field("employment_type", pa.list_(pa.string())),
    pa.field("url", pa.string()),
    pa.field("source_type", pa.string()),
    pa.field("source", pa.string()),
    pa.field("source_domain", pa.string()),
    pa.field("remote_derived", pa.bool_()),
    pa.field("seniority", pa.string()),
    pa.field("directapply", pa.bool_()),
    pa.field("load_date", pa.string()),
])

BRONZE_SCRAPPED_JOBS_SCHEMA = pa.schema([
    pa.field("title", pa.string()),
    pa.field("company", pa.string()),
    pa.field("company_avatar_url", pa.string()),
    pa.field("company_location", pa.string()),
    pa.field("location_working_type", pa.string()),
    pa.field("working_type", pa.string()),
    pa.field("date_posted", pa.string()),
    pa.field("number_applicants", pa.string()),
    pa.field("job_url", pa.string()),
    pa.field("description", pa.string()),
    pa.field("salary", pa.string()),
    pa.field("role", pa.string()),
    pa.field("level", pa.string()),
    pa.field("source", pa.string()),
    pa.field("load_date", pa.string()),
])

# Silver layer schemas
SILVER_JOBS_SCHEMA = pa.schema([
    pa.field("title", pa.string()),
    pa.field("company_id", pa.string()),
    pa.field("source_name", pa.string()),
    pa.field("location_type", pa.string()),
    pa.field("employment_type", pa.string()),
    pa.field("date_posted", pa.string()),
    pa.field("techstacks", pa.list_(pa.string())),
    pa.field("job_url", pa.string()),
    pa.field("salary", pa.string()),
    pa.field("level", pa.string()),
    pa.field("role", pa.string()),
    pa.field("number_applicants", pa.int32()),
    pa.field("processed_at", pa.timestamp('us')),
])

SILVER_COMPANIES_SCHEMA = pa.schema([
    pa.field("id", pa.string()),
    pa.field("name", pa.string()),
    pa.field("industry", pa.string()),
    pa.field("company_size", pa.string()),
    pa.field("location", pa.string()),
    pa.field("source_name", pa.string()),
])

# Gold layer schemas
GOLD_JOB_ANALYTICS_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("company_name", pa.string()),
    pa.field("job_title", pa.string()),
    pa.field("location_type", pa.string()),
    pa.field("employment_type", pa.string()),
    pa.field("seniority_level", pa.string()),
    pa.field("total_jobs", pa.int64()),
    pa.field("total_applicants", pa.int64()),
    pa.field("avg_applicants_per_job", pa.float64()),
    pa.field("top_technologies", pa.list_(pa.string())),
    pa.field("tech_count", pa.int32()),
])

GOLD_TECH_TRENDS_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("technology", pa.string()),
    pa.field("job_count", pa.int64()),
    pa.field("percentage", pa.float64()),
    pa.field("avg_salary_min", pa.float64()),
    pa.field("avg_salary_max", pa.float64()),
])

GOLD_SALARY_INSIGHTS_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("job_title", pa.string()),
    pa.field("seniority_level", pa.string()),
    pa.field("location_type", pa.string()),
    pa.field("avg_salary_min", pa.float64()),
    pa.field("avg_salary_max", pa.float64()),
    pa.field("median_salary", pa.float64()),
    pa.field("job_count", pa.int64()),
])

# Partition specifications
JOBS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1, transform=DayTransform(), name="processed_at_day")
)

COMPANIES_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1, transform=IdentityTransform(), name="source_name")
)

ANALYTICS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1, transform=DayTransform(), name="date_day")
)
