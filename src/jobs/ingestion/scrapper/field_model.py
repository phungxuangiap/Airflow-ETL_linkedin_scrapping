from dataclasses import dataclass
from typing import FrozenSet, Tuple


@dataclass(frozen=True)
class ScraperFieldModel:
    source_name: str = "source_name"
    title: str = "title"
    company: str = "company"
    company_location: str = "company_location"
    location_working_type: str = "location_working_type"
    working_type: str = "working_type"
    date_posted: str = "date_posted"
    number_applicants: str = "number_applicants"
    prefix_job_url: str = "prefix_job_url"
    job_url: str = "job_url"
    description: str = "description"
    salary: str = "salary"
    role: str = "role"
    level: str = "level"

    @property
    def source_config_fields(self) -> Tuple[str, ...]:
        return (
            self.source_name,
            self.title,
            self.company,
            self.company_location,
            self.location_working_type,
            self.working_type,
            self.date_posted,
            self.number_applicants,
            self.prefix_job_url,
            self.job_url,
            self.description,
            self.salary,
            self.role,
            self.level,
        )

    @property
    def required_fields(self) -> FrozenSet[str]:
        return frozenset(
            {
                self.source_name,
                self.title,
                self.prefix_job_url,
                self.job_url,
            }
        )

    @property
    def nullable_fields(self) -> FrozenSet[str]:
        return frozenset(self.source_config_fields) - self.required_fields

    @property
    def detail_enrichment_fields(self) -> Tuple[str, ...]:
        return (
            self.description,
            self.salary,
            self.role,
            self.level,
            self.working_type,
            self.location_working_type,
            self.date_posted,
            self.number_applicants,
        )

    @property
    def listing_extract_fields(self) -> Tuple[str, ...]:
        return tuple(
            field
            for field in self.source_config_fields
            if field not in {self.source_name, self.prefix_job_url, self.description}
        )


SCRAPER_FIELD_MODEL = ScraperFieldModel()
