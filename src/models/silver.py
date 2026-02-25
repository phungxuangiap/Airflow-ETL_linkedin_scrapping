from dataclasses import dataclass
from typing import Optional, List, Any

@dataclass
class JobSilver:
    title: Optional[str]  # Job title/position name
    source_name: Optional[str]  # Source of the job posting (e.g., LinkedIn)
    company_id: Optional[str]  # Company identifier (e.g., LinkedIn company ID)
    location_type: Optional[str]  # Location type (On-site, Remote, Hybrid)
    employment_type: Optional[str]  # Employment type (Full-time, Part-time, Contract, etc.)
    date_posted: Optional[str]  # Date when the job was posted
    number_applicants: Optional[str]  # Number of applicants for the job
    job_url: Optional[str]  # Direct URL link to the job posting
    techstacks: List[Optional[str]]  # List of technologies/techstacks required for the job
    salary: Optional[str] = None  # Salary information if available
    role: Optional[str] = None  # Job role/category
    level: Optional[str] = None  # Seniority level (Entry, Mid, Senior, etc.)
    def __post_init__(self):
        pass


@dataclass
class CompanySilver:
    id: Optional[str]  # Company identifier (e.g., LinkedIn company ID)
    name: Optional[str]  # Name of the company
    industry: Optional[str]  # Industry sector of the company
    company_size: Optional[str]  # Size of the company (e.g., number of employees)
    location: Optional[str]  # Headquarters location of the company
    def __post_init__(self):
        pass