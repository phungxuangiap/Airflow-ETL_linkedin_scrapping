from dataclasses import dataclass
from typing import Optional, List, Any

@dataclass
class Job:
    """Represents a LinkedIn job posting with extracted information."""
    identifyers: Optional[Any]  # Deprecated: HTML element identifiers (no longer used)
    title: Optional[str]  # Job title/position name
    company: Optional[str]  # Company name
    company_avatar_url: Optional[str]  # URL to company logo/avatar image
    company_location: Optional[str]  # Company's physical location
    location_working_type: Optional[str]  # Location type (On-site, Remote, Hybrid)
    working_type: Optional[str]  # Employment type (Full-time, Part-time, Contract, etc.)
    date_posted: Optional[str]  # Date when the job was posted
    number_applicants: Optional[str]  # Number of applicants for the job
    job_url: Optional[str]  # Direct URL link to the job posting
    description: Optional[str]  # Full job description and requirements
    salary: Optional[str] = None  # Salary information if available
    role: Optional[str] = None  # Job role/category
    level: Optional[str] = None  # Seniority level (Entry, Mid, Senior, etc.)
    def __post_init__(self):
        pass