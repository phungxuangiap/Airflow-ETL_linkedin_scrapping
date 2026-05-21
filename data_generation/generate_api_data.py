#!/usr/bin/env python3
"""
Script to generate mock LinkedIn job API data
Generates JSONL files with jobboard API format
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path


def generate_api_jobs():
    """Generate mock API source data with full jobboard format."""
    jobs = [
        {
            "id": "1885795075",
            "date_posted": "2025-10-13T13:56:06",
            "date_created": "2025-10-13T13:56:35.543987",
            "title": "Développeur C# (H/F)",
            "organization": "Extia",
            "organization_url": "https://www.linkedin.com/company/extia",
            "date_validthrough": "2025-11-12T13:56:06",
            "locations_raw": [
                {
                    "@type": "Place",
                    "address": {
                        "@type": "PostalAddress",
                        "addressCountry": "FR",
                        "addressLocality": "Brest",
                        "addressRegion": None,
                        "streetAddress": None
                    },
                    "latitude": 48.38987,
                    "longitude": -4.487176
                }
            ],
            "location_type": None,
            "location_requirements_raw": None,
            "salary_raw": {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 35000,
                    "maxValue": 42000,
                    "unitText": "YEAR"
                }
            },
            "employment_type": ["FULL_TIME"],
            "url": "https://fr.linkedin.com/jobs/view/développeur-c-h-f-at-extia-4314025490",
            "source_type": "jobboard",
            "source": "linkedin",
            "source_domain": "fr.linkedin.com",
            "organization_logo": "https://media.licdn.com/dms/image/v2/D560BAQFcwg6XJ5r3Eg/company-logo_200_200/company-logo_200_200/0/1667403446853/extia_logo",
            "cities_derived": ["Brest"],
            "counties_derived": ["Finistère"],
            "regions_derived": ["Brittany"],
            "countries_derived": ["France"],
            "locations_derived": ["Brest, Brittany, France"],
            "timezones_derived": ["Europe/Paris"],
            "lats_derived": [48.3905283],
            "lngs_derived": [-4.4860088],
            "remote_derived": False,
            "linkedin_org_employees": 1887,
            "linkedin_org_url": "http://www.extia-group.com",
            "linkedin_org_size": "1,001-5,000 employees",
            "linkedin_org_slogan": "First who, then what!",
            "linkedin_org_industry": "IT Services and IT Consulting",
            "linkedin_org_followers": 264863,
            "linkedin_org_headquarters": "Sèvres, Île-de-France",
            "linkedin_org_type": "Partnership",
            "linkedin_org_foundeddate": "2007",
            "seniority": "Cadre",
            "directapply": False
        },
        {
            "id": "1885795076",
            "date_posted": "2025-10-14T10:30:00",
            "date_created": "2025-10-14T10:30:45.123456",
            "title": "Senior Python Developer",
            "organization": "TechCorp",
            "organization_url": "https://www.linkedin.com/company/techcorp",
            "date_validthrough": "2025-11-13T10:30:00",
            "locations_raw": [
                {
                    "@type": "Place",
                    "address": {
                        "@type": "PostalAddress",
                        "addressCountry": "FR",
                        "addressLocality": "Paris",
                        "addressRegion": "Île-de-France",
                        "streetAddress": None
                    },
                    "latitude": 48.8566,
                    "longitude": 2.3522
                }
            ],
            "location_type": "REMOTE",
            "location_requirements_raw": None,
            "salary_raw": {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 50000,
                    "maxValue": 65000,
                    "unitText": "YEAR"
                }
            },
            "employment_type": ["FULL_TIME"],
            "url": "https://fr.linkedin.com/jobs/view/senior-python-developer-at-techcorp",
            "source_type": "jobboard",
            "source": "linkedin",
            "source_domain": "fr.linkedin.com",
            "organization_logo": "https://example.com/techcorp_logo.png",
            "cities_derived": ["Paris"],
            "counties_derived": ["Paris"],
            "regions_derived": ["Île-de-France"],
            "countries_derived": ["France"],
            "locations_derived": ["Paris, Île-de-France, France"],
            "timezones_derived": ["Europe/Paris"],
            "lats_derived": [48.8566],
            "lngs_derived": [2.3522],
            "remote_derived": True,
            "linkedin_org_employees": 500,
            "linkedin_org_url": "http://www.techcorp.com",
            "linkedin_org_size": "201-500 employees",
            "seniority": "Senior",
            "directapply": True
        },
        {
            "id": "1885795077",
            "date_posted": "2025-10-15T14:20:00",
            "date_created": "2025-10-15T14:20:30.654321",
            "title": "Data Engineer (Spark/Scala)",
            "organization": "DataSystems",
            "organization_url": "https://www.linkedin.com/company/datasystems",
            "date_validthrough": "2025-11-14T14:20:00",
            "locations_raw": [
                {
                    "@type": "Place",
                    "address": {
                        "@type": "PostalAddress",
                        "addressCountry": "BE",
                        "addressLocality": "Brussels",
                        "addressRegion": None,
                        "streetAddress": None
                    },
                    "latitude": 50.8503,
                    "longitude": 4.3517
                }
            ],
            "location_type": "HYBRID",
            "location_requirements_raw": None,
            "salary_raw": {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 55000,
                    "maxValue": 70000,
                    "unitText": "YEAR"
                }
            },
            "employment_type": ["FULL_TIME", "CONTRACT"],
            "url": "https://be.linkedin.com/jobs/view/data-engineer-at-datasystems",
            "source_type": "jobboard",
            "source": "linkedin",
            "source_domain": "be.linkedin.com",
            "organization_logo": "https://example.com/datasystems_logo.png",
            "cities_derived": ["Brussels"],
            "counties_derived": ["Brussels"],
            "regions_derived": ["Brussels-Capital Region"],
            "countries_derived": ["Belgium"],
            "locations_derived": ["Brussels, Brussels-Capital Region, Belgium"],
            "timezones_derived": ["Europe/Brussels"],
            "lats_derived": [50.8503],
            "lngs_derived": [4.3517],
            "remote_derived": False,
            "linkedin_org_employees": 300,
            "linkedin_org_url": "http://www.datasystems.be",
            "linkedin_org_size": "201-500 employees",
            "seniority": "Mid-Level",
            "directapply": False
        },
        {
            "id": "1885795078",
            "date_posted": "2025-10-16T14:45:00",
            "date_created": "2025-10-16T14:45:30.123456",
            "title": "DevOps Engineer",
            "organization": "CloudInnovate",
            "organization_url": "https://www.linkedin.com/company/cloudinnovate",
            "date_validthrough": "2025-11-15T14:45:00",
            "locations_raw": [
                {
                    "@type": "Place",
                    "address": {
                        "@type": "PostalAddress",
                        "addressCountry": "FR",
                        "addressLocality": "Lyon",
                        "addressRegion": "Rhône-Alpes",
                        "streetAddress": None
                    },
                    "latitude": 45.7640,
                    "longitude": 4.8357
                }
            ],
            "location_type": "HYBRID",
            "location_requirements_raw": None,
            "salary_raw": {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 45000,
                    "maxValue": 58000,
                    "unitText": "YEAR"
                }
            },
            "employment_type": ["FULL_TIME"],
            "url": "https://fr.linkedin.com/jobs/view/devops-engineer-at-cloudinnovate",
            "source_type": "jobboard",
            "source": "linkedin",
            "source_domain": "fr.linkedin.com",
            "organization_logo": "https://example.com/cloudinnovate_logo.png",
            "cities_derived": ["Lyon"],
            "counties_derived": ["Rhône"],
            "regions_derived": ["Rhône-Alpes"],
            "countries_derived": ["France"],
            "locations_derived": ["Lyon, Rhône-Alpes, France"],
            "timezones_derived": ["Europe/Paris"],
            "lats_derived": [45.7640],
            "lngs_derived": [4.8357],
            "remote_derived": False,
            "linkedin_org_employees": 450,
            "linkedin_org_url": "http://www.cloudinnovate.fr",
            "linkedin_org_size": "201-500 employees",
            "seniority": "Mid-Level",
            "directapply": False
        },
        {
            "id": "1885795079",
            "date_posted": "2025-10-17T11:20:00",
            "date_created": "2025-10-17T11:20:45.654321",
            "title": "Machine Learning Engineer",
            "organization": "AIVision",
            "organization_url": "https://www.linkedin.com/company/aivision",
            "date_validthrough": "2025-11-16T11:20:00",
            "locations_raw": [
                {
                    "@type": "Place",
                    "address": {
                        "@type": "PostalAddress",
                        "addressCountry": "FR",
                        "addressLocality": "Grenoble",
                        "addressRegion": "Isère",
                        "streetAddress": None
                    },
                    "latitude": 45.1885,
                    "longitude": 5.7245
                }
            ],
            "location_type": None,
            "location_requirements_raw": None,
            "salary_raw": {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "minValue": 55000,
                    "maxValue": 75000,
                    "unitText": "YEAR"
                }
            },
            "employment_type": ["FULL_TIME"],
            "url": "https://fr.linkedin.com/jobs/view/machine-learning-engineer-at-aivision",
            "source_type": "jobboard",
            "source": "linkedin",
            "source_domain": "fr.linkedin.com",
            "organization_logo": "https://example.com/aivision_logo.png",
            "cities_derived": ["Grenoble"],
            "counties_derived": ["Isère"],
            "regions_derived": ["Auvergne-Rhône-Alpes"],
            "countries_derived": ["France"],
            "locations_derived": ["Grenoble, Isère, France"],
            "timezones_derived": ["Europe/Paris"],
            "lats_derived": [45.1885],
            "lngs_derived": [5.7245],
            "remote_derived": False,
            "linkedin_org_employees": 120,
            "linkedin_org_url": "http://www.aivision.ai",
            "linkedin_org_size": "51-200 employees",
            "seniority": "Senior",
            "directapply": True
        }
    ]
    return jobs


def generate_api_files(base_path=None):
    """
    Generate API data file for today.
    
    Args:
        base_path: Directory where files will be saved. 
                   If None, uses /project_root/tmp/api_sources (Docker) or local project path
    """
    if base_path is None:
        # Try Docker path first, then local path
        if os.path.exists('/project_root'):
            base_path = '/project_root/tmp/api_sources'
        else:
            base_path = '/mnt/Document/project/Airflow-ETL_linkedin_scrapping/tmp/api_sources'
    Path(base_path).mkdir(parents=True, exist_ok=True)
    
    jobs = generate_api_jobs()
    
    date = datetime.now()
    timestamp_str = date.strftime("%Y%m%d_%H%M%S")
    
    filepath = Path(base_path) / f"linkedin_jobs_api_{timestamp_str}.jsonl"
    
    # Write as JSONL format (one JSON object per line)
    with open(filepath, 'w', encoding='utf-8') as f:
        for job in jobs:
            f.write(json.dumps(job, ensure_ascii=False) + '\n')
    
    # Get file size in bytes
    timestamp = datetime.now().isoformat()
    file_size = os.path.getsize(filepath)
    filename = f"linkedin_jobs_api_{timestamp_str}.jsonl"
    
    result = {
        'filepath': str(filepath),
        'filename': filename,
        'job_count': len(jobs),
        'timestamp': timestamp,
        'file_size': file_size
    }
    
    print(f"✅ Created: {filepath}")
    print(f"📊 Metadata: {result}")
    return result


if __name__ == "__main__":
    import sys
    
    # Default path is project tmp/api_sources directory
    output_path = sys.argv[1] if len(sys.argv) > 1 else "/mnt/Document/project/Airflow-ETL_linkedin_scrapping/tmp/api_sources"
    
    file = generate_api_files(output_path)
    print(f"📁 File created for today")
    print(f"📍 Location: {output_path}")
