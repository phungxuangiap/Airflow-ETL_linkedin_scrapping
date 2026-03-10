#!/usr/bin/env python3
"""
Script to generate mock LinkedIn job scrapped data
Generates JSON files with Job dataclass format
"""

import json
import os
from datetime import datetime
from pathlib import Path


def generate_scrapped_jobs():
    """Generate mock scrapped data matching Job dataclass format."""
    jobs = [
        {
            "title": "Data Engineer (Spark/Scala)",
            "company": "DataSystems",
            "company_avatar_url": "https://example.com/datasystems_logo.png",
            "company_location": "Brussels, Belgium",
            "location_working_type": "Hybrid",
            "working_type": "Full-time",
            "date_posted": "2025-10-15T14:20:00",
            "number_applicants": "67",
            "job_url": "https://be.linkedin.com/jobs/view/data-engineer-at-datasystems",
            "description": "Looking for experienced Data Engineer with Spark expertise. Build scalable data pipelines and ETL processes...",
            "salary": "€55,000 - €70,000/year",
            "role": "Data Engineer",
            "level": "Mid-Level"
        },
        {
            "title": "Backend Engineer (Java/Spring)",
            "company": "EnterpriseSoft",
            "company_avatar_url": "https://example.com/enterprisesoft_logo.png",
            "company_location": "Nantes, Pays de la Loire",
            "location_working_type": "On-site",
            "working_type": "Full-time",
            "date_posted": "2025-10-16T10:00:00",
            "number_applicants": "73",
            "job_url": "https://fr.linkedin.com/jobs/view/backend-engineer-java-spring-at-enterprisesoft",
            "description": "Build scalable backend services with Java Spring Boot. Microservices architecture and REST API development...",
            "salary": "€45,000 - €60,000/year",
            "role": "Backend Developer",
            "level": "Mid-Level"
        },
        {
            "title": "QA Automation Engineer",
            "company": "QualityFirst",
            "company_avatar_url": "https://example.com/qualityfirst_logo.png",
            "company_location": "Lille, Hauts-de-France",
            "location_working_type": "Remote",
            "working_type": "Full-time",
            "date_posted": "2025-10-17T09:30:00",
            "number_applicants": "91",
            "job_url": "https://fr.linkedin.com/jobs/view/qa-automation-engineer-at-qualityfirst",
            "description": "Design and implement automated test suites using Selenium/Cypress. CI/CD integration and test reporting...",
            "salary": "€38,000 - €48,000/year",
            "role": "QA Engineer",
            "level": "Junior to Mid-Level"
        },
        {
            "title": "Solutions Architect",
            "company": "TechConsulting",
            "company_avatar_url": "https://example.com/techconsulting_logo.png",
            "company_location": "Nice, Provence-Alpes-Côte d'Azur",
            "location_working_type": "Hybrid",
            "working_type": "Full-time",
            "date_posted": "2025-10-18T14:15:00",
            "number_applicants": "36",
            "job_url": "https://fr.linkedin.com/jobs/view/solutions-architect-at-techconsulting",
            "description": "Lead technical solutions for enterprise clients. Gather requirements, design architectures, and manage implementation...",
            "salary": "€65,000 - €85,000/year",
            "role": "Solutions Architect",
            "level": "Senior"
        },
        {
            "title": "Security Engineer",
            "company": "CyberGuard",
            "company_avatar_url": "https://example.com/cyberguard_logo.png",
            "company_location": "Toulouse, Occitanie",
            "location_working_type": "On-site",
            "working_type": "Full-time",
            "date_posted": "2025-10-19T11:00:00",
            "number_applicants": "44",
            "job_url": "https://fr.linkedin.com/jobs/view/security-engineer-at-cyberguard",
            "description": "Implement security measures, conduct penetration testing, and vulnerability assessments. OWASP best practices...",
            "salary": "€50,000 - €68,000/year",
            "role": "Security Engineer",
            "level": "Senior"
        }
    ]
    return jobs


def generate_scrapped_files(base_path=None):
    """
    Generate scrapped data file for today.
    
    Args:
        base_path: Directory where files will be saved.
                   If None, uses /project_root/tmp/scrapping_script (Docker) or local path
    """
    if base_path is None:
        if os.path.exists('/project_root'):
            base_path = '/project_root/tmp/scrapping_script'
        else:
            base_path = '/mnt/Document/project/Airflow-ETL_linkedin_scrapping/tmp/scrapping_script'
    
    Path(base_path).mkdir(parents=True, exist_ok=True)
    
    jobs = generate_scrapped_jobs()
    
    date = datetime.now()
    timestamp_str = date.strftime("%Y%m%d_%H%M%S")
    
    filepath = Path(base_path) / f"linkedin_jobs_scrapped_{timestamp_str}.jsonl"
    
    # Write as JSONL format (one JSON object per line)
    with open(filepath, 'w', encoding='utf-8') as f:
        for job in jobs:
            f.write(json.dumps(job, ensure_ascii=False) + '\n')
    
    # Get file size in bytes
    timestamp = datetime.now().isoformat()
    file_size = os.path.getsize(str(filepath))
    filename = f"linkedin_jobs_scrapped_{timestamp_str}.jsonl"
    
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
    import os
    
    # Determine default path based on environment
    if os.path.exists('/project_root'):
        default_path = '/project_root/tmp/scrapping_script'
    else:
        default_path = '/mnt/Document/project/Airflow-ETL_linkedin_scrapping/tmp/scrapping_script'
    
    output_path = sys.argv[1] if len(sys.argv) > 1 else default_path
    
    file = generate_scrapped_files(output_path)
    print(f"📁 File created for today")
    print(f"📍 Location: {output_path}")
