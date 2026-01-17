from unittest import mock
import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import List, Dict, Any

mock_all_jobs = [
    {
    "id": "1945301034",
    "date_posted": "2026-01-15T05:54:53",
    "date_created": "2026-01-15T06:03:24.506172",
    "title": "Data Engineer",
    "organization": "Durlston Partners",
    "organization_url": "https://www.linkedin.com/company/durlston-partners",
    "date_validthrough": "2026-02-14T05:54:53",
    "locations_raw": [
      {
        "@type": "Place",
        "address": {
          "@type": "PostalAddress",
          "addressCountry": "GB",
          "addressLocality": "London Area",
          "addressRegion": 'null',
          "streetAddress": 'null'
        },
        "latitude": 51.51649,
        "longitude": -0.128427
      }
    ],
    "location_type": 'null',
    "location_requirements_raw": 'null',
    "salary_raw": 'null',
    "employment_type": [
      "FULL_TIME"
    ],
    "url": "https://uk.linkedin.com/jobs/view/data-engineer-at-durlston-partners-4353260120",
    "source_type": "jobboard",
    "source": "linkedin",
    "source_domain": "uk.linkedin.com",
    "organization_logo": "https://media.licdn.com/dms/image/v2/C4D0BAQFuuhTCF5UhNw/company-logo_200_200/company-logo_200_200/0/1630461162788/durlston_partners_llp_logo?e=2147483647&v=beta&t=_K4pY_RoeHC84eLyxKOvPQfWUgkGBJ8ZUB4LBn9dEfQ",
    "cities_derived": [
      "London"
    ],
    "counties_derived": [
      "Greater London"
    ],
    "regions_derived": [
      "England"
    ],
    "countries_derived": [
      "United Kingdom"
    ],
    "locations_derived": [
      "London, England, United Kingdom"
    ],
    "timezones_derived": [
      "Europe/London"
    ],
    "lats_derived": [
      51.5074456
    ],
    "lngs_derived": [
      -0.1277653
    ],
    "remote_derived": 'false',
    "linkedin_org_employees": 26,
    "linkedin_org_url": "http://www.durlstonpartners.com",
    "linkedin_org_size": "11-50 employees",
    "linkedin_org_slogan": "Expert talent advisory and delivery for the global AI, tech, quantitative finance, crypto and data-science communities",
    "linkedin_org_industry": "Staffing and Recruiting",
    "linkedin_org_followers": 168204,
    "linkedin_org_headquarters": "London, England",
    "linkedin_org_type": "Privately Held",
    "linkedin_org_foundeddate": "2010",
    "linkedin_org_specialties": [
      ""
    ],
    "linkedin_org_locations": [
      "1 Conduit Street, London, England W1S 2XA, GB",
      "750 Lexington Ave, New York, NY, New York NY 10011, US",
      "Dubai, Dubai, AE"
    ],
    "linkedin_org_description": "Durlston Partners have specialised in building Hedge Funds, Family Offices, AI and Digital Asset Firms for over 15 years, across the globe, with offices in London, New York, Dubai and Mumbai. \n\nWe provide talent, market intelligence, capital and world class events, globally. \n\nWe apply a human touch, kindness and deep insight into building robust teams that stand the test of time, building world class firms from the ground up through the top Quantitative Portfolio Manager, Traders, Software / AI Engineers and Data Scientists.  \n",
    "linkedin_org_recruitment_agency_derived": 'true',
    "seniority": "Mid-Senior level",
    "directapply": 'true',
    "linkedin_org_slug": "durlston-partners",
    "no_jb_schema": 'null',
    "external_apply_url": 'null',
    "ats_duplicate": 'null',
    "description_text": "Data Engineer (Anonymised)\nLocation: Middle East (primary) | Open to international candidates, relocation supported\nOverview A large, globally active institutional investment organisation is seeking a Data Engineer to join its central data and analytics platform team. The role focuses on building and maintaining scalable, reliable data infrastructure that supports investment teams across public and private markets. This search is being conducted on an anonymised basis; further details will be shared with shortlisted candidates.\nRole Scope As a Data Engineer, you will design, build, and operate data pipelines and platforms that power research, portfolio management, risk, and reporting. You will work closely with investment professionals, quantitative teams, and technology partners to ensure high-quality, well-governed data is available across the organisation.\nKey Responsibilities\nDesign and develop scalable data pipelines for structured and unstructured data\nBuild and maintain data ingestion frameworks from internal and external sources\nEnsure data quality, reliability, lineage, and documentation across platforms\nOptimise data storage and retrieval for analytics and downstream consumption\nPartner with investment, risk, and research teams to understand data needs\nSupport cloud-based and on-prem data platforms and tooling\nImplement monitoring, alerting, and best practices for production data systems\nContribute to data governance, security, and access control initiatives\nRequired Experience & Skills\nProfessional experience as a Data Engineer or in a closely related role\nStrong SQL skills and experience with at least one programming language (e.g. Python, Java, or Scala)\nExperience building ETL/ELT pipelines and working with large datasets\nFamiliarity with data warehousing and data lake architectures\nUnderstanding of software engineering best practices (testing, version control, CI/CD)\nStrong problem-solving skills and ability to work with non-technical stakeholders\nPreferred / Nice-to-Have\nExperience with cloud platforms (AWS, Azure, or GCP)\nExposure to investment, financial market, or risk data\nExperience with workflow orchestration tools (e.g. Airflow)\nKnowledge of streaming technologies or real-time data processing\nExperience working in large, complex, or highly regulated environments\nWhat This Role Is Not\nNot a pure BI or reporting role\nNot a data science or research position\nNot a short-term contract or consulting role\nWhy This Opportunity\nWork at scale on data platforms supporting global investment activities\nLong-term, stable environment with significant investment in technology\nHigh-impact role supporting multiple asset classes and strategies\nCollaborative culture with strong emphasis on quality and robustness"
  }
]

list_role_filters = [
    "Data Engineer", 
    # "Data Scientist", 
    "Machine Learning Engineer",
    "Data Analyst", 
    # "Full Stack Developer", 
    # "DevOps Engineer", 
    # "Cloud Engineer", 
    # "Software Engineer", 
    # "AI Engineer", 
    # "ETL Developer", 
    # "Data Architect", 
    # "Backend Engineer",
    # "Frontend Developer",
    # "Mobile Developer",
    # "Embedded Engineer",
    # "Game Developer",
    ]

load_dotenv()
country_filters = 'Vietnam'
RAPIDAPI_HOST = "linkedin-job-search-api.p.rapidapi.com"
api_keys_str = os.getenv("_KEY_RAPIDAPI", "[]")
RAPIDAPI_KEYS = json.loads(api_keys_str)
current_key_index = 0

def get_next_api_key() -> str:
    """Get next API key from the list."""
    global current_key_index
    if not RAPIDAPI_KEYS:
        raise ValueError("No API keys available in _KEY_RAPIDAPI")
    
    current_key_index = (current_key_index + 1) % len(RAPIDAPI_KEYS)
    return RAPIDAPI_KEYS[current_key_index]

def fetch_jobs_from_api_and_store_in_local(
    limit: int = 2,
    offset: int = 0,
    max_retries: int = 2,
    RAPIDAPI_KEYS: List[str] = RAPIDAPI_KEYS,
    country_filters: str = country_filters,
    RAPIDAPI_HOST: str = RAPIDAPI_HOST,
    list_role_filters: List[str] = list_role_filters,
    LOCAL_TEMP_DIR: str = '/tmp/api_source/linkedin_jobs'
) -> List[Dict[str, Any]]:
    """
    Fetch jobs from LinkedIn API for each role filter with API key rotation.
    
    Args:
        limit: Number of results per request
        offset: Offset for pagination
        max_retries: Maximum retries per role before skipping
        RAPIDAPI_KEYS: List[str] = RAPIDAPI_KEYS,
        country_filters: str = country_filters,
        RAPIDAPI_HOST: str = RAPIDAPI_HOST,
        list_role_filters: List[str] = list_role_filters,
        LOCAL_TEMP_DIR: str = '/tmp/api_source/linkedin_jobs'
    Returns:
        List of job data combined from all role filters
    """
    global current_key_index
    all_jobs = []
    
    for role in list_role_filters:
        retry_count = 0
        success = False
        print(f"Fetching jobs for role: {role}")
        while retry_count < max_retries and not success:
            try:
                url = f"https://{RAPIDAPI_HOST}/active-jb-24h"
                
                current_key = RAPIDAPI_KEYS[current_key_index]
                
                params = {
                    "limit": limit,
                    "offset": offset,
                    "title_filter": f'"{role}"',
                    "location_filter": f'"{country_filters}"',
                    "description_type": "text"
                }
                
                headers = {
                    "x-rapidapi-host": RAPIDAPI_HOST,
                    "x-rapidapi-key": current_key
                }
                
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                # Handle 429 Too Many Requests
                if response.status_code == 429:
                    print(f"⚠ Rate limit reached with key index {current_key_index}. Switching to next key...")
                    get_next_api_key()
                    retry_count += 1
                    continue
                
                response.raise_for_status()
                
                data = response.json()
                for job in data:
                    job["role_filter"] = role  
                    all_jobs.append(job)
                    
                success = True
                
            except requests.exceptions.RequestException as e:
                if "429" in str(e):
                    print(f"⚠ Rate limit error for role '{role}'. Switching key...")
                    get_next_api_key()
                    retry_count += 1
                else:
                    print(f"✗ Error fetching jobs for role '{role}': {str(e)}")
                    retry_count += 1
        
        if not success:
            print(f"✗ Failed to fetch jobs for role '{role}' after {max_retries} retries")
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_from_api_{timestamp}.jsonl"
    output_file = os.path.join(LOCAL_TEMP_DIR, filename)
    with open(output_file, 'w', encoding='utf-8') as f:
        for job in all_jobs:
            f.write(json.dumps(job, ensure_ascii=False) + '\n')
    return {
        'filepath': output_file,
        'filename': filename,
        'job_count': len(all_jobs),
        'timestamp': timestamp,
        'file_size': os.path.getsize(output_file)
    }

def main():
    """Main function to orchestrate the job fetching."""
    print(f"Starting job fetch for {len(list_role_filters)} roles in {country_filters}...")
    print(f"Using {len(RAPIDAPI_KEYS)} API keys")
    
    # Fetch jobs
    jobs = fetch_jobs_from_api_and_store_in_local()
    
    if jobs:
        print(jobs)
    else:
        print("No jobs found!")

if __name__ == "__main__":
    main()