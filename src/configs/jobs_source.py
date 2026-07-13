from urllib.parse import quote_plus


LIST_ROLE = [
    # "software engineer",
    # "software developer",
    # "backend engineer",
    # "backend developer",
    # "frontend engineer",
    # "frontend developer",
    # "fullstack engineer",
    # "fullstack developer",
    # "web developer",
    # "mobile developer",
    # "android developer",
    # "ios developer",
    # "flutter developer",
    # "react native developer",
    # "java developer",
    # "python developer",
    # "golang developer",
    # "nodejs developer",
    # "php developer",
    # "dotnet developer",
    # "ruby developer",
    # "c++ developer",
    # "embedded software engineer",
    # "firmware engineer",
    # "game developer",
    # "blockchain developer",
    # "ai engineer",
    # "machine learning engineer",
    # "deep learning engineer",
    # "computer vision engineer",
    # "nlp engineer",
    # "data engineer",
    # "big data engineer",
    # "data analyst",
    # "business intelligence analyst",
    # "bi developer",
    # "data scientist",
    # "database administrator",
    # "database developer",
    # "etl developer",
    # "analytics engineer",
    # "devops engineer",
    # "site reliability engineer",
    # "platform engineer",
    # "cloud engineer",
    # "cloud architect",
    # "solutions architect",
    # "system administrator",
    # "linux administrator",
    # "network engineer",
    # "security engineer",
    # "cybersecurity analyst",
    # "information security analyst",
    # "penetration tester",
    # "soc analyst",
    # "qa engineer",
    # "quality assurance engineer",
    # "manual tester",
    # "automation tester",
    # "test engineer",
    # "performance tester",
    # "business analyst",
    # "system analyst",
    # "product owner",
    # "product manager",
    # "project manager",
    # "scrum master",
    # "technical lead",
    # "engineering manager",
    # "it manager",
    # "it support",
    # "helpdesk technician",
    # "technical support engineer",
    # "erp consultant",
    # "sap consultant",
    # "salesforce developer",
    "ui ux designer",
]


def _role_slug(role: str) -> str:
    return role.strip().lower().replace(" ", "-")


def _build_jobs_source():
    sources = []
    for role in LIST_ROLE:
        role_slug = _role_slug(role)
        role_query = quote_plus(role.strip())
        sources.extend(
            [
                {
                    "source_name": "careerviet",
                    "role": role,
                    "url": f"https://careerviet.vn/viec-lam/{role_slug}-k-trang-1-vi.html",
                    "entry_point": ".job-item",
                    "detail_entry_point": ".job-detail-page",
                },
                {
                    "source_name": "joboko",
                    "role": role,
                    "url": f"https://vn.joboko.com/jobs?q={role_query}&pr=1",
                    "entry_point": ".nw-job-list__list > .item",
                    "detail_entry_point": ".nw-job-list__main.nw-wysiwyg",
                },
                # {
                #     "source_name": "careerlink",
                #     "role": role,
                #     "url": f"https://www.careerlink.vn/viec-lam/k/{role_slug}",
                #     "entry_point": ".list-group-item.job-item",
                #     "detail_entry_point": ".job-detail",
                # },
                {
                    "source_name": "topdev",
                    "role": role,
                    "url": f"https://topdev.vn/viec-lam/tim-kiem?keyword={role_query}",
                    "entry_point": 'div.text-card-foreground:has(> div.relative img[alt="job-image"]):has(a[href^="/viec-lam/"])',
                    "detail_entry_point": 'div:has(span:-soup-contains("Your role & responsibilities")):has(span:-soup-contains("Your skills & qualifications")):has(span:-soup-contains("Benefits"))',
                },
            ]
        )
    return sources


JOBS_SOURCE = _build_jobs_source()
