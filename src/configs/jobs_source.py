from urllib.parse import quote_plus

from src.constants.job_fields import JOB_SOURCE_ROLE_SLUGS


JOB_SOURCE_CONFIGS = [
    {
        "source_name": "careerviet",
        "url_template": "https://careerviet.vn/viec-lam/{role_slug}-k-trang-{page}-vi.html",
        "entry_point": ".job-item",
        "detail_entry_point": ".job-detail-content",
        "end_page": 2,
    },
    # {
    #     "source_name": "joboko",
    #     "url_template": "https://vn.joboko.com/jobs?q={role_query}",
    #     "entry_point": ".nw-job-list__list > .item",
    #     "detail_entry_point": ".nw-job-list__main.nw-wysiwyg",
    # },
    # {
    #     "source_name": "careerlink",
    #     "url_template": "https://www.careerlink.vn/viec-lam/k/{role_slug}",
    #     "entry_point": ".list-group-item.job-item",
    #     "detail_entry_point": ".job-detail",
    # },
    # {
    #     "source_name": "topdev",
    #     "url_template": "https://topdev.vn/viec-lam/tim-kiem?keyword={role_query}",
    #     "entry_point": 'div.text-card-foreground:has(> div.relative img[alt="job-image"]):has(a[href^="/viec-lam/"])',
    #     "detail_entry_point": 'div:has(span:-soup-contains("Your role & responsibilities")):has(span:-soup-contains("Your skills & qualifications")):has(span:-soup-contains("Benefits"))',
    # },
    {
        "source_name": "itviet",
        "url_template": "https://itviec.com/it-jobs/{role_slug}?page={page}",
        "entry_point": ".job-card",
        "detail_entry_point": ".preview-job-content",
        "end_page": 10,
    },
    {
        "source_name": "topCV",
        "scraper": "bypass",
        "url_template": "https://www.topcv.vn/tim-viec-lam-{role_slug}?page={page}",
        "entry_point": ".job-item-search-result",
        "detail_entry_point": ".job-detail__information-detail",
        "end_page": 5,
    },
    # {
    #     "source_name": "jobsgo",
    #     "scraper": "bypass",
    #     "url_template": "https://jobsgo.vn/viec-lam-{role_slug}.html?page={page}",
    #     "entry_point": ".card.job-card",
    #     "detail_entry_point": ".job-detail-card",
    #     "end_page": 1,
    # },
]


def _build_source_url(source_config, role_slug, role_query, page):
    return source_config["url_template"].format(
        role_slug=role_slug,
        role_query=role_query,
        page=page,
    )


def _build_source_entry(source_config, role, role_slug, role_query, page):
    source = {
        key: value
        for key, value in source_config.items()
        if key not in {"url_template", "end_page"}
    }
    source["role"] = role
    source["url"] = _build_source_url(source_config, role_slug, role_query, page)
    source["page"] = page
    return source


def _build_jobs_source():
    sources = []
    for role_slug in JOB_SOURCE_ROLE_SLUGS:
        role = role_slug.replace("-", " ")
        role_query = quote_plus(role)

        for source_config in JOB_SOURCE_CONFIGS:
            end_page = max(1, int(source_config.get("end_page", 1)))
            for page in range(1, end_page + 1):
                sources.append(_build_source_entry(source_config, role, role_slug, role_query, page))

    return sources


JOBS_SOURCE = _build_jobs_source()
