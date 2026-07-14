from urllib.parse import quote_plus

from src.constants.job_fields import JOB_SOURCE_ROLE_SLUGS


def _build_jobs_source():
    sources = []
    for role_slug in JOB_SOURCE_ROLE_SLUGS:
        role = role_slug.replace("-", " ")
        role_query = quote_plus(role)
        sources.extend(
            [
                # {
                #     "source_name": "careerviet",
                #     "role": role,
                #     "url": f"https://careerviet.vn/viec-lam/{role_slug}-k-trang-1-vi.html",
                #     "entry_point": ".job-item",
                #     "detail_entry_point": ".job-detail-content",
                # },
                # {
                #     "source_name": "joboko",
                #     "role": role,
                #     "url": f"https://vn.joboko.com/jobs?q={role_query}&pr=1",
                #     "entry_point": ".nw-job-list__list > .item",
                #     "detail_entry_point": ".nw-job-list__main.nw-wysiwyg",
                # },
                # {
                #     "source_name": "careerlink",
                #     "role": role,
                #     "url": f"https://www.careerlink.vn/viec-lam/k/{role_slug}",
                #     "entry_point": ".list-group-item.job-item",
                #     "detail_entry_point": ".job-detail",
                # },
                # {
                #     "source_name": "topdev",
                #     "role": role,
                #     "url": f"https://topdev.vn/viec-lam/tim-kiem?keyword={role_query}",
                #     "entry_point": 'div.text-card-foreground:has(> div.relative img[alt="job-image"]):has(a[href^="/viec-lam/"])',
                #     "detail_entry_point": 'div:has(span:-soup-contains("Your role & responsibilities")):has(span:-soup-contains("Your skills & qualifications")):has(span:-soup-contains("Benefits"))',
                # },
                # {
                #     "source_name": "itviet",
                #     "role": role,
                #     "url": f"https://itviec.com/it-jobs/{role_slug}",
                #     "entry_point": ".job-card",
                #     "detail_entry_point": '.preview-job-content'
                # }
                # {
                #     "source_name": "topCV",
                #     "scraper": "bypass",
                #     "role": role,
                #     "url": f"https://www.topcv.vn/tim-viec-lam-software-engineer?page=2",
                #     "entry_point": ".job-item-search-result",
                #     "detail_entry_point": ".job-detail__information-detail"
                # }
                {
                    "source_name": "jobsgo",
                    "scraper": "bypass",
                    "role": role,
                    "url": f"https://jobsgo.vn/viec-lam-software-engineer.html",
                    "entry_point": ".card.job-card",
                    "detail_entry_point": ".job-detail-card"
                }
            ]
        )
    return sources


JOBS_SOURCE = _build_jobs_source()
