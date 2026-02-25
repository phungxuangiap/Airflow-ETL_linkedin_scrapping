from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List


# think to refactor or remove that.
@dataclass
class ScrappedObject:
    list_job_url: str
    job_details_path_url: str
    scrollable_entrypoint: str
    job_entrypoint: str
    step_pagination: int
    pagination_query_param: str
    login_page_url: Optional[str] = None
    login_user_name: Optional[str] = None
    login_user_password: Optional[str] = None
    

    def __post_init__(self):
        print(f"Khởi tạo thành công object cho: {self.list_job_url}")