CREATE TABLE IF NOT EXISTS raw_jobs (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT,
    title TEXT,
    company TEXT,
    company_location TEXT,
    location_working_type TEXT,
    working_type TEXT,
    date_posted TEXT,
    number_applicants TEXT,
    job_url TEXT NOT NULL,
    description TEXT,
    salary TEXT,
    role TEXT,
    level TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT raw_jobs_job_url_key UNIQUE (job_url)
);

CREATE INDEX IF NOT EXISTS idx_raw_jobs_source_name ON raw_jobs (source_name);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_created_at ON raw_jobs (created_at);
