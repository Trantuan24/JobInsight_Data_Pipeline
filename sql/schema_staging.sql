CREATE SCHEMA IF NOT EXISTS jobinsight_staging;

CREATE TABLE IF NOT EXISTS jobinsight_staging.staging_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    title_clean VARCHAR(255) NOT NULL,
    job_url TEXT,
    company_name VARCHAR(200),
    company_name_standardized VARCHAR(200),
    company_url TEXT,
    salary VARCHAR(100),
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),
    skills JSONB,
    location VARCHAR(100),
    location_detail TEXT,
    location_pairs JSONB,
    deadline VARCHAR(50),
    verified_employer BOOLEAN,
    last_update VARCHAR(100),
    logo_url TEXT,
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    due_date TIMESTAMP WITH TIME ZONE,
    time_remaining TEXT,
    raw_data JSONB
);

CREATE INDEX IF NOT EXISTS staging_jobs_company_idx ON jobinsight_staging.staging_jobs(company_name);
CREATE INDEX IF NOT EXISTS staging_jobs_location_idx ON jobinsight_staging.staging_jobs(location);
CREATE INDEX IF NOT EXISTS staging_jobs_posted_time_idx ON jobinsight_staging.staging_jobs(posted_time);
CREATE INDEX IF NOT EXISTS staging_jobs_crawled_at_idx ON jobinsight_staging.staging_jobs(crawled_at);

COMMENT ON TABLE jobinsight_staging.staging_jobs IS 'Bảng Staging lưu dữ liệu từ TopCV';


INSERT INTO jobinsight_staging.staging_jobs(
	job_id,
	title,
    title_clean,
	job_url,
	company_name,
	company_url,
	salary,
	skills,
	location,
	location_detail,
	deadline,
	verified_employer,
	last_update,
	logo_url,
	posted_time,
	crawled_at,
	raw_data
)
SELECT
	job_id,
	title,
    title,
	job_url,
	company_name,
	company_url,
	salary,
	skills,
	location,
	location_detail,
	deadline,
	verified_employer,
	last_update,
	logo_url,
	posted_time,
	crawled_at,
	raw_data
FROM public.raw_jobs
ON CONFLICT (job_id) DO NOTHING;

