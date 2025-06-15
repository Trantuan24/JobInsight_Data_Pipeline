-- Chèn dữ liệu từ raw_jobs vào staging_jobs
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
    crawled_at
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
    crawled_at
FROM public.raw_jobs
ON CONFLICT (job_id) DO NOTHING; 