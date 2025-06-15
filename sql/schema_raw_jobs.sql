-- Tạo bảng raw_jobs để lưu dữ liệu thô từ TopCV
CREATE TABLE IF NOT EXISTS raw_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    job_url TEXT,
    company_name VARCHAR(200),
    company_url TEXT,
    salary VARCHAR(100),
    skills JSONB,
    location VARCHAR(100),
    location_detail TEXT,
    deadline VARCHAR(50),
    verified_employer BOOLEAN,
    last_update VARCHAR(100),
    logo_url TEXT,
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index để tăng tốc truy vấn
CREATE INDEX IF NOT EXISTS raw_jobs_company_idx ON raw_jobs(company_name);
CREATE INDEX IF NOT EXISTS raw_jobs_location_idx ON raw_jobs(location);
CREATE INDEX IF NOT EXISTS raw_jobs_posted_time_idx ON raw_jobs(posted_time);
CREATE INDEX IF NOT EXISTS raw_jobs_crawled_at_idx ON raw_jobs(crawled_at);

-- Comment
COMMENT ON TABLE raw_jobs IS 'Bảng lưu dữ liệu thô crawl từ TopCV';

