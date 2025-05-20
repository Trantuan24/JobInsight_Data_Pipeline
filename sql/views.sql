-- View để lấy top 10 việc làm tại Hà Nội với mức lương 10-15 triệu, deadline gần nhất
CREATE OR REPLACE VIEW vw_top10_hanoi_jobs AS
SELECT 
    pj.job_id,
    pj.title,
    pj.company_name,
    pj.location,
    pj.salary_min,
    pj.salary_max,
    pj.deadline,
    pj.days_to_deadline,
    pj.posting_date,
    pj.job_type,
    pj.experience,
    pj.job_level,
    pj.job_url,
    pj.company_url,
    pj.skills
FROM 
    processed_jobs pj
WHERE 
    pj.is_expired = FALSE
    AND pj.city ILIKE '%hà nội%'
    AND pj.salary_min >= 10000000
    AND pj.salary_max <= 15000000
    AND pj.days_to_deadline > 0
ORDER BY 
    pj.days_to_deadline ASC
LIMIT 10;

-- View để phân tích phân bố công việc theo kinh nghiệm yêu cầu
CREATE OR REPLACE VIEW vw_job_experience_distribution AS
SELECT 
    experience,
    COUNT(*) as job_count,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM processed_jobs)), 2) as percentage
FROM 
    processed_jobs
GROUP BY 
    experience
ORDER BY 
    job_count DESC;

-- View để phân tích phân bố công việc theo vị trí
CREATE OR REPLACE VIEW vw_job_location_distribution AS
SELECT 
    city,
    COUNT(*) as job_count,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM processed_jobs WHERE city IS NOT NULL)), 2) as percentage
FROM 
    processed_jobs
WHERE 
    city IS NOT NULL
GROUP BY 
    city
ORDER BY 
    job_count DESC;

-- View để phân tích mức lương trung bình theo kinh nghiệm
CREATE OR REPLACE VIEW vw_avg_salary_by_experience AS
SELECT 
    experience,
    ROUND(AVG(salary_min)) as avg_salary_min,
    ROUND(AVG(salary_max)) as avg_salary_max,
    ROUND(AVG((salary_min + salary_max) / 2)) as avg_salary,
    COUNT(*) as job_count
FROM 
    processed_jobs
WHERE 
    salary_min > 0 AND salary_max > 0
GROUP BY 
    experience
ORDER BY 
    avg_salary DESC;
