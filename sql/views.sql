-- View để lọc job theo lương 10-15 triệu và bỏ job đã hết hạn
CREATE OR REPLACE VIEW vw_job_salary_filter AS
SELECT DISTINCT
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.posted_time,
    f.time_remaining,
    j.job_url,
    j.skills,
    l.province,
    l.city,
    l.district
FROM 
    FactJobPostingDaily f
    INNER JOIN DimJob j ON f.job_sk = j.job_sk
    INNER JOIN DimCompany c ON f.company_sk = c.company_sk
    LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
    LEFT JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE 
    j.is_current = TRUE
    AND c.is_current = TRUE
    AND (l.is_current = TRUE OR l.is_current IS NULL)
    AND f.salary_min >= 10.00
    AND f.salary_max <= 20.00
    AND f.due_date >= CURRENT_DATE  -- Bỏ job đã hết hạn
    AND f.salary_min IS NOT NULL
    AND f.salary_max IS NOT NULL;

-- View để lấy top 10 việc làm tại Hà Nội với mức lương 10-15 triệu, sắp xếp theo deadline
CREATE OR REPLACE VIEW vw_top10_hn AS
SELECT 
    job_id,
    title_clean,
    company_name_standardized,
    salary_min,
    salary_max,
    due_date,
    time_remaining,
    posted_time,
    job_url,
    skills,
    city,
    province,
    EXTRACT(DAYS FROM (due_date - CURRENT_DATE)) as days_to_deadline
FROM 
    vw_job_salary_filter
WHERE 
    (city ILIKE '%hà nội%' OR city ILIKE '%hanoi%' OR province ILIKE '%hà nội%')
ORDER BY 
    due_date ASC  -- Sắp xếp theo deadline gần nhất
LIMIT 10;