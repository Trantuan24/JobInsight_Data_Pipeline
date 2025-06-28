-- JobInsight Analysis Queries
-- Tập hợp các truy vấn phân tích dữ liệu từ kho dữ liệu

-- 1. View lọc job có mức lương 10-15 triệu
CREATE OR REPLACE VIEW vw_job_salary_filter AS
SELECT 
    j.job_sk,
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.salary_type,
    f.due_date,
    f.posted_time,
    l.city,
    l.province,
    l.district,
    CASE 
        WHEN f.due_date IS NOT NULL THEN 
            EXTRACT(DAYS FROM (f.due_date - CURRENT_DATE))
        ELSE NULL
    END as days_to_deadline,
    j.skills
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk AND l.is_current = TRUE
JOIN DimDate d ON f.date_id = d.date_id
WHERE 
    f.salary_min >= 10000000 
    AND f.salary_max <= 15000000
    AND (f.due_date IS NULL OR f.due_date >= CURRENT_DATE);

-- 2. Top 10 công việc lương 10-15 triệu tại Hà Nội
CREATE OR REPLACE VIEW vw_top10_hn AS
SELECT *
FROM vw_job_salary_filter
WHERE city = 'Hà Nội' OR province = 'Hà Nội'
ORDER BY days_to_deadline, due_date
LIMIT 10;

-- 3. Phân bố công việc theo thành phố
CREATE OR REPLACE VIEW vw_city_distribution AS
SELECT 
    COALESCE(city, 'Unknown') as city,
    COUNT(*) as job_count,
    ROUND(AVG(salary_min), 0) as avg_min_salary,
    ROUND(AVG(salary_max), 0) as avg_max_salary
FROM vw_job_salary_filter
GROUP BY city
ORDER BY job_count DESC;

-- 4. Phân bố công việc theo thời gian deadline
CREATE OR REPLACE VIEW vw_deadline_distribution AS
SELECT 
    CASE 
        WHEN days_to_deadline <= 7 THEN '1-7 ngày'
        WHEN days_to_deadline <= 14 THEN '8-14 ngày'
        WHEN days_to_deadline <= 30 THEN '15-30 ngày'
        ELSE '>30 ngày'
    END as deadline_range,
    COUNT(*) as job_count
FROM vw_job_salary_filter
WHERE days_to_deadline IS NOT NULL
GROUP BY deadline_range
ORDER BY 
    CASE deadline_range
        WHEN '1-7 ngày' THEN 1
        WHEN '8-14 ngày' THEN 2
        WHEN '15-30 ngày' THEN 3
        ELSE 4
    END;

-- 5. Phân tích kỹ năng phổ biến nhất
CREATE OR REPLACE VIEW vw_popular_skills AS
WITH skill_data AS (
    SELECT 
        j.job_sk,
        json_array_elements_text(j.skills::json) as skill
    FROM vw_job_salary_filter j
    WHERE j.skills IS NOT NULL
)
SELECT 
    skill,
    COUNT(*) as job_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT job_sk) FROM skill_data), 1) as percentage
FROM skill_data
GROUP BY skill
HAVING COUNT(*) > 2
ORDER BY job_count DESC;

-- 6. Công ty đăng tuyển nhiều nhất
CREATE OR REPLACE VIEW vw_top_companies AS
SELECT 
    company_name_standardized,
    verified_employer,
    COUNT(*) as job_count,
    ROUND(AVG(salary_min), 0) as avg_min_salary,
    ROUND(AVG(salary_max), 0) as avg_max_salary
FROM vw_job_salary_filter
GROUP BY company_name_standardized, verified_employer
ORDER BY job_count DESC
LIMIT 20;

-- 7. Phân tích xu hướng theo tháng
CREATE OR REPLACE VIEW vw_monthly_trends AS
SELECT 
    TO_CHAR(posted_time, 'YYYY-MM') as month,
    COUNT(*) as job_count,
    ROUND(AVG(salary_min), 0) as avg_min_salary,
    ROUND(AVG(salary_max), 0) as avg_max_salary
FROM vw_job_salary_filter
WHERE posted_time IS NOT NULL
GROUP BY TO_CHAR(posted_time, 'YYYY-MM')
ORDER BY month DESC;
