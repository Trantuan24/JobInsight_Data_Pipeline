-- Hàm chuyển đổi salary_text thành salary_min, salary_max và salary_type
CREATE OR REPLACE FUNCTION normalize_salary(salary_text VARCHAR)
RETURNS TABLE(
    salary_min FLOAT,
    salary_max FLOAT,
    salary_type VARCHAR
) AS $$
DECLARE
    matches TEXT[];
    usd_exchange_rate FLOAT := 24000;
BEGIN
    IF salary_text IS NULL OR salary_text = '' OR lower(salary_text) = 'thoả thuận' THEN
        salary_min := 0;
        salary_max := 0;
        salary_type := 'negotiable';

    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd', 'i');
        salary_min := replace(matches[1], ',', '')::FLOAT * usd_exchange_rate / 1000000;
        salary_max := replace(matches[2], ',', '')::FLOAT * usd_exchange_rate / 1000000;
        salary_type := 'range';

    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::FLOAT;
        salary_max := replace(matches[2], ',', '.')::FLOAT;
        salary_type := 'range';

    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*usd' THEN
        matches := regexp_matches(salary_text, 'tới\s+([0-9,.]+)\s*usd', 'i');
        salary_min := 0;
        salary_max := replace(matches[1], ',', '')::FLOAT * usd_exchange_rate / 1000000;
        salary_type := 'upto';

    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, 'tới\s+([0-9,.]+)\s*triệu', 'i');
        salary_min := 0;
        salary_max := replace(matches[1], ',', '.')::FLOAT;
        salary_type := 'upto';

    ELSIF salary_text ~* 'từ\s+([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, 'từ\s+([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::FLOAT;
        salary_max := salary_min;
        salary_type := 'from';

    ELSIF salary_text ~* '([0-9,.]+)\s*usd' AND salary_text !~* '-' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*usd', 'i');
        salary_min := replace(matches[1], ',', '')::FLOAT * usd_exchange_rate / 1000000;
        salary_max := salary_min;
        salary_type := 'range';

    ELSIF salary_text ~* '([0-9,.]+)\s*triệu' AND salary_text !~* '-' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::FLOAT;
        salary_max := salary_min;
        salary_type := 'range';

    ELSIF salary_text = '0.0 - 0.0 triệu' THEN
        salary_min := 0;
        salary_max := 0;
        salary_type := 'negotiable';

    ELSE
        salary_min := 0;
        salary_max := 0;
        salary_type := 'negotiable';
    END IF;

    IF salary_min IS NULL THEN
        salary_min := 0;
    END IF;

    IF salary_max IS NULL OR salary_max = 0 THEN
        salary_max := salary_min;
    END IF;

    RETURN QUERY SELECT salary_min, salary_max, salary_type;
END;
$$ LANGUAGE plpgsql;


-- Thêm các cột nếu chưa tồn tại trong bảng staging
ALTER TABLE jobinsight_staging.staging_jobs ADD COLUMN IF NOT EXISTS salary_min FLOAT;
ALTER TABLE jobinsight_staging.staging_jobs ADD COLUMN IF NOT EXISTS salary_max FLOAT;
ALTER TABLE jobinsight_staging.staging_jobs ADD COLUMN IF NOT EXISTS salary_type VARCHAR;

ALTER TABLE jobinsight_staging.staging_jobs ADD COLUMN IF NOT EXISTS due_date TIMESTAMP WITH TIME ZONE;
ALTER TABLE jobinsight_staging.staging_jobs ADD COLUMN IF NOT EXISTS time_remaining TEXT;


-- Cập nhật giá trị salary_min, salary_max, salary_type trong staging
UPDATE jobinsight_staging.staging_jobs
SET (salary_min, salary_max, salary_type) = (
    SELECT n.salary_min, n.salary_max, n.salary_type
    FROM normalize_salary(jobinsight_staging.staging_jobs.salary) AS n
);


-- Cập nhật cột due_date (thời hạn thực tế)
UPDATE jobinsight_staging.staging_jobs
SET due_date = crawled_at + (deadline || ' days')::interval
WHERE due_date IS NULL;


-- Tạo thủ tục cập nhật thời gian còn lại
CREATE OR REPLACE PROCEDURE update_deadline()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(DAY FROM (due_date - CURRENT_TIMESTAMP))::INT || ' ngày để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 day';

    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(HOUR FROM (due_date - CURRENT_TIMESTAMP))::INT || ' giờ để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 hour' AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 day';

    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(MINUTE FROM (due_date - CURRENT_TIMESTAMP))::INT || ' phút để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 minute' AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 hour';

    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(SECOND FROM (due_date - CURRENT_TIMESTAMP))::INT || ' giây để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 minute';

    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Đã hết thời gian ứng tuyển'
    WHERE due_date <= CURRENT_TIMESTAMP;
END;
$$;

-- Gọi thủ tục cập nhật deadline
CALL update_deadline();
