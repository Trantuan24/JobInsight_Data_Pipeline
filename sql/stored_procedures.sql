/* =======================================================================
   1. Hàm chuẩn hoá lương
   =======================================================================*/
DROP FUNCTION IF EXISTS jobinsight_staging.normalize_salary(text);

CREATE FUNCTION jobinsight_staging.normalize_salary(salary_text text)
RETURNS TABLE (
    salary_min  numeric,
    salary_max  numeric,
    salary_type varchar
)
LANGUAGE plpgsql
AS $$
DECLARE
    matches           text[];
    usd_exchange_rate numeric := 24000;          -- 1 USD ≈ 24 000 VND
BEGIN
    /* 1. Thoả thuận hoặc trống */
    IF salary_text IS NULL
       OR salary_text = ''
       OR lower(salary_text) = 'thoả thuận' THEN
        salary_min  := 0;
        salary_max  := 0;
        salary_type := 'negotiable';

    /* 2. Dạng "x - y USD" */
    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd' THEN
        matches     := regexp_matches(salary_text,
                       '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd', 'i');
        salary_min  := replace(matches[1], ',', '')::numeric
                       * usd_exchange_rate / 1e6;
        salary_max  := replace(matches[2], ',', '')::numeric
                       * usd_exchange_rate / 1e6;
        salary_type := 'range';

    /* 3. Dạng "x - y triệu" */
    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu' THEN
        matches     := regexp_matches(salary_text,
                       '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu', 'i');
        salary_min  := replace(matches[1], ',', '.')::numeric;
        salary_max  := replace(matches[2], ',', '.')::numeric;
        salary_type := 'range';

    /* 4. Dạng "tới x USD/triệu" */
    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*usd' THEN
        matches     := regexp_matches(salary_text,
                       'tới\s+([0-9,.]+)\s*usd', 'i');
        salary_min  := 0;
        salary_max  := replace(matches[1], ',', '')::numeric
                       * usd_exchange_rate / 1e6;
        salary_type := 'upto';

    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*triệu' THEN
        matches     := regexp_matches(salary_text,
                       'tới\s+([0-9,.]+)\s*triệu', 'i');
        salary_min  := 0;
        salary_max  := replace(matches[1], ',', '.')::numeric;
        salary_type := 'upto';

    /* 5. Dạng "từ x triệu" */
    ELSIF salary_text ~* 'từ\s+([0-9,.]+)\s*triệu' THEN
        matches     := regexp_matches(salary_text,
                       'từ\s+([0-9,.]+)\s*triệu', 'i');
        salary_min  := replace(matches[1], ',', '.')::numeric;
        salary_max  := salary_min;
        salary_type := 'from';

    /* 6. Chỉ 1 số USD/triệu (không có "-") */
    ELSIF salary_text ~* '([0-9,.]+)\s*usd'
          AND salary_text !~* '-' THEN
        matches     := regexp_matches(salary_text,
                       '([0-9,.]+)\s*usd', 'i');
        salary_min  := replace(matches[1], ',', '')::numeric
                       * usd_exchange_rate / 1e6;
        salary_max  := salary_min;
        salary_type := 'range';

    ELSIF salary_text ~* '([0-9,.]+)\s*triệu'
          AND salary_text !~* '-' THEN
        matches     := regexp_matches(salary_text,
                       '([0-9,.]+)\s*triệu', 'i');
        salary_min  := replace(matches[1], ',', '.')::numeric;
        salary_max  := salary_min;
        salary_type := 'range';

    /* 7. Trường hợp dữ liệu đặc biệt "0.0 - 0.0 triệu" */
    ELSIF salary_text = '0.0 - 0.0 triệu' THEN
        salary_min  := 0;
        salary_max  := 0;
        salary_type := 'negotiable';

    /* 8. Mặc định */
    ELSE
        salary_min  := 0;
        salary_max  := 0;
        salary_type := 'negotiable';
    END IF;

    /* Phòng ngừa NULL */
    salary_min := coalesce(salary_min, 0);
    salary_max := coalesce(NULLIF(salary_max, 0), salary_min);

    RETURN QUERY SELECT salary_min, salary_max, salary_type;
END;
$$;


/* =======================================================================
   2. Thêm cột cho bảng staging (nếu chưa có)
   =======================================================================*/
ALTER TABLE jobinsight_staging.staging_jobs
    ADD COLUMN IF NOT EXISTS salary_min    numeric,
    ADD COLUMN IF NOT EXISTS salary_max    numeric,
    ADD COLUMN IF NOT EXISTS salary_type   varchar,
    ADD COLUMN IF NOT EXISTS due_date      timestamptz,
    ADD COLUMN IF NOT EXISTS time_remaining text;


/* =======================================================================
   3. Cập nhật lương & hạn nộp
   =======================================================================*/

/* 3.1. Tính lại lương */
UPDATE jobinsight_staging.staging_jobs
SET (salary_min, salary_max, salary_type) = (n.salary_min, n.salary_max, n.salary_type)
FROM jobinsight_staging.staging_jobs AS s
CROSS JOIN jobinsight_staging.normalize_salary(s.salary) AS n
WHERE jobinsight_staging.staging_jobs.job_id = s.job_id
  AND (jobinsight_staging.staging_jobs.salary_min, jobinsight_staging.staging_jobs.salary_max, jobinsight_staging.staging_jobs.salary_type) 
      IS DISTINCT FROM (n.salary_min, n.salary_max, n.salary_type);

/* 3.2. Xác định due_date từ "deadline (số ngày)" nếu chưa có */
UPDATE jobinsight_staging.staging_jobs
SET    due_date = crawled_at + (deadline || ' days')::interval
WHERE  due_date IS NULL;


/* =======================================================================
   4. Thủ tục cập nhật time_remaining
   =======================================================================*/
DROP PROCEDURE IF EXISTS jobinsight_staging.update_deadline();

CREATE PROCEDURE jobinsight_staging.update_deadline()
LANGUAGE plpgsql
AS $$
BEGIN
    /* > 1 ngày */
    UPDATE jobinsight_staging.staging_jobs
    SET    time_remaining = 'Còn '
           || EXTRACT(day FROM (due_date - CURRENT_TIMESTAMP))::int
           || ' ngày để ứng tuyển'
    WHERE  due_date > CURRENT_TIMESTAMP
      AND  due_date - CURRENT_TIMESTAMP >= INTERVAL '1 day';

    /* 1 giờ – < 1 ngày */
    UPDATE jobinsight_staging.staging_jobs
    SET    time_remaining = 'Còn '
           || EXTRACT(hour FROM (due_date - CURRENT_TIMESTAMP))::int
           || ' giờ để ứng tuyển'
    WHERE  due_date > CURRENT_TIMESTAMP
      AND  due_date - CURRENT_TIMESTAMP >= INTERVAL '1 hour'
      AND  due_date - CURRENT_TIMESTAMP <  INTERVAL '1 day';

    /* 1 phút – < 1 giờ */
    UPDATE jobinsight_staging.staging_jobs
    SET    time_remaining = 'Còn '
           || EXTRACT(minute FROM (due_date - CURRENT_TIMESTAMP))::int
           || ' phút để ứng tuyển'
    WHERE  due_date > CURRENT_TIMESTAMP
      AND  due_date - CURRENT_TIMESTAMP >= INTERVAL '1 minute'
      AND  due_date - CURRENT_TIMESTAMP <  INTERVAL '1 hour';

    /* < 1 phút */
    UPDATE jobinsight_staging.staging_jobs
    SET    time_remaining = 'Còn '
           || EXTRACT(second FROM (due_date - CURRENT_TIMESTAMP))::int
           || ' giây để ứng tuyển'
    WHERE  due_date > CURRENT_TIMESTAMP
      AND  due_date - CURRENT_TIMESTAMP < INTERVAL '1 minute';

    /* Hết hạn */
    UPDATE jobinsight_staging.staging_jobs
    SET    time_remaining = 'Đã hết thời gian ứng tuyển'
    WHERE  due_date <= CURRENT_TIMESTAMP;
END;
$$;


/* =======================================================================
   5. Gọi thủ tục ngay lần đầu
   =======================================================================*/
CALL jobinsight_staging.update_deadline();
