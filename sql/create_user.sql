-- Script tạo user và database cho JobInsight
-- Được chạy đầu tiên trong quá trình khởi tạo container PostgreSQL

-- Tạo user jobinsight nếu chưa tồn tại
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_user
        WHERE usename = 'jobinsight'
    ) THEN
        CREATE USER jobinsight WITH PASSWORD 'jobinsight';
    END IF;
END
$$;

-- Cấp quyền cho user
ALTER ROLE jobinsight WITH LOGIN CREATEDB;

-- Tạo database jobinsight nếu chưa tồn tại
CREATE DATABASE jobinsight
    WITH 
    OWNER = jobinsight
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TEMPLATE = template0
    CONNECTION LIMIT = -1;

-- Kết nối tới database jobinsight
\c jobinsight;

-- Cấp tất cả quyền cho user jobinsight trên database jobinsight
GRANT ALL PRIVILEGES ON DATABASE jobinsight TO jobinsight;

-- Tạo extension nếu cần
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Đảm bảo user jobinsight là owner của schema public
ALTER SCHEMA public OWNER TO jobinsight;

-- Đảm bảo quyền mặc định
GRANT ALL ON SCHEMA public TO jobinsight;
GRANT USAGE ON SCHEMA public TO jobinsight;

-- Output thông báo hoàn thành
\echo 'JobInsight database và user đã được tạo thành công' 