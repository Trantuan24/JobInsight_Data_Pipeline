-- Tạo database nếu chưa tồn tại
SELECT 'CREATE DATABASE jobinsight' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'jobinsight')\gexec

-- Kết nối vào database
\c jobinsight;

-- Chạy các script tạo bảng và procedure
\i /docker-entrypoint-initdb.d/schema_raw_jobs.sql
\i /docker-entrypoint-initdb.d/processed_jobs.sql
\i /docker-entrypoint-initdb.d/stored_procedures.sql
\i /docker-entrypoint-initdb.d/views.sql 