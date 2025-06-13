-- Script khởi tạo cho JobInsight database
-- Lưu ý: Database và user jobinsight đã được tạo trong script create_user.sql

-- Kết nối vào database jobinsight
\c jobinsight;

-- Log bắt đầu khởi tạo
\echo 'Starting initialization of JobInsight database'

-- Chạy các script tạo bảng và procedure
-- Các scripts được mount riêng biệt trong docker-compose và sẽ chạy theo thứ tự:
-- 02-schema_raw_jobs.sql 
-- 03-schema_staging.sql
-- 04-stored_procedures.sql 
-- 05-schema_dwh.sql
-- 06-views.sql

-- Log hoàn thành
\echo 'Initialization of JobInsight database completed' 