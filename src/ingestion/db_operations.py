import json
import pandas as pd
from typing import Dict, Any, List, Tuple
import os
import sys

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.db import get_connection, execute_sql_file
    from src.utils.config import RAW_BATCH_SIZE
    # Loại bỏ import từ cdc để tránh circular import
    # from src.ingestion.cdc import save_cdc_record
except ImportError as e:
    # Thử cách thay thế
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from utils.db import get_connection, execute_sql_file
    from utils.config import RAW_BATCH_SIZE
    # Loại bỏ import từ cdc để tránh circular import
    # from cdc import save_cdc_record

logger = get_logger("ingestion.db_operations")

# Constants
BATCH_SIZE = RAW_BATCH_SIZE if 'RAW_BATCH_SIZE' in dir() else 1000  # Insert theo batch để tối ưu performance

def insert_record(record, batch_size=1):
    """Chèn hoặc cập nhật một record vào database"""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Sử dụng UPSERT pattern của PostgreSQL (ON CONFLICT DO UPDATE)
            upsert_query = """
            INSERT INTO raw_jobs (
                job_id, title, job_url, company_name, company_url,
                salary, skills, location, location_detail, deadline,
                verified_employer, last_update, logo_url, posted_time, crawled_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                title = EXCLUDED.title,
                job_url = EXCLUDED.job_url,
                company_name = EXCLUDED.company_name,
                company_url = EXCLUDED.company_url,
                salary = EXCLUDED.salary,
                skills = EXCLUDED.skills,
                location = EXCLUDED.location,
                location_detail = EXCLUDED.location_detail,
                deadline = EXCLUDED.deadline,
                verified_employer = EXCLUDED.verified_employer,
                last_update = EXCLUDED.last_update,
                logo_url = EXCLUDED.logo_url,
                posted_time = CASE 
                    WHEN raw_jobs.posted_time IS NULL THEN EXCLUDED.posted_time
                    ELSE raw_jobs.posted_time
                END,
                crawled_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
            """
            
            cursor.execute(upsert_query, (
                record['job_id'],
                record['title'],
                record['job_url'],
                record['company_name'],
                record['company_url'],
                record['salary'],
                json.dumps(record['skills']) if record['skills'] else None,
                record['location'],
                record['location_detail'],
                record['deadline'],
                record['verified_employer'],
                record['last_update'],
                record['logo_url'],
                record['posted_time'],
                record['crawled_at']
            ))
            
            # Kiểm tra kết quả để biết là insert hay update
            result = cursor.fetchone()
            is_insert = result and result[0]
            action = 'insert_single' if is_insert else 'update_single'
            
            conn.commit()
            
            # Log CDC sẽ được xử lý bởi caller
            return 'inserted' if is_insert else 'updated'
                
    except Exception as e:
        logger.error(f"Error inserting record {record['job_id']}: {str(e)}")
        raise

def batch_insert_records(records, batch_size=10) -> Tuple[int, int, int]:
    """Batch insert records vào database"""
    inserted = 0
    updated = 0
    errors = 0
    
    # Validate batch size
    if batch_size <= 0:
        batch_size = 1
    elif batch_size > 100:
        batch_size = 100
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Lấy danh sách job_id để kiểm tra tồn tại
            all_job_ids = [record['job_id'] for record in records]
            
            # Kiểm tra job_id nào đã tồn tại
            if all_job_ids:
                existing_query = """
                SELECT job_id FROM raw_jobs 
                WHERE job_id = ANY(%s)
                """
                cursor.execute(existing_query, (all_job_ids,))
                existing_job_ids = {row[0] for row in cursor.fetchall()}
            else:
                existing_job_ids = set()
            
            # Tạo batches để xử lý
            batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]
            
            # Sử dụng UPSERT pattern của PostgreSQL (ON CONFLICT DO UPDATE)
            upsert_query = """
            INSERT INTO raw_jobs (
                job_id, title, job_url, company_name, company_url,
                salary, skills, location, location_detail, deadline,
                verified_employer, last_update, logo_url, posted_time, crawled_at
            ) VALUES %s
            ON CONFLICT (job_id) DO UPDATE SET
                title = EXCLUDED.title,
                job_url = EXCLUDED.job_url,
                company_name = EXCLUDED.company_name,
                company_url = EXCLUDED.company_url,
                salary = EXCLUDED.salary,
                skills = EXCLUDED.skills,
                location = EXCLUDED.location,
                location_detail = EXCLUDED.location_detail,
                deadline = EXCLUDED.deadline,
                verified_employer = EXCLUDED.verified_employer,
                last_update = EXCLUDED.last_update,
                logo_url = EXCLUDED.logo_url,
                posted_time = CASE 
                    WHEN raw_jobs.posted_time IS NULL THEN EXCLUDED.posted_time
                    ELSE raw_jobs.posted_time
                END,
                crawled_at = CURRENT_TIMESTAMP
            """
            
            for batch in batches:
                try:
                    # Chuẩn bị dữ liệu cho batch
                    batch_data = []
                    for record in batch:
                        batch_data.append((
                            record['job_id'],
                            record['title'],
                            record['job_url'],
                            record['company_name'],
                            record['company_url'],
                            record['salary'],
                            json.dumps(record['skills']) if record['skills'] else None,
                            record['location'],
                            record['location_detail'],
                            record['deadline'],
                            record['verified_employer'],
                            record['last_update'],
                            record['logo_url'],
                            record['posted_time'],
                            record['crawled_at']
                        ))
                    
                    # Thực hiện batch upsert với execute_values
                    from psycopg2.extras import execute_values
                    execute_values(cursor, upsert_query, batch_data, template=None, page_size=batch_size)
                    
                    # Count inserts and updates
                    for record in batch:
                        job_id = record['job_id']
                        if job_id in existing_job_ids:
                            updated += 1
                        else:
                            inserted += 1
                    
                    # Commit sau mỗi batch
                    conn.commit()
                    logger.debug(f"Đã xử lý batch với {len(batch)} records")
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý batch: {str(e)}")
                    errors += 1
                    # Tiếp tục với batch tiếp theo
    
    except Exception as e:
        logger.error(f"Lỗi khi batch insert: {str(e)}")
        errors += 1
    
    return inserted, updated, errors

def ensure_table_exists():
    """Đảm bảo bảng raw_jobs tồn tại với schema đúng"""
    # Đọc schema từ file SQL
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    sql_schema_file = os.path.join(project_root, "sql", "schema_raw_jobs.sql")
    
    if not os.path.exists(sql_schema_file):
        logger.warning(f"File schema không tồn tại: {sql_schema_file}. Sử dụng schema mặc định.")
        # Fallback nếu không tìm thấy file schema
        create_table_query = """
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
        
        -- Thêm các index cần thiết
        CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON raw_jobs(company_name);
        CREATE INDEX IF NOT EXISTS idx_raw_jobs_location ON raw_jobs(location);
        CREATE INDEX IF NOT EXISTS idx_raw_jobs_posted_time ON raw_jobs(posted_time);
        CREATE INDEX IF NOT EXISTS idx_raw_jobs_crawled_at ON raw_jobs(crawled_at);
        """
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
    else:
        # Sử dụng hàm execute_sql_file để thực thi file SQL
        execute_sql_file(sql_schema_file)
        
        # Thêm migration script để đảm bảo tương thích với schema cũ
        migration_query = """
        -- Migration script để thêm các cột mới nếu chưa có
        DO $$
        BEGIN
            -- Các cột bắt buộc phải có cho structure mới
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                        WHERE table_name='raw_jobs' AND column_name='location_detail') THEN
                ALTER TABLE raw_jobs ADD COLUMN location_detail TEXT;
            END IF;
            
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                        WHERE table_name='raw_jobs' AND column_name='verified_employer') THEN
                ALTER TABLE raw_jobs ADD COLUMN verified_employer BOOLEAN DEFAULT FALSE;
            END IF;
        END
        $$;
        """
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(migration_query)
            conn.commit()
            
        logger.info(f"Đã tạo/cập nhật bảng raw_jobs từ file schema: {sql_schema_file}") 