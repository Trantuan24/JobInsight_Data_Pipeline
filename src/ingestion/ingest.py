#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import logging
import sys
from datetime import datetime
import pandas as pd
from psycopg2 import extras
import numpy as np

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "db_ingest.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import modules từ utils
try:
    from src.utils.config import DB_CONFIG, RAW_JOBS_TABLE, RAW_BATCH_SIZE
    from src.utils.db import get_connection, execute_query, table_exists
except ImportError as e:
    # Thử cách thay thế
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
    from utils.config import DB_CONFIG, RAW_JOBS_TABLE, RAW_BATCH_SIZE
    from utils.db import get_connection, execute_query, table_exists

# Đường dẫn đến thư mục SQL
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.getcwd(), "sql")
    os.makedirs(SQL_DIR, exist_ok=True)

def execute_sql_file(sql_file_path):
    """Thực thi các lệnh SQL từ file"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_script)
                conn.commit()
        
        logger.info(f"Đã thực thi thành công file SQL: {sql_file_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thực thi file SQL {sql_file_path}: {str(e)}")
        return False

def setup_database_schema():
    """Thiết lập schema cho database từ các file SQL"""
    try:
        # Kiểm tra bảng raw_jobs
        if table_exists(RAW_JOBS_TABLE):
            logger.info(f"Bảng {RAW_JOBS_TABLE} đã tồn tại")
            return True
        
        # Tạo bảng raw_jobs nếu chưa tồn tại
        raw_jobs_schema = os.path.join(SQL_DIR, "schema_raw_jobs.sql")
        if os.path.exists(raw_jobs_schema):
            if not execute_sql_file(raw_jobs_schema):
                logger.error("Không thể thiết lập bảng raw_jobs!")
                return False
        else:
            logger.error(f"Không tìm thấy file schema: {raw_jobs_schema}")
            return False
        
        logger.info("Đã thiết lập schema database thành công!")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thiết lập schema database: {str(e)}")
        return False

def dataframe_to_records(df):
    """Chuyển đổi DataFrame thành danh sách các bản ghi để insert vào database"""
    if df.empty:
        logger.warning("DataFrame trống, không có dữ liệu để xử lý")
        return []
    
    processed_jobs = []
    try: 
        for _, row in df.iterrows():
            job_dict = row.to_dict()
            
            # Bỏ qua nếu không có job_id
            if 'job_id' not in job_dict or not job_dict['job_id']:
                continue
            
            # Xử lý skills thành JSON
            if 'skills' in job_dict:
                if isinstance(job_dict['skills'], list):
                    job_dict['skills'] = json.dumps(job_dict['skills'])
                elif isinstance(job_dict['skills'], str) and not job_dict['skills'].startswith('['):
                    job_dict['skills'] = json.dumps([job_dict['skills']])
            else:
                job_dict['skills'] = json.dumps([])
            
            # Xử lý posted_time và crawled_at - xử lý NaN và các giá trị không hợp lệ
            if 'posted_time' in job_dict:
                if job_dict['posted_time'] is None or pd.isna(job_dict['posted_time']) or job_dict['posted_time'] == '':
                    job_dict['posted_time'] = None
            else:
                job_dict['posted_time'] = None
                
            if 'crawled_at' in job_dict:
                if job_dict['crawled_at'] is None or pd.isna(job_dict['crawled_at']) or job_dict['crawled_at'] == '':
                    job_dict['crawled_at'] = datetime.now()
            else:
                job_dict['crawled_at'] = datetime.now()
            
            # Đảm bảo raw_data là JSON
            if 'raw_data' not in job_dict:
                job_dict['raw_data'] = json.dumps(job_dict)
            elif not isinstance(job_dict['raw_data'], str):
                job_dict['raw_data'] = json.dumps(job_dict['raw_data'])
                
            processed_jobs.append(job_dict)
            
        logger.info(f"Đã chuyển đổi {len(processed_jobs)} bản ghi từ DataFrame")
        return processed_jobs
    except Exception as e:
        logger.error(f"Lỗi khi chuyển đổi DataFrame: {str(e)}")
        return []

def upsert_job_data(job_data_list):
    """Insert hoặc update dữ liệu vào bảng raw_jobs"""
    if not job_data_list:
        logger.warning("Không có dữ liệu để insert")
        return 0
    
    inserted_count = 0
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Chuẩn bị dữ liệu
            for job in job_data_list:
                # Đảm bảo raw_data luôn tồn tại
                if 'raw_data' not in job:
                    job['raw_data'] = json.dumps(job)
                elif not isinstance(job['raw_data'], str):
                    job['raw_data'] = json.dumps(job['raw_data'])
            
            # Lấy danh sách cột của bảng raw_jobs
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'raw_jobs' AND table_schema = 'public'
            """)
            db_columns = [row[0] for row in cursor.fetchall()]
            
            # Lọc các cột hiện có
            sample_job = job_data_list[0]
            available_columns = [col for col in db_columns if col in sample_job and col != 'last_updated']
            
            # Tạo query
            columns_str = ", ".join(available_columns)
            placeholders = ", ".join([f"%({col})s" for col in available_columns])
            
            # Sửa cách tạo update_set để giữ lại posted_time cũ nếu giá trị mới là null
            update_set = []
            for col in available_columns:
                if col != 'job_id':
                    if col == 'posted_time':
                        # Luôn giữ giá trị posted_time cũ, không bao giờ cập nhật
                        update_set.append(f"{col} = {RAW_JOBS_TABLE}.{col}")
                    else:
                        update_set.append(f"{col} = EXCLUDED.{col}")
            
            update_set = ", ".join(update_set)
            
            # Thêm cập nhật last_updated nếu có
            if 'last_updated' in db_columns:
                update_set += ", last_updated = CURRENT_TIMESTAMP"
            
            # Tạo câu query UPSERT
            insert_query = f"""
            INSERT INTO raw_jobs ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (job_id) DO UPDATE SET {update_set}
            """
            
            # Thực hiện insert theo batch
            batch_size = RAW_BATCH_SIZE
            total_jobs = len(job_data_list)
            
            for i in range(0, total_jobs, batch_size):
                batch = job_data_list[i:i+batch_size]
                try:
                    cursor.executemany(insert_query, batch)
                    conn.commit()
                    inserted_count += len(batch)
                    logger.info(f"Đã insert/update batch {i//batch_size + 1}: {len(batch)} bản ghi")
                except Exception as e:
                    logger.error(f"Lỗi khi insert batch {i//batch_size + 1}: {str(e)}")
                    conn.rollback()
            
            logger.info(f"Tổng cộng đã insert/update thành công {inserted_count} bản ghi")
    except Exception as e:
        logger.error(f"Lỗi khi insert dữ liệu: {str(e)}")
    
    return inserted_count

def ingest_dataframe(df):
    """Nhận DataFrame và import vào database"""
    try:
        logger.info(f"Bắt đầu import DataFrame với {len(df)} bản ghi")
        
        # Đảm bảo database và schema đã được thiết lập
        if not setup_database_schema():
            logger.error("Không thể thiết lập database schema. Dừng quá trình import!")
            return 0
        
        # Chuyển đổi DataFrame thành danh sách bản ghi
        records = dataframe_to_records(df)
        
        # Insert dữ liệu vào database
        inserted_count = upsert_job_data(records)
        
        logger.info(f"Hoàn thành import DataFrame: {inserted_count}/{len(df)} bản ghi")
        return inserted_count
    except Exception as e:
        logger.error(f"Lỗi khi import DataFrame: {str(e)}")
        return 0

def run_crawler(num_pages=1, keywords=None):
    """Chạy crawler và trả về DataFrame kết quả"""
    try:
        logger.info("Đang import crawler module...")
        try:
            # Thử import crawler từ src.crawler
            from src.crawler.crawler import crawl_multiple_keywords
        except ImportError:
            # Thử import crawler trực tiếp
            sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
            from crawler.crawler import crawl_multiple_keywords
        
        logger.info(f"Bắt đầu crawl dữ liệu từ TopCV (pages={num_pages}, keywords={keywords or 'default'})...")
        df_jobs = crawl_multiple_keywords(num_pages=num_pages, keywords=keywords)
        
        if df_jobs is None or df_jobs.empty:
            logger.warning("Crawler không tìm thấy dữ liệu nào!")
            return None
            
        logger.info(f"Crawler đã thu thập được {len(df_jobs)} job listings")
        return df_jobs
    except Exception as e:
        logger.error(f"Lỗi khi chạy crawler: {str(e)}")
        return None

def main():
    """Hàm chính để ingest dữ liệu từ crawler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import dữ liệu vào database từ crawler')
    parser.add_argument('--pages', '-p', type=int, default=1, help='Số trang cần crawl (mặc định: 1)')
    parser.add_argument('--keywords', '-k', nargs='+', help='Danh sách từ khóa tìm kiếm')
    args = parser.parse_args()
    
    # Đảm bảo schema đã được thiết lập
    if not setup_database_schema():
        logger.error("Không thể thiết lập database schema. Dừng chương trình!")
        return
    
    # Chạy crawler với số trang và từ khóa được chỉ định
    logger.info("Bắt đầu crawl và import dữ liệu...")
    df = run_crawler(num_pages=args.pages, keywords=args.keywords)
    
    if df is not None and not df.empty:
        imported_count = ingest_dataframe(df)
        if imported_count > 0:
            logger.info(f"Import thành công {imported_count} bản ghi từ crawler!")
        else:
            logger.error("Không import được dữ liệu từ crawler vào database!")
    else:
        logger.error("Không thu thập được dữ liệu từ crawler!")

if __name__ == "__main__":
    main()

