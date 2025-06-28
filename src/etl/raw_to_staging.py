"""
ETL process for JobInsight data

Luồng ETL:
1. Dữ liệu đã được load từ raw_jobs vào staging_jobs bằng SQL
2. Stored procedures xử lý cơ bản salary, deadline đã được thực thi
3. Script này xử lý thêm các trường phức tạp bằng pandas và cập nhật lại staging_jobs

- Input: staging_jobs table (đã có dữ liệu cơ bản và một số trường đã được xử lý bằng SQL)
- Output: staging_jobs table (cập nhật thêm các trường đã xử lý bằng pandas)
"""

import os
import json
import logging
import sys
from datetime import datetime
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "processing.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import modules từ utils
from src.utils.config import DB_CONFIG, RAW_JOBS_TABLE, DWH_STAGING_SCHEMA, STAGING_JOBS_TABLE, RAW_BATCH_SIZE
from src.utils.db import get_connection, execute_query, table_exists, get_dataframe, execute_stored_procedure, execute_sql_file
from src.processing.data_processing import clean_title, clean_company_name, extract_location_info, refine_location

# Đường dẫn đến thư mục SQL
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.getcwd(), "sql")
    os.makedirs(SQL_DIR, exist_ok=True)

def setup_database_schema():
    """Thiết lập schema và bảng"""
    try:
        # Kiểm tra và tạo schema nếu chưa tồn tại
        with get_connection() as conn:
            with conn.cursor() as cursor:
                # Kiểm tra schema đã tồn tại chưa
                cursor.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = %s
                );
                """, (DWH_STAGING_SCHEMA,))
                schema_exists = cursor.fetchone()[0]
                
                if not schema_exists:
                    logger.info(f"Schema {DWH_STAGING_SCHEMA} chưa tồn tại, đang tạo mới...")
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_STAGING_SCHEMA};")
                    conn.commit()
                    logger.info(f"Đã tạo schema {DWH_STAGING_SCHEMA}")
                else:
                    logger.info(f"Schema {DWH_STAGING_SCHEMA} đã tồn tại")

        # Thực thi file schema_staging.sql để thiết lập bảng và chỉ mục
        schema_staging = os.path.join(SQL_DIR, "schema_staging.sql")
        if os.path.exists(schema_staging):
            if not execute_sql_file(schema_staging):
                logger.error("Không thể thiết lập schema và bảng!")
                return False
        else:
            logger.error(f"Không tìm thấy file schema: {schema_staging}")
            return False
        
        logger.info("Đã thiết lập schema và bảng database thành công!")
        
        # Thực thi file insert_raw_to_staging.sql để chèn dữ liệu từ raw_jobs vào staging_jobs
        insert_staging = os.path.join(SQL_DIR, "insert_raw_to_staging.sql")
        if os.path.exists(insert_staging):
            logger.info("Đang chèn dữ liệu từ raw_jobs vào staging_jobs...")
            if not execute_sql_file(insert_staging):
                logger.error("Không thể chèn dữ liệu từ raw_jobs vào staging_jobs!")
                return False
            logger.info("Đã chèn dữ liệu từ raw_jobs vào staging_jobs thành công!")
        else:
            logger.error(f"Không tìm thấy file insert: {insert_staging}")
            return False
        
        # Đếm số bản ghi đã chèn
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {DWH_STAGING_SCHEMA}.staging_jobs")
                record_count = cursor.fetchone()[0]
                logger.info(f"Tìm thấy {record_count} bản ghi trong bảng staging_jobs")
                
                # Kiểm tra số lượng bản ghi trong raw_jobs
                cursor.execute(f"SELECT COUNT(*) FROM public.raw_jobs")
                raw_count = cursor.fetchone()[0]
                logger.info(f"Số bản ghi trong raw_jobs: {raw_count}")
                
                # Thống kê dữ liệu
                cursor.execute(f"SELECT COUNT(DISTINCT company_name) FROM {DWH_STAGING_SCHEMA}.staging_jobs")
                company_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(DISTINCT location) FROM {DWH_STAGING_SCHEMA}.staging_jobs")
                location_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(*), (COUNT(*) * 100.0 / {record_count}) FROM {DWH_STAGING_SCHEMA}.staging_jobs WHERE verified_employer = TRUE")
                verified_count, verified_percent = cursor.fetchone()
                
                logger.info("Thống kê dữ liệu:")
                logger.info(f"- Số công ty: {company_count}")
                logger.info(f"- Số địa điểm: {location_count}")
                logger.info(f"- Số nhà tuyển dụng đã xác thực: {verified_count} ({verified_percent:.1f}%)")
        
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thiết lập schema database: {str(e)}")
        return False

def run_stored_procedures():
    """Thực thi stored procedures"""
    try:
        logger.info("Đang thực thi stored procedures...")
        
        # Thực thi file stored procedures để đảm bảo các hàm đã được tạo
        stored_procs_file = os.path.join(SQL_DIR, "stored_procedures.sql")
        if os.path.exists(stored_procs_file):
            if not execute_sql_file(stored_procs_file):
                logger.error("Không thể tạo stored procedures!")
                return False
        else:
            logger.error(f"Không tìm thấy file stored procedures: {stored_procs_file}")
            return False
        
        # Gọi stored procedure cập nhật deadline
        try:
            logger.info("Thực thi stored procedure update_deadline...")
            execute_stored_procedure('update_deadline')
            logger.info("Đã cập nhật thành công thời gian còn lại")
            
            # Có thể thêm các stored procedures khác nếu cần
            
            return True
        except Exception as e:
            logger.error(f"Lỗi khi thực thi stored procedure: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Lỗi khi thực thi stored procedures: {e}")
        return False

def load_staging_data(limit=None, offset=0, query_filter=None):
    """
    Load dữ liệu từ bảng staging_jobs vào DataFrame
    
    Args:
        limit: Giới hạn số bản ghi cần lấy
        offset: Vị trí bắt đầu
        query_filter: Điều kiện WHERE để lọc dữ liệu
        
    Returns:
        DataFrame chứa dữ liệu từ staging_jobs
    """
    try:
        # Sửa lại tên bảng để tránh lặp lại schema
        table_name = f"{STAGING_JOBS_TABLE}" 
        logger.info(f"Đang tải dữ liệu từ bảng {table_name}...")
        
        # Xây dựng query
        query = f"SELECT * FROM {table_name}"
        
        # Thêm điều kiện lọc nếu có
        if query_filter:
            query += f" {query_filter}"
            
        # Thêm limit và offset
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset > 0:
            query += f" OFFSET {offset}"
        
        # Lấy dữ liệu
        df = get_dataframe(query)
        logger.info(f"Đã tải thành công {len(df)} bản ghi từ bảng {table_name}")
        return df
    except Exception as e:
        logger.error(f"Lỗi khi tải dữ liệu từ {table_name}: {e}")
        raise

def save_back_to_staging(df):
    """
    Lưu DataFrame đã xử lý trở lại bảng staging_jobs
    
    Args:
        df: DataFrame đã xử lý
        
    Returns:
        bool: Kết quả thực hiện
    """
    try:
        # Sửa lại tên bảng để tránh lặp lại schema
        table_name = f"{DWH_STAGING_SCHEMA}.staging_jobs"
        logger.info(f"Đang lưu {len(df)} bản ghi vào bảng {table_name}...")
        
        # Cách đơn giản hơn sử dụng upsert trực tiếp
        from sqlalchemy import create_engine, text
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        
        # Tạo một bản sao để tránh sửa đổi df gốc
        df_to_save = df.copy()
        
        # Xử lý các cột kiểu JSONB
        for col in ['skills', 'location_pairs', 'raw_data']:
            if col in df_to_save.columns:
                df_to_save[col] = df_to_save[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else 
                             (None if pd.isna(x) else json.dumps(x))
                )
        
        # Tạo temporary table trong phiên làm việc hiện tại
        with engine.begin() as conn:
            # Tạo bảng tạm
            conn.execute(text("DROP TABLE IF EXISTS temp_staging_jobs"))
            conn.execute(text(f"CREATE TABLE temp_staging_jobs (LIKE {table_name})"))
            
            # Lưu DataFrame vào bảng tạm
            df_to_save.to_sql('temp_staging_jobs', conn, if_exists='append', index=False)
            
            # Thực hiện upsert từ bảng tạm vào bảng chính
            update_columns = [c for c in df_to_save.columns if c != 'job_id']
            update_stmt = ", ".join([f"{col} = excluded.{col}" for col in update_columns])
            
            upsert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM temp_staging_jobs
            ON CONFLICT (job_id) 
            DO UPDATE SET {update_stmt}
            """
            
            # Thực thi upsert
            conn.execute(text(upsert_query))
            
            # Xóa bảng tạm
            conn.execute(text("DROP TABLE IF EXISTS temp_staging_jobs"))
        
        logger.info(f"Đã cập nhật thành công {len(df)} bản ghi vào bảng {table_name}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi lưu dữ liệu vào bảng {table_name}: {e}")
        return False

# Hàm xử lý dữ liệu
def process_staging_data(df):
    """
    Xử lý tất cả các trường dữ liệu cần thiết bằng pandas
    
    Args:
        df: DataFrame từ staging_jobs
        
    Returns:
        DataFrame đã xử lý
    """
    # Tạo bản sao để tránh ảnh hưởng đến dữ liệu gốc
    processed_df = df.copy()
    
    # 1. Xử lý location
    logger.info("Đang xử lý location...")
    if 'location_detail' in processed_df.columns:
        processed_df['location_pairs'] = processed_df['location_detail'].apply(extract_location_info)
    
    if 'location' in processed_df.columns and 'location_pairs' in processed_df.columns:
        processed_df['location'] = processed_df.apply(refine_location, axis=1)
    
    # 2. Xử lý title
    logger.info("Đang xử lý title...")
    if 'title' in processed_df.columns:
        processed_df['title_clean'] = processed_df['title'].apply(clean_title)
    
    # 3. Xử lý company_name
    logger.info("Đang xử lý company_name...")
    if 'company_name' in processed_df.columns:
        processed_df['company_name_standardized'] = processed_df['company_name'].apply(clean_company_name)
    
    logger.info(f"Đã hoàn thành xử lý chi tiết cho {len(processed_df)} bản ghi")
    return processed_df

def verify_etl_integrity(source_count, target_count, threshold=0.98):
    """
    Kiểm tra tính toàn vẹn dữ liệu sau ETL.
    
    Args:
        source_count (int): Số bản ghi nguồn
        target_count (int): Số bản ghi đích
        threshold (float): Ngưỡng chấp nhận (% dữ liệu được xử lý thành công)
        
    Returns:
        bool: True nếu tỷ lệ dữ liệu chuyển đổi đạt threshold
    """
    if source_count == 0:
        logger.warning("Không có dữ liệu nguồn để xử lý")
        return True
        
    success_rate = target_count / source_count
    logger.info(f"Tỷ lệ dữ liệu xử lý thành công: {success_rate:.2%} ({target_count}/{source_count})")
    
    if success_rate < threshold:
        logger.error(f"Tỷ lệ dữ liệu xử lý ({success_rate:.2%}) thấp hơn ngưỡng {threshold:.2%}")
        return False
        
    return True

def run_etl(batch_size=None, only_unprocessed=False, verbose=False):
    """
    Thực hiện quy trình ETL hoàn chỉnh
    
    Args:
        batch_size (int, optional): Số bản ghi xử lý mỗi batch, mặc định là None (xử lý tất cả)
        only_unprocessed (bool, optional): Chỉ xử lý các bản ghi chưa được xử lý, mặc định là False
        verbose (bool, optional): Hiển thị thêm thông tin chi tiết, mặc định là False
    
    Returns:
        dict: Kết quả thực hiện với các thông tin chi tiết
    """
    try:
        # Thiết lập logging level nếu verbose
        if verbose:
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                handler.setLevel(logging.DEBUG)
        
        logger.info(f"Bắt đầu ETL từ raw_jobs sang staging_jobs (batch_size={batch_size}, only_unprocessed={only_unprocessed})...")
        start_time = datetime.now()
        
        # 1. Thiết lập schema và bảng nếu cần
        if not setup_database_schema():
            logger.error("Không thể thiết lập schema và bảng!")
            return {"success": False, "error": "Không thể thiết lập schema và bảng!"}
        
        # 2. Chạy stored procedures để xử lý dữ liệu cơ bản
        if not run_stored_procedures():
            logger.warning("Có lỗi khi thực thi stored procedures!")
            # Vẫn tiếp tục vì có thể một số SP đã chạy thành công
        
        # 3. Load dữ liệu từ staging để xử lý thêm bằng pandas
        try:
            # Nếu only_unprocessed là True, chỉ lấy các bản ghi chưa được xử lý
            query_filter = None
            if only_unprocessed:
                query_filter = "WHERE processed IS NULL OR processed = FALSE"
                
            staging_df = load_staging_data(limit=batch_size, query_filter=query_filter)
            source_count = len(staging_df)
            
            if source_count == 0:
                logger.warning("Không có dữ liệu trong bảng staging_jobs để xử lý!")
                return {
                    "success": True,
                    "message": "Không có dữ liệu để xử lý",
                    "stats": {
                        "total_records": 0,
                        "processed_records": 0,
                        "success_count": 0,
                        "failure_count": 0,
                        "success_rate": 100.0,
                        "duration_seconds": (datetime.now() - start_time).total_seconds(),
                        "batch_count": 0
                    }
                }
                
            # 4. Xử lý chi tiết bằng pandas
            processed_df = process_staging_data(staging_df)
            processed_count = len(processed_df)
            
            # Kiểm tra tính toàn vẹn dữ liệu sau bước xử lý
            if not verify_etl_integrity(source_count, processed_count):
                logger.warning("Phát hiện mất mát dữ liệu trong quá trình xử lý!")
                # Vẫn tiếp tục nhưng đã cảnh báo
            
            # 5. Lưu kết quả trở lại bảng staging
            if not save_back_to_staging(processed_df):
                logger.error("Không thể lưu kết quả vào bảng staging!")
                return {
                    "success": False,
                    "error": "Không thể lưu kết quả vào bảng staging",
                    "stats": {
                        "total_records": source_count,
                        "processed_records": processed_count,
                        "duration_seconds": (datetime.now() - start_time).total_seconds()
                    }
                }
        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu staging: {e}")
            return {
                "success": False,
                "error": f"Lỗi khi xử lý dữ liệu staging: {str(e)}"
            }
        
        # Tính thời gian chạy
        duration = (datetime.now() - start_time).total_seconds()
        
        # Thống kê ETL
        etl_stats = {
            "total_records": source_count,
            "processed_records": processed_count,
            "success_count": processed_count,  # Giả sử tất cả đều thành công
            "failure_count": source_count - processed_count,
            "success_rate": processed_count / max(1, source_count) * 100,
            "duration_seconds": duration,
            "batch_count": 1  # Mặc định là 1 batch
        }
        
        logger.info(f"Quy trình ETL đã hoàn thành thành công trong {duration:.2f} giây!")
        logger.info(f"Thống kê ETL: {json.dumps(etl_stats)}")
        
        return {
            "success": True,
            "message": "ETL raw_to_staging hoàn thành thành công",
            "stats": etl_stats
        }
    except Exception as e:
        logger.error(f"Lỗi trong quy trình ETL: {e}")
        return {
            "success": False,
            "error": f"Lỗi trong quy trình ETL: {str(e)}"
        }

# Hàm main
if __name__ == "__main__":
    try:
        import argparse
        
        # Tạo parser
        parser = argparse.ArgumentParser(description="Job Data ETL Process")
        parser.add_argument("--limit", type=int, help="Giới hạn số bản ghi xử lý")
        parser.add_argument("--verbose", "-v", action="store_true", help="Hiển thị thông tin chi tiết")
        
        args = parser.parse_args()
        
        # Thiết lập logging level
        if args.verbose:
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                handler.setLevel(logging.DEBUG)
        
        # Thực thi ETL
        success = run_etl()
        
        if success:
            print("✅ Quy trình ETL đã hoàn thành thành công!")
            sys.exit(0)
        else:
            print("❌ Quy trình ETL thất bại!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Lỗi không xác định: {e}")
        print(f"❌ Lỗi không xác định: {e}")
        sys.exit(1)

