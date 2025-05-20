#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL module cho việc chuyển dữ liệu từ Staging sang Data Warehouse (DuckDB)
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import pandas as pd
import duckdb

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
        logging.FileHandler(os.path.join(LOGS_DIR, "etl.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from src.utils.logger import get_logger
from src.utils.db import get_connection, get_dataframe, execute_query
from src.utils.config import DUCKDB_PATH, DWH_STAGING_SCHEMA, STAGING_JOBS_TABLE

try:
    from src.processing.data_prepare import prepare_dim_job, prepare_dim_company, prepare_dim_location, generate_date_range
except ImportError:
    from processing.data_prepare import prepare_dim_job, prepare_dim_company, prepare_dim_location, generate_date_range

# Đường dẫn đến thư mục SQL
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.getcwd(), "sql")
    os.makedirs(SQL_DIR, exist_ok=True)

def get_duckdb_connection(duckdb_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """ Kết nối đến DuckDB """
    # Đảm bảo đường dẫn là tuyệt đối
    if not os.path.isabs(duckdb_path):
        duckdb_path = os.path.join(PROJECT_ROOT, duckdb_path)
    
    # Đảm bảo thư mục cha tồn tại
    parent_dir = os.path.dirname(duckdb_path)
    os.makedirs(parent_dir, exist_ok=True)
    
    logger.info(f"Kết nối DuckDB tại: {duckdb_path}")
    
    return duckdb.connect(duckdb_path)

def execute_sql_file_duckdb(sql_file_path, conn=None):
    """Thực thi file SQL trên DuckDB với connection truyền vào (hoặc tự tạo)"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        # Nếu không truyền conn thì tự tạo và tự đóng, còn truyền vào thì không đóng
        close_conn = False
        if conn is None:
            conn = get_duckdb_connection()
            close_conn = True
        conn.execute(sql_script)
        if close_conn:
            conn.close()
        logger.info(f"Đã thực thi file SQL: {sql_file_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thực thi file SQL {sql_file_path}: {str(e)}")
        return False

def setup_duckdb_schema():
    """Thiết lập schema và bảng cho DuckDB"""
    try:
        with get_duckdb_connection() as conn:
            # Tạo schema nếu chưa có (DuckDB sẽ không báo lỗi nếu đã tồn tại)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_STAGING_SCHEMA};")
            logger.info(f"Đã đảm bảo tồn tại schema: {DWH_STAGING_SCHEMA}")

            # Thực thi file schema_dwh.sql để tạo bảng và chỉ mục
            schema_dwh = os.path.join(SQL_DIR, "schema_dwh.sql")
            if os.path.exists(schema_dwh):
                if not execute_sql_file_duckdb(schema_dwh, conn=conn):
                    logger.error("Không thể thiết lập schema và bảng!")
                    return False
            else:
                logger.error(f"Không tìm thấy file schema: {schema_dwh}")
                return False

            # Kiểm tra xem các bảng đã được tạo thành công chưa
            tables_in_db = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
            for table in ['DimJob', 'DimCompany', 'DimLocation', 'DimDate', 'FactJobPostingDaily', 'FactJobLocationBridge']:
                if table in tables_in_db:
                    logger.info(f"✓ Bảng {table} đã được tạo thành công")
                else:
                    logger.warning(f"✗ Bảng {table} KHÔNG được tạo thành công")

        logger.info("Đã thiết lập schema và bảng database thành công cho DuckDB!")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thiết lập schema database với DuckDB: {str(e)}")
        return False
    

def get_staging_batch(last_etl_date: datetime) -> pd.DataFrame:
    """
    Lấy batch dữ liệu từ staging jobs kể từ lần ETL gần nhất
    
    Args:
        last_etl_date: Timestamp của lần ETL gần nhất
        
    Returns:
        DataFrame chứa bản ghi cần xử lý
    """
    try:
        logger.info(f"Truy vấn dữ liệu từ bảng {STAGING_JOBS_TABLE}")
        
        query = f"""
            SELECT *
            FROM {STAGING_JOBS_TABLE}
            WHERE crawled_at >= %s
            OR
            (crawled_at IS NOT NULL AND %s IS NULL)
        """
        
        # Kiểm tra xem bảng có tồn tại không
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{DWH_STAGING_SCHEMA}' 
                AND table_name = 'staging_jobs'
            );
        """
        
        # Thực hiện kiểm tra
        exists = execute_query(table_exists_query, fetch=True)
        
        if not exists or not exists[0].get('exists', False):
            logger.error(f"Bảng {STAGING_JOBS_TABLE} không tồn tại!")
            # Trả về DataFrame rỗng nếu bảng không tồn tại
            return pd.DataFrame()
        
        df = get_dataframe(query, params=(last_etl_date, last_etl_date))
        logger.info(f"Đã lấy {len(df)} bản ghi từ staging (từ {last_etl_date})")
        
        # Log các cột để debug
        if not df.empty:
            logger.info(f"Các cột có trong dữ liệu: {list(df.columns)}")
        
        return df
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu từ staging: {e}", exc_info=True)
        # Trả về DataFrame rỗng trong trường hợp lỗi
        return pd.DataFrame()
    
def lookup_dimension_key(
    duck_conn: duckdb.DuckDBPyConnection,
    dim_table: str,
    key_column: str,
    key_value: Any,
    surrogate_key_col: str
) -> Optional[int]:
    """
    Tìm surrogate key từ bảng dimension
    
    Args:
        duck_conn: Kết nối DuckDB
        dim_table: Tên bảng dimension
        key_column: Tên cột dùng để tìm kiếm
        key_value: Giá trị cần tìm
        surrogate_key_col: Tên cột surrogate key
    
    Returns:
        Surrogate key hoặc None nếu không tìm thấy
    """
    try:
        query = f"""
            SELECT {surrogate_key_col}
            FROM {dim_table}
            WHERE {key_column} = ?
            AND is_current = TRUE
            LIMIT 1
        """
        
        result = duck_conn.execute(query, [key_value]).fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Lỗi khi tìm khóa trong {dim_table}: {e}")
        return None

def generate_fact_records(
    duck_conn: duckdb.DuckDBPyConnection,
    staging_records: pd.DataFrame
) -> Tuple[List[Dict], List[Dict]]:
    """
    Tạo bản ghi fact và bridge từ dữ liệu staging
    
    Args:
        duck_conn: Kết nối DuckDB
        staging_records: Dữ liệu từ staging
    
    Returns:
        Tuple chứa (fact_records, bridge_records)
    """
    fact_records = []
    bridge_records = []
    
    for _, job in staging_records.iterrows():
        # Lookup dimension keys
        job_sk = lookup_dimension_key(
            duck_conn, 'DimJob', 'job_id', job.job_id, 'job_sk'
        )
        
        company_name = job.company_name_standardized if pd.notna(job.company_name_standardized) else job.company_name
        company_sk = lookup_dimension_key(
            duck_conn, 'DimCompany', 'company_name_standardized', company_name, 'company_sk'
        )
        
        # Check if required keys exist
        if not job_sk or not company_sk:
            logger.warning(f"Bỏ qua job_id={job.job_id}: Không tìm thấy dimension key (job_sk={job_sk}, company_sk={company_sk})")
            continue
        
        # Xử lý ngày
        today = datetime.now().date()
        due_date = pd.to_datetime(job.due_date).date() if pd.notna(job.due_date) else None
        posted_time = pd.to_datetime(job.posted_time) if pd.notna(job.posted_time) else None
        
        # Tạo bản ghi fact
        fact_record = {
            'job_sk': job_sk,
            'company_sk': company_sk,
            'date_id': today,
            'salary_min': job.salary_min if pd.notna(job.salary_min) else None,
            'salary_max': job.salary_max if pd.notna(job.salary_max) else None,
            'salary_type': job.salary_type if pd.notna(job.salary_type) else None,
            'due_date': due_date,
            'time_remaining': job.time_remaining if pd.notna(job.time_remaining) else None,
            'verified_employer': job.verified_employer if pd.notna(job.verified_employer) else False,
            'posted_time': posted_time,
            'crawled_at': pd.to_datetime(job.crawled_at) if pd.notna(job.crawled_at) else datetime.now()
        }
        
        # Insert fact record và lấy fact_id
        columns = ', '.join([k for k, v in fact_record.items() if v is not None])
        placeholders = ', '.join(['?'] * len([v for v in fact_record.values() if v is not None]))
        values = [v for v in fact_record.values() if v is not None]
        
        insert_query = f"""
            INSERT INTO FactJobPostingDaily ({columns})
            VALUES ({placeholders})
            RETURNING fact_id
        """
        
        try:
            result = duck_conn.execute(insert_query, values).fetchone()
            if not result:
                logger.warning(f"Không thể insert fact record cho job_id={job.job_id}")
                continue
                
            fact_id = result[0]
            fact_records.append(fact_record)
            
            # Xử lý locations
            if pd.isna(job.location):
                # Nếu không có location, thêm Unknown
                location_sk = lookup_dimension_key(
                    duck_conn, 'DimLocation', 'location', 'Unknown', 'location_sk'
                )
                if location_sk:
                    bridge_records.append({'fact_id': fact_id, 'location_sk': location_sk})
            else:
                # Tách locations từ string
                if isinstance(job.location, str):
                    for loc in job.location.split(','):
                        location = loc.strip()
                        if location:
                            location_sk = lookup_dimension_key(
                                duck_conn, 'DimLocation', 'location', location, 'location_sk'
                            )
                            if location_sk:
                                bridge_records.append({'fact_id': fact_id, 'location_sk': location_sk})
        
        except Exception as e:
            logger.error(f"Lỗi khi insert fact record cho job_id={job.job_id}: {e}")
            continue
    
    return fact_records, bridge_records



if __name__ == "__main__":
    # 1. Lấy dữ liệu từ staging
    last_etl_date = datetime.now() - timedelta(days=7)
    staging_batch = get_staging_batch(last_etl_date)
    if not staging_batch.empty:
        logger.info(f"Đã lấy {len(staging_batch)} bản ghi từ staging")
    else:
        logger.info("Không có bản ghi nào để xử lý từ staging")
        sys.exit(0)  # Thoát sớm nếu không có dữ liệu

    # 2. Có thể xóa file DuckDB cũ nếu cần (tùy chọn)
    if os.path.exists(DUCKDB_PATH) and input("Xóa file DuckDB cũ? (y/n): ").lower() == 'y':
        os.remove(DUCKDB_PATH)
        logger.info(f"Đã xóa file DuckDB cũ: {DUCKDB_PATH}")

    # 3. Thiết lập schema và bảng (sau khi xóa file nếu cần)
    setup_duckdb_schema()

    # 4. Kết nối DuckDB và thực hiện insert
    duck_conn = get_duckdb_connection(DUCKDB_PATH)
    
    # 5. Xử lý và insert dữ liệu
    dim_stats = {}
    
    # 5.1 DimJob
    logger.info("Xử lý DimJob")
    dim_job_df = prepare_dim_job(staging_batch)
    job_inserted = 0
    
    # Direct insert to DimJob
    for _, job in dim_job_df.iterrows():
        try:
            job_dict = job.to_dict()
            
            # Loại bỏ cột job_sk để DuckDB tự tạo qua AUTOINCREMENT
            if 'job_sk' in job_dict:
                del job_dict['job_sk']
            
            # Lọc các cột vào đúng thứ tự và loại bỏ job_sk
            columns = [col for col in job_dict.keys() if col != 'job_sk']
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            values = [job_dict[col] for col in columns]
            
            # Đảm bảo các giá trị JSON được xử lý đúng
            for i, val in enumerate(values):
                if isinstance(val, (list, dict)):
                    values[i] = json.dumps(val)
            
            query = f"""
                INSERT INTO DimJob ({columns_str})
                VALUES ({placeholders})
            """
            
            duck_conn.execute(query, values)
            job_inserted += 1
        except Exception as e:
            logger.error(f"Lỗi khi insert vào DimJob: {e}")
            logger.error(f"Record: {job_dict}")
    
    dim_stats['DimJob'] = {'inserted': job_inserted}
    logger.info(f"Đã insert {job_inserted} bản ghi vào DimJob")

    # 5.2. DimCompany
    logger.info("Xử lý DimCompany")
    dim_company_df = prepare_dim_company(staging_batch)
    company_inserted = 0
    
    # Direct insert to DimCompany
    for _, company in dim_company_df.iterrows():
        try:
            company_dict = company.to_dict()
            
            # Loại bỏ cột company_sk để DuckDB tự tạo qua AUTOINCREMENT
            if 'company_sk' in company_dict:
                del company_dict['company_sk']
            
            # Lọc các cột và loại bỏ company_sk
            columns = [col for col in company_dict.keys() if col != 'company_sk']
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            values = [company_dict[col] for col in columns]
            
            query = f"""
                INSERT INTO DimCompany ({columns_str})
                VALUES ({placeholders})
            """
            
            duck_conn.execute(query, values)
            company_inserted += 1
        except Exception as e:
            logger.error(f"Lỗi khi insert vào DimCompany: {e}")
            logger.error(f"Record: {company_dict}")
    
    dim_stats['DimCompany'] = {'inserted': company_inserted}
    logger.info(f"Đã insert {company_inserted} bản ghi vào DimCompany")

    # 2.3. DimLocation
    logger.info("Xử lý DimLocation")
    dim_location_df = prepare_dim_location(staging_batch)
    location_inserted = 0
    
    # Direct insert to DimLocation
    for _, location in dim_location_df.iterrows():
        try:
            location_dict = location.to_dict()
            
            # Loại bỏ cột location_sk để DuckDB tự tạo qua AUTOINCREMENT
            if 'location_sk' in location_dict:
                del location_dict['location_sk']
            
            # Lọc các cột và loại bỏ location_sk
            columns = [col for col in location_dict.keys() if col != 'location_sk']
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            values = [location_dict[col] for col in columns]
            
            # Xử lý các trường JSON
            for i, val in enumerate(values):
                if isinstance(val, (dict, list)):
                    values[i] = json.dumps(val)
            
            query = f"""
                INSERT INTO DimLocation ({columns_str})
                VALUES ({placeholders})
            """
            
            duck_conn.execute(query, values)
            location_inserted += 1
        except Exception as e:
            logger.error(f"Lỗi khi insert vào DimLocation: {e}")
            logger.error(f"Record: {location_dict}")
    
    dim_stats['DimLocation'] = {'inserted': location_inserted}
    logger.info(f"Đã insert {location_inserted} bản ghi vào DimLocation")

    # 5.4. Đảm bảo bảng DimDate có đầy đủ các ngày cần thiết
    logger.info("Xử lý DimDate")
    start_date = (datetime.now() - timedelta(days=60)).date()
    end_date = (datetime.now() + timedelta(days=240)).date()
    
    date_df = generate_date_range(start_date, end_date)
    date_inserted = 0
    
    # Direct insert to DimDate
    for _, date_record in date_df.iterrows():
        try:
            date_dict = date_record.to_dict()
            
            # Check if date already exists
            exists = duck_conn.execute(f"SELECT 1 FROM DimDate WHERE date_id = ?", [date_dict['date_id']]).fetchone()
            if exists:
                continue
            
            # Insert
            columns = ", ".join(date_dict.keys())
            placeholders = ", ".join(["?"] * len(date_dict))
            values = list(date_dict.values())
            
            query = f"""
                INSERT INTO DimDate ({columns})
                VALUES ({placeholders})
            """
            
            duck_conn.execute(query, values)
            date_inserted += 1
        except Exception as e:
            logger.error(f"Lỗi khi insert vào DimDate: {e}")
            logger.error(f"Record: {date_dict}")
            continue
    
    dim_stats['DimDate'] = {'inserted': date_inserted}
    logger.info(f"Đã insert {date_inserted} bản ghi vào DimDate")

    # 5.5. Insert dữ liệu vào FactJobPostingDaily và FactJobLocationBridge
    logger.info("Xử lý FactJobPostingDaily và FactJobLocationBridge")
    fact_records, bridge_records = generate_fact_records(duck_conn, staging_batch)
    logger.info(f"Đã insert {len(fact_records)} bản ghi vào FactJobPostingDaily")
    logger.info(f"Chuẩn bị insert {len(bridge_records)} bản ghi vào FactJobLocationBridge")

    # Insert bridge records vào FactJobLocationBridge
    for bridge in bridge_records:
        try:
            columns = ", ".join(bridge.keys())
            placeholders = ", ".join(["?"] * len(bridge))
            values = list(bridge.values())
            query = f"""
                INSERT INTO FactJobLocationBridge ({columns})
                VALUES ({placeholders})
            """
            duck_conn.execute(query, values)
        except Exception as e:
            logger.error(f"Lỗi khi insert vào FactJobLocationBridge: {e}")
            logger.error(f"Record: {bridge}")
    logger.info(f"Đã insert {len(bridge_records)} bản ghi vào FactJobLocationBridge")
