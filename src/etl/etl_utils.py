#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Các hàm tiện ích cho ETL process
"""
import pandas as pd
import logging
import json
import os
import sys
import duckdb
from datetime import datetime
from typing import Dict, List, Optional, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Thiết lập logging
logger = logging.getLogger(__name__)

from src.utils.config import DUCKDB_PATH, DWH_STAGING_SCHEMA

def get_duckdb_connection(duckdb_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """ 
    Kết nối đến DuckDB 
    
    Args:
        duckdb_path: Đường dẫn đến file DuckDB
        
    Returns:
        Kết nối DuckDB
    """
    # Đảm bảo đường dẫn là tuyệt đối
    if not os.path.isabs(duckdb_path):
        duckdb_path = os.path.join(PROJECT_ROOT, duckdb_path)
    
    # Đảm bảo thư mục cha tồn tại
    parent_dir = os.path.dirname(duckdb_path)
    os.makedirs(parent_dir, exist_ok=True)
    
    logger.info(f"Kết nối DuckDB tại: {duckdb_path}")
    
    return duckdb.connect(duckdb_path)

def execute_sql_file_duckdb(sql_file_path: str, conn=None) -> bool:
    """
    Thực thi file SQL trên DuckDB
    
    Args:
        sql_file_path: Đường dẫn đến file SQL
        conn: Kết nối DuckDB (tùy chọn)
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Nếu không truyền conn thì tự tạo và tự đóng
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

def setup_duckdb_schema() -> bool:
    """
    Thiết lập schema và bảng cho DuckDB
    
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        # Đường dẫn đến thư mục SQL
        SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
        if not os.path.exists(SQL_DIR):
            SQL_DIR = os.path.join(os.getcwd(), "sql")
            os.makedirs(SQL_DIR, exist_ok=True)
            
        with get_duckdb_connection() as conn:
            # Tạo schema nếu chưa có
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

def batch_insert_records(
    duck_conn: duckdb.DuckDBPyConnection, 
    table_name: str, 
    records: List[Dict], 
    batch_size: int = 1000,
    on_conflict: str = None
) -> int:
    """
    Batch insert records để tối ưu performance
    
    Args:
        duck_conn: Kết nối DuckDB
        table_name: Tên bảng
        records: List các records cần insert
        batch_size: Kích thước batch
        on_conflict: Xử lý conflict (e.g., "DO NOTHING")
        
    Returns:
        int: Số bản ghi đã insert thành công
    """
    if not records:
        return 0
        
    inserted_count = 0
    
    # Chia records thành các batch
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        try:
            # Chuẩn bị dữ liệu batch
            df_batch = pd.DataFrame(batch)
            
            # Xử lý các cột JSON
            for col in df_batch.columns:
                df_batch[col] = df_batch[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                )
            
            # Xây dựng câu lệnh INSERT
            if on_conflict:
                duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_batch {on_conflict}")
            else:
                duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_batch")
                
            inserted_count += len(batch)
            
        except Exception as e:
            logger.warning(f"Lỗi khi batch insert vào {table_name}: {e}")
            # Fallback: insert từng record
            for record in batch:
                try:
                    columns = list(record.keys())
                    placeholders = ', '.join(['?'] * len(columns))
                    values = [record[col] for col in columns]
                    
                    # Xử lý giá trị JSON
                    for j, val in enumerate(values):
                        if isinstance(val, (dict, list)):
                            values[j] = json.dumps(val)
                    
                    # Xây dựng câu lệnh INSERT
                    query = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    
                    if on_conflict:
                        query += f" {on_conflict}"
                        
                    duck_conn.execute(query, values)
                    inserted_count += 1
                except Exception as e2:
                    logger.error(f"Lỗi khi insert single record vào {table_name}: {e2}")
    
    # Thay đổi mức độ log từ INFO xuống DEBUG
    if inserted_count > 0:
        if inserted_count > 10:
            logger.info(f"Đã batch insert {inserted_count} records vào {table_name}")
        else:
            logger.debug(f"Đã batch insert {inserted_count} records vào {table_name}")
    
    return inserted_count

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

def lookup_location_key(
    duck_conn: duckdb.DuckDBPyConnection,
    province: str = None,
    city: str = None,
    district: str = None
) -> Optional[int]:
    """
    Tìm location_sk từ bảng DimLocation dựa trên province, city, district
    
    Args:
        duck_conn: Kết nối DuckDB
        province: Tên tỉnh
        city: Tên thành phố
        district: Tên quận/huyện
    
    Returns:
        location_sk hoặc None nếu không tìm thấy
    """
    try:
        # Xây dựng query động dựa trên các tham số có giá trị
        conditions = ["is_current = TRUE"]
        params = []
        
        if province is not None:
            conditions.append("province = ?")
            params.append(province)
        else:
            conditions.append("province IS NULL")
            
        if city is not None:
            conditions.append("city = ?")
            params.append(city)
        else:
            conditions.append("city IS NULL")
            
        if district is not None:
            conditions.append("district = ?")
            params.append(district)
        else:
            conditions.append("district IS NULL")
        
        query = f"""
            SELECT location_sk
            FROM DimLocation
            WHERE {' AND '.join(conditions)}
            LIMIT 1
        """
        
        result = duck_conn.execute(query, params).fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Lỗi khi tìm location_sk: {e}")
        return None 