#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module cung cấp các hàm cơ bản để tương tác với cơ sở dữ liệu PostgreSQL.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import pandas as pd
import time
from contextlib import contextmanager
import logging
from typing import Dict, List, Any, Optional, Union, Tuple

# Import module cấu hình
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    
    # Fallback config nếu không import được Config
    class Config:
        class Database:
            @classmethod
            def get_connection_params(cls):
                return {
                    "host": "localhost",
                    "port": 5432,
                    "user": "jobinsight",
                    "password": "jobinsight",
                    "database": "jobinsight",
                    "dbname": "jobinsight"
                }

logger = get_logger("db.core")

def get_connection_string(conn_params=None):
    """
    Tạo chuỗi kết nối PostgreSQL
    
    Args:
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        str: Chuỗi kết nối
    """
    params = conn_params or Config.Database.get_connection_params()
    return f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

def get_engine(conn_params=None):
    """
    Tạo SQLAlchemy engine để kết nối với database
    
    Args:
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine
    """
    connection_string = get_connection_string(conn_params)
    return create_engine(connection_string)

@contextmanager
def get_connection(conn_params=None):
    """
    Context manager để quản lý kết nối tới PostgreSQL
    
    Args:
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Yields:
        connection: Kết nối database
    """
    conn = None
    max_attempts = 5
    attempt = 0
    
    params = conn_params or Config.Database.get_connection_params()
    
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host=params["host"],
                port=params["port"],
                database=params["database"],
                user=params["user"],
                password=params["password"]
            )
            conn.autocommit = False
            yield conn
            if not conn.closed:
                conn.commit()
            break
        except psycopg2.OperationalError as e:
            attempt += 1
            logger.warning(f"Lỗi kết nối tới DB (lần {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error("Không thể kết nối tới PostgreSQL sau nhiều lần thử")
                raise
        finally:
            if conn is not None and not conn.closed:
                conn.close()
                logger.debug("Đã đóng kết nối DB")

def execute_query(query, params=None, fetch=True, conn_params=None):
    """
    Thực thi truy vấn SQL và trả về kết quả
    
    Args:
        query (str): Câu truy vấn SQL
        params (tuple, dict, optional): Tham số truy vấn
        fetch (bool): Có lấy kết quả trả về hay không
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        list: Kết quả truy vấn
    """
    with get_connection(conn_params) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None

def get_dataframe(query, params=None, conn_params=None):
    """
    Thực thi truy vấn SQL và trả về DataFrame
    
    Args:
        query (str): Câu truy vấn SQL
        params (tuple, dict, optional): Tham số truy vấn
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        pandas.DataFrame: Kết quả truy vấn dạng DataFrame
    """
    try:
        engine = get_engine(conn_params)
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn dữ liệu: {e}")
        raise

def table_exists(table_name, schema='public', conn_params=None):
    """
    Kiểm tra bảng có tồn tại trong database không
    
    Args:
        table_name (str): Tên bảng
        schema (str): Schema chứa bảng
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        bool: True nếu bảng tồn tại, False nếu không
    """
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    );
    """
    with get_connection(conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            return cursor.fetchone()[0]

def execute_stored_procedure(procedure_name, params=None, conn_params=None):
    """
    Thực thi stored procedure
    
    Args:
        procedure_name (str): Tên stored procedure
        params (tuple, optional): Tham số procedure
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        list: Kết quả từ procedure nếu có
    """
    call_statement = f"CALL {procedure_name}("
    if params:
        placeholders = ", ".join(["%s"] * len(params))
        call_statement += placeholders
    call_statement += ");"
    
    with get_connection(conn_params) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(call_statement, params)
            try:
                return cursor.fetchall()
            except psycopg2.ProgrammingError:
                # Procedure không trả về kết quả
                return None

def execute_sql_file(sql_file_path, conn_params=None):
    """
    Thực thi file SQL
    
    Args:
        sql_file_path (str): Đường dẫn đến file SQL cần thực thi
        conn_params: Tham số kết nối, nếu None sẽ lấy từ Config
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Thực thi toàn bộ file SQL như một khối lệnh duy nhất
        with get_connection(conn_params) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql_script)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Lỗi khi thực thi SQL file: {sql_file_path}")
                    logger.error(f"Chi tiết lỗi: {type(e).__name__}: {str(e)}")
                    return False
        
        logger.info(f"Đã thực thi thành công file SQL: {sql_file_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thực thi file SQL {sql_file_path}: {str(e)}")
        logger.error(f"Chi tiết lỗi: {type(e).__name__}: {str(e)}")
        return False 