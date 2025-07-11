"""
Database utilities for connecting and executing queries on PostgreSQL
"""

import sys
import logging
from pathlib import Path
import time
import pandas as pd

# Thử import các module cần thiết, nếu không có thì bỏ qua
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from sqlalchemy import create_engine
    from contextlib import contextmanager
    _HAS_DB_MODULES = True
except ImportError:
    # Thông báo cho người dùng biết không có các module DB
    logging.warning("Database modules (psycopg2, sqlalchemy) not found. Database functionality will be limited.")
    _HAS_DB_MODULES = False

from src.utils.logger import get_logger
try:
    from src.utils.config import Config
except ImportError:
    from src.utils.config import DB_CONFIG

logger = get_logger("db")

def get_connection_string():
    """
    Tạo chuỗi kết nối PostgreSQL
    
    Returns:
        str: Chuỗi kết nối
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return None
        
    try:
        db_config = Config.Database.get_connection_params()
    except (NameError, AttributeError):
        db_config = DB_CONFIG
        
    return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

def get_engine():
    """
    Tạo SQLAlchemy engine để kết nối với database
    
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return None
        
    connection_string = get_connection_string()
    return create_engine(connection_string)

@contextmanager
def get_connection():
    """
    Context manager để quản lý kết nối tới PostgreSQL
    
    Yields:
        connection: Kết nối database
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        yield None
        return
        
    conn = None
    max_attempts = 5
    attempt = 0
    
    try:
        db_config = Config.Database.get_connection_params()
    except (NameError, AttributeError):
        db_config = DB_CONFIG
    
    while attempt < max_attempts:
        try:
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"]
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

def execute_query(query, params=None, fetch=True):
    """
    Thực thi truy vấn SQL và trả về kết quả
    
    Args:
        query (str): Câu truy vấn SQL
        params (tuple, dict, optional): Tham số truy vấn
        fetch (bool): Có lấy kết quả trả về hay không
        
    Returns:
        list: Kết quả truy vấn
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return None
        
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None

def insert_dataframe(df, table_name, if_exists='append'):
    """
    Chèn DataFrame vào bảng PostgreSQL
    
    Args:
        df (pandas.DataFrame): DataFrame cần chèn
        table_name (str): Tên bảng
        if_exists (str): Hành động khi bảng đã tồn tại ('append', 'replace', 'fail')
        
    Returns:
        int: Số bản ghi đã chèn
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return None
        
    try:
        engine = get_engine()
        result = df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"Đã chèn {len(df)} bản ghi vào bảng {table_name}")
        return result
    except Exception as e:
        logger.error(f"Lỗi khi chèn dữ liệu vào {table_name}: {e}")
        raise

def get_dataframe(query, params=None):
    """
    Thực thi truy vấn SQL và trả về DataFrame
    
    Args:
        query (str): Câu truy vấn SQL
        params (tuple, dict, optional): Tham số truy vấn
        
    Returns:
        pandas.DataFrame: Kết quả truy vấn dạng DataFrame
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return pd.DataFrame()
        
    try:
        engine = get_engine()
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn dữ liệu: {e}")
        raise

def table_exists(table_name, schema='public'):
    """
    Kiểm tra bảng có tồn tại trong database không
    
    Args:
        table_name (str): Tên bảng
        schema (str): Schema chứa bảng
        
    Returns:
        bool: True nếu bảng tồn tại, False nếu không
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return False
        
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    );
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (schema, table_name))
            return cursor.fetchone()[0]

def execute_stored_procedure(procedure_name, params=None):
    """
    Thực thi stored procedure
    
    Args:
        procedure_name (str): Tên stored procedure
        params (tuple, optional): Tham số procedure
        
    Returns:
        list: Kết quả từ procedure nếu có
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return None
        
    call_statement = f"CALL {procedure_name}("
    if params:
        placeholders = ", ".join(["%s"] * len(params))
        call_statement += placeholders
    call_statement += ");"
    
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(call_statement, params)
            try:
                return cursor.fetchall()
            except psycopg2.ProgrammingError:
                # Procedure không trả về kết quả
                return None

# Hàm tiện ích
def execute_sql_file(sql_file_path):
    """
    Thực thi file SQL
    
    Args:
        sql_file_path (str): Đường dẫn đến file SQL cần thực thi
        
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    if not _HAS_DB_MODULES:
        logger.error("Database modules not available")
        return False
        
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Thực thi toàn bộ file SQL như một khối lệnh duy nhất
        # thay vì cắt theo dấu chấm phẩy
        with get_connection() as conn:
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
    