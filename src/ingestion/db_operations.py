#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module quản lý các thao tác bulk với database
"""

import os
import sys
import io
import psycopg2
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
import tempfile
import csv

# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.config import DB_CONFIG
    from src.db.bulk_operations import DBBulkOperations  # Import từ db module thay vì định nghĩa lại
    from src.common.decorators import retry
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    # Fallback DB config
    DB_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "user": "jobinsight",
        "password": "jobinsight",
        "database": "jobinsight"
    }
    # Import DBBulkOperations từ module db
    from src.db.bulk_operations import DBBulkOperations
    # Fallback cho retry decorator
    def retry(max_tries=3, delay_seconds=1.0, **kwargs):
        def decorator(func):
            return func
        return decorator

logger = get_logger("ingestion.db_operations")

# Note: Xóa định nghĩa class DBBulkOperations tại đây và thay bằng import ở trên

# Các hàm phụ trợ cho ingestion
@retry(max_tries=3, delay_seconds=2, backoff_factor=2.0, logger=logger, 
       exceptions=[psycopg2.Error, psycopg2.OperationalError, IOError])
def batch_insert_records(table_name, records, schema=None):
    """
    Thực hiện batch insert nhiều record vào bảng
    
    Args:
        table_name: Tên bảng
        records: List của dict các record cần insert
        schema: Schema của bảng nếu khác public
        
    Returns:
        Dict: Kết quả thao tác
    """
    if not records:
        logger.warning("Không có records để insert")
        return {'inserted': 0}
    
    # Sử dụng DBBulkOperations đã import từ src.db.bulk_operations
    db_ops = DBBulkOperations()
    
    try:
        # Chuyển từ list dict sang DataFrame
        df = pd.DataFrame(records)
                    
        # Dùng bulk insert với COPY
        result = db_ops.bulk_insert_with_copy(df, table_name, schema)
        
        return {'inserted': result.get('rows_inserted', 0)}
    
    except Exception as e:
        logger.error(f"Lỗi khi batch insert: {str(e)}")
        raise

@retry(max_tries=3, delay_seconds=1.0, logger=logger, 
       exceptions=[psycopg2.Error, psycopg2.OperationalError])
def ensure_table_exists(table_name, schema=None):
    """
    Kiểm tra xem bảng đã tồn tại chưa
    
    Args:
        table_name: Tên bảng cần kiểm tra
        schema: Schema của bảng nếu khác public
        
    Returns:
        bool: True nếu bảng tồn tại
    """
    try:
        schema_check = schema or "public"
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s
                AND table_name = %s
            )
        """
        
        # Sử dụng DBBulkOperations để lấy connection
        db_ops = DBBulkOperations()
        conn = db_ops._get_connection()
        
        try:
            with conn.cursor() as cur:
                cur.execute(query, (schema_check, table_name))
                exists = cur.fetchone()[0]
                return exists
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra table exists: {str(e)}")
        return False

@retry(max_tries=2, delay_seconds=1.0, logger=logger, 
       exceptions=[psycopg2.Error, psycopg2.OperationalError])
def insert_record(table_name, record, schema=None):
    """
    Insert một record vào bảng
    
    Args:
        table_name: Tên bảng
        record: Dict record cần insert
        schema: Schema của bảng nếu khác public
        
    Returns:
        bool: True nếu thành công
    """
    return batch_insert_records(table_name, [record], schema).get('inserted') == 1

# Các hàm khác giữ nguyên... 