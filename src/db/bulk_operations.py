#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module quản lý các thao tác bulk với database.
"""

import os
import sys
import io
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import csv
import tempfile
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

# Import module cấu hình
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config
    from src.db.core import get_connection
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
    
    # Fallback get_connection
    from contextlib import contextmanager
    @contextmanager
    def get_connection(conn_params=None):
        params = conn_params or Config.Database.get_connection_params()
        conn = psycopg2.connect(
            host=params["host"],
            port=params["port"],
            database=params["database"],
            user=params["user"],
            password=params["password"]
        )
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

logger = get_logger("db.bulk_operations")

class DBBulkOperations:
    """
    Class xử lý các thao tác bulk với database
    - Bulk insert với COPY
    - Bulk upsert với đánh dấu
    - Quản lý connection pool
    """
    
    def __init__(self, conn_params=None):
        """
        Khởi tạo DBBulkOperations
        
        Args:
            conn_params: Thông số kết nối DB, nếu None sẽ dùng Config.Database
        """
        self.conn_params = conn_params if conn_params is not None else Config.Database.get_connection_params()
        self._pool = None  # Connection pool (nếu cần)
        
        logger.info(f"Khởi tạo DBBulkOperations kết nối tới {self.conn_params.get('host')}:{self.conn_params.get('port')}")
    
    def _get_connection(self):
        """
        Lấy connection từ pool hoặc tạo mới
        
        Returns:
            psycopg2.connection: Kết nối đến database
        """
        try:
            # Nếu có pool, lấy từ pool
            if self._pool is not None:
                return self._pool.getconn()
            
            # Không có pool, tạo connection mới
            conn = psycopg2.connect(
                host=self.conn_params.get('host'),
                port=self.conn_params.get('port'),
                user=self.conn_params.get('user'),
                password=self.conn_params.get('password'),
                dbname=self.conn_params.get('database')
            )
            
            return conn
        
        except Exception as e:
            logger.error(f"Lỗi kết nối database: {str(e)}")
            raise
    
    def bulk_insert_with_copy(self, df: pd.DataFrame, table_name: str, schema: str = None) -> Dict[str, Any]:
        """
        Sử dụng PostgreSQL COPY để insert nhanh
        
        Args:
            df: DataFrame cần insert
            table_name: Tên bảng đích
            schema: Schema của bảng, nếu None sẽ sử dụng public
            
        Returns:
            Dict: Kết quả thao tác {'rows_inserted': int, 'execution_time': float}
        """
        if df.empty:
            logger.warning(f"DataFrame rỗng, không thể insert vào {table_name}")
            return {'rows_inserted': 0, 'execution_time': 0}
        
        # Tạo full table name nếu có schema
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        
        conn = None
        result = {'rows_inserted': 0, 'execution_time': 0}
        
        try:
            # Lấy connection
            conn = self._get_connection()
            
            # Bắt đầu transaction
            with conn.cursor() as cur:
                # Chuẩn bị dữ liệu để copy
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
                output.seek(0)
            
                # Tạo mệnh đề COPY với tên cột
                columns = ", ".join([f'"{col}"' for col in df.columns])
                copy_query = f"COPY {full_table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\"')"
                
                # Thực hiện copy
                start_time = datetime.now()
                cur.copy_expert(copy_query, output)
                
            # Commit transaction
            conn.commit()
            end_time = datetime.now()
            
            # Cập nhật kết quả
            result['rows_inserted'] = len(df)
            result['execution_time'] = (end_time - start_time).total_seconds()
            
            logger.info(f"Đã insert {result['rows_inserted']} rows vào {full_table_name} trong {result['execution_time']:.2f}s")
                
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Lỗi khi thực hiện COPY vào {full_table_name}: {str(e)}")
            raise
        
        finally:
            if conn:
                conn.close()
        
        return result
    
    def bulk_upsert(self, df: pd.DataFrame, table_name: str, key_columns: List[str], 
                    schema: str = None, update_columns: List[str] = None) -> Dict[str, Any]:
        """
        Thực hiện upsert (INSERT ... ON CONFLICT UPDATE)
        
        Args:
            df: DataFrame cần upsert
            table_name: Tên bảng đích
            key_columns: Danh sách cột làm key cho conflict
            schema: Schema của bảng, nếu None sẽ sử dụng public
            update_columns: Danh sách cột cần update khi conflict, nếu None sẽ update tất cả
            
        Returns:
            Dict: Kết quả thao tác {'inserted': int, 'updated': int, 'execution_time': float}
        """
        if df.empty:
            logger.warning(f"DataFrame rỗng, không thể upsert vào {table_name}")
            return {'inserted': 0, 'updated': 0, 'execution_time': 0}
        
        # Tạo full table name nếu có schema
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        temp_table_name = f"tmp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        conn = None
        result = {'inserted': 0, 'updated': 0, 'execution_time': 0}
        
        try:
            # Lấy connection
            conn = self._get_connection()
            
            start_time = datetime.now()
            
            with conn.cursor() as cur:
                # 1. Tạo bảng tạm với schema giống bảng chính
                cur.execute(f"""
                    CREATE TEMP TABLE {temp_table_name} 
                    ON COMMIT DROP 
                    AS SELECT * FROM {full_table_name} WHERE 1=0
                """)
                
                # 2. Copy dữ liệu vào bảng tạm
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
                output.seek(0)
                
                columns = ", ".join([f'"{col}"' for col in df.columns])
                copy_query = f"COPY {temp_table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\"')"
                cur.copy_expert(copy_query, output)
                
                # 3. Chuẩn bị mệnh đề conflict
                conflict_constraint = ", ".join([f'"{col}"' for col in key_columns])
                
                # 4. Chuẩn bị mệnh đề update
                if update_columns is None:
                    # Update tất cả cột ngoại trừ key columns
                    update_columns = [col for col in df.columns if col not in key_columns]
                
                update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
                    
                # 5. Thực hiện UPSERT từ bảng tạm vào bảng chính
                upsert_query = f"""
                    INSERT INTO {full_table_name} ({columns})
                    SELECT {columns} FROM {temp_table_name}
                    ON CONFLICT ({conflict_constraint})
                    DO UPDATE SET {update_clause}
                    RETURNING (xmax = 0) AS inserted, (xmax <> 0) AS updated;
                """
                
                cur.execute(upsert_query)
                
                # 6. Đếm kết quả
                results = cur.fetchall()
                result['inserted'] = sum(1 for r in results if r[0])  # True cho inserted
                result['updated'] = sum(1 for r in results if r[1])   # True cho updated
                
                # 7. Commit transaction
                conn.commit()
                
                end_time = datetime.now()
                result['execution_time'] = (end_time - start_time).total_seconds()
                
                logger.info(f"Đã upsert ({result['inserted']} inserted, {result['updated']} updated) " + 
                           f"vào {full_table_name} trong {result['execution_time']:.2f}s")
    
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Lỗi khi thực hiện upsert vào {full_table_name}: {str(e)}")
            raise
        
        finally:
            if conn:
                conn.close()
        
        return result
    
    def execute_batch_query(self, query: str, params_list: List[Dict[str, Any]]) -> int:
        """
        Thực hiện một query nhiều lần với danh sách tham số khác nhau
        
        Args:
            query: Câu SQL query
            params_list: Danh sách các dict tham số
            
        Returns:
            int: Số rows bị ảnh hưởng
        """
        conn = None
        rows_affected = 0
        
        try:
            # Lấy connection
            conn = self._get_connection()
            
            with conn.cursor() as cur:
                for params in params_list:
                    cur.execute(query, params)
                    rows_affected += cur.rowcount
                
                conn.commit()
                
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Lỗi khi thực hiện batch query: {str(e)}")
            raise
        
        finally:
            if conn:
                conn.close()
        
        return rows_affected
    
    def execute_batch_with_values(self, query: str, values_list: List[Tuple], 
                                  page_size: int = 100) -> int:
        """
        Thực hiện batch execute với execute_values của psycopg2
        
        Args:
            query: SQL query với placeholder %s
            values_list: Danh sách các tuple giá trị
            page_size: Số lượng records mỗi batch
            
        Returns:
            int: Số rows bị ảnh hưởng
        """
        conn = None
        rows_affected = 0
        
        try:
            # Lấy connection
            conn = self._get_connection()
            
            with conn.cursor() as cur:
                execute_values(cur, query, values_list, page_size=page_size)
                rows_affected = cur.rowcount
                conn.commit()
                
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Lỗi khi thực hiện batch execute_values: {str(e)}")
            raise
        
        finally:
            if conn:
                conn.close()
        
        return rows_affected 