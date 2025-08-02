#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module xử lý dimension tables với SCD Type 2
"""
import pandas as pd
import logging
import json
import os
import sys
import duckdb
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Thiết lập logging
logger = logging.getLogger(__name__)

from src.etl.etl_utils import batch_insert_records

class DimensionHandler:
    """
    Class xử lý dimension tables với SCD Type 2
    """
    
    def __init__(self, duck_conn: duckdb.DuckDBPyConnection):
        """
        Khởi tạo DimensionHandler
        
        Args:
            duck_conn: Kết nối DuckDB
        """
        self.duck_conn = duck_conn
    
    def process_dimension_with_scd2(
        self,
        staging_records: pd.DataFrame,
        dim_table: str,
        prepare_function,
        natural_key: str,
        surrogate_key: str,
        compare_columns: List[str]
    ) -> Dict[str, int]:
        """
        Xử lý dimension table với SCD Type 2
        
        Args:
            staging_records: Dữ liệu staging
            dim_table: Tên bảng dimension
            prepare_function: Function chuẩn bị dữ liệu
            natural_key: Natural key column
            surrogate_key: Surrogate key column
            compare_columns: Columns để so sánh thay đổi
        
        Returns:
            Dict thống kê insert/update
        """
        logger.info(f"Xử lý {dim_table} với SCD Type 2")
        
        # Chuẩn bị dữ liệu
        prepared_data = prepare_function(staging_records)
        
        if prepared_data.empty:
            logger.warning(f"Không có dữ liệu để xử lý cho {dim_table}")
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}
        
        # Kiểm tra thay đổi
        to_insert, to_update, unchanged = self.check_dimension_changes(
            prepared_data, dim_table, natural_key, compare_columns
        )
        
        stats = {
            'inserted': 0,
            'updated': 0,
            'unchanged': len(unchanged)
        }
        
        # Áp dụng updates (SCD Type 2)
        if to_update:
            self.apply_scd_type2_updates(dim_table, surrogate_key, to_update)
            stats['updated'] = len(to_update)
        
        # Insert records mới
        if not to_insert.empty:
            insert_records = []
            for _, record in to_insert.iterrows():
                record_dict = record.to_dict()
                # Loại bỏ surrogate key
                if surrogate_key in record_dict:
                    del record_dict[surrogate_key]
                insert_records.append(record_dict)
            
            stats['inserted'] = batch_insert_records(self.duck_conn, dim_table, insert_records)
        
        logger.info(f"{dim_table} - Inserted: {stats['inserted']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")
        return stats
    
    def check_dimension_changes(
        self,
        new_records: pd.DataFrame,
        dim_table: str,
        natural_key: str,
        compare_columns: List[str]
    ) -> Tuple[pd.DataFrame, List[Dict], pd.DataFrame]:
        """
        Kiểm tra thay đổi trong dimension table (SCD Type 2)
        
        Args:
            new_records: Dữ liệu mới cần kiểm tra
            dim_table: Tên bảng dimension
            natural_key: Cột natural key để so sánh
            compare_columns: Các cột cần so sánh để detect changes
        
        Returns:
            Tuple chứa (records_to_insert, records_to_update, unchanged_records)
        """
        records_to_insert = []
        records_to_update = []
        unchanged_records = []
        
        try:
            for _, new_record in new_records.iterrows():
                natural_key_value = new_record[natural_key]
                
                # Tìm bản ghi hiện tại trong dimension
                query = f"""
                    SELECT * FROM {dim_table}
                    WHERE {natural_key} = ? AND is_current = TRUE
                """
                
                existing = self.duck_conn.execute(query, [natural_key_value]).fetchdf()
                
                if existing.empty:
                    # Bản ghi mới hoàn toàn
                    records_to_insert.append(new_record)
                else:
                    # So sánh các trường để xem có thay đổi không
                    existing_record = existing.iloc[0]
                    has_changes = False
                    
                    for col in compare_columns:
                        if col in new_record.index and col in existing_record.index:
                            old_val = existing_record[col]
                            new_val = new_record[col]
                            
                            # Handle null values
                            old_is_null = pd.isna(old_val) if old_val is not None else True
                            new_is_null = pd.isna(new_val) if new_val is not None else True
                            
                            if old_is_null and new_is_null:
                                continue
                            elif old_is_null != new_is_null:
                                has_changes = True
                                break
                            elif str(old_val) != str(new_val):
                                has_changes = True
                                break
                    
                    if has_changes:
                        # Cần update: đóng record cũ và tạo record mới
                        records_to_update.append({
                            'old_record': existing_record,
                            'new_record': new_record
                        })
                    else:
                        # Không có thay đổi
                        unchanged_records.append(new_record)
        
        except Exception as e:
            logger.error(f"Lỗi khi check dimension changes: {e}")
            # Fallback: coi tất cả là insert mới
            records_to_insert = new_records.to_dict('records')
        
        return (
            pd.DataFrame(records_to_insert) if records_to_insert else pd.DataFrame(),
            records_to_update,
            pd.DataFrame(unchanged_records) if unchanged_records else pd.DataFrame()
        )
    
    def apply_scd_type2_updates(self, dim_table: str, surrogate_key: str, updates: List[Dict]):
        """
        Áp dụng SCD Type 2 updates: đóng bản ghi cũ và tạo bản ghi mới
        
        Args:
            dim_table: Tên bảng dimension
            surrogate_key: Tên cột surrogate key
            updates: List các bản ghi cần update
        """
        today = datetime.now().date()
        
        for update_info in updates:
            old_record = update_info['old_record']
            new_record = update_info['new_record']
            try:
                # 1. Đóng bản ghi cũ
                surrogate_key_value = old_record[surrogate_key]
                if hasattr(surrogate_key_value, 'item'):
                    surrogate_key_value = surrogate_key_value.item()  # Chuyển numpy.int32 -> int
                
                update_query = f"""
                    UPDATE {dim_table}
                    SET expiry_date = ?, is_current = FALSE
                    WHERE {surrogate_key} = ?
                """
                self.duck_conn.execute(update_query, [today, surrogate_key_value])
                
                # 2. Insert bản ghi mới với UPSERT để tránh duplicate key
                new_record_dict = new_record.to_dict() if hasattr(new_record, 'to_dict') else dict(new_record)

                # Loại bỏ mọi trường liên quan đến surrogate key
                for key in list(new_record_dict.keys()):
                    if key.lower() == surrogate_key.lower():
                        del new_record_dict[key]

                # Đặt effective_date và is_current
                new_record_dict['effective_date'] = today
                new_record_dict['is_current'] = True
                new_record_dict['expiry_date'] = None

                # Handle JSON values
                for key, val in new_record_dict.items():
                    if isinstance(val, (dict, list)):
                        new_record_dict[key] = json.dumps(val)

                # FIXED: Use UPSERT to handle potential duplicates
                columns = list(new_record_dict.keys())
                placeholders = ', '.join(['?'] * len(columns))
                values = [new_record_dict[col] for col in columns]

                # Determine natural key for UPSERT
                if dim_table == 'DimJob':
                    natural_key = 'job_id'
                elif dim_table == 'DimCompany':
                    natural_key = 'company_name_standardized'
                elif dim_table == 'DimLocation':
                    natural_key = 'city'  # Simplified for now
                else:
                    natural_key = 'id'

                # Build UPSERT query
                update_columns = [col for col in columns if col != natural_key]
                update_stmt = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

                upsert_query = f"""
                    INSERT INTO {dim_table} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT ({natural_key})
                    DO UPDATE SET {update_stmt}
                """
                self.duck_conn.execute(upsert_query, values)
                
                logger.info(f"Updated {dim_table}: closed old record {old_record[surrogate_key]} and created new record")
                
            except Exception as e:
                logger.error(f"Lỗi khi apply SCD2 update cho {dim_table}: {e}")
    
    def process_location_dimension(self, staging_records: pd.DataFrame, prepare_location_function) -> Dict[str, int]:
        """
        Xử lý đặc biệt cho DimLocation với composite key
        
        Args:
            staging_records: Dữ liệu staging
            prepare_location_function: Function chuẩn bị dữ liệu location
            
        Returns:
            Dict thống kê insert/update
        """
        logger.info("Xử lý DimLocation với composite key")
        
        # Chuẩn bị dữ liệu
        dim_location_df = prepare_location_function(staging_records)
        
        if dim_location_df.empty:
            logger.warning("Không có dữ liệu để xử lý cho DimLocation")
            return {'inserted': 0, 'updated': 0, 'unchanged': 0}
        
        # Kiểm tra location đã tồn tại chưa để tránh duplicate
        location_records = []
        for _, location in dim_location_df.iterrows():
            province = location['province']
            city = location['city']
            district = location['district']
            
            # Kiểm tra location đã tồn tại chưa
            query = """
                SELECT location_sk 
                FROM DimLocation
                WHERE (province IS NULL AND ? IS NULL OR province = ?)
                AND city = ?
                AND (district IS NULL AND ? IS NULL OR district = ?)
                AND is_current = TRUE
            """
            
            params = [province, province, city, district, district]
            existing = self.duck_conn.execute(query, params).fetchone()
            
            if not existing:
                # Location mới
                location_dict = location.to_dict()
                if 'location_sk' in location_dict:
                    del location_dict['location_sk']
                location_records.append(location_dict)
        
        # Insert location mới
        inserted = 0
        if location_records:
            inserted = batch_insert_records(self.duck_conn, 'DimLocation', location_records)
            
        stats = {
            'inserted': inserted,
            'updated': 0,  # DimLocation không áp dụng update SCD2
            'unchanged': len(dim_location_df) - len(location_records)
        }
        
        logger.info(f"DimLocation - Inserted: {stats['inserted']}, Unchanged: {stats['unchanged']}")
        return stats