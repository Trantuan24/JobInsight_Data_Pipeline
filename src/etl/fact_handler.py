#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Module xử lý fact tables và bridge tables
"""
import pandas as pd
import logging
import json
import os
import sys
import duckdb
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Thiết lập logging
logger = logging.getLogger(__name__)

from src.etl.etl_utils import batch_insert_records, lookup_dimension_key, lookup_location_key

try:
    from src.processing.data_prepare import (
        generate_daily_fact_records, calculate_load_month, parse_job_location
    )
except ImportError:
    from processing.data_prepare import (
        generate_daily_fact_records, calculate_load_month, parse_job_location
    )

class FactHandler:
    """
    Class xử lý fact tables và bridge tables
    """
    
    def __init__(self, duck_conn: duckdb.DuckDBPyConnection):
        """
        Khởi tạo FactHandler
        
        Args:
            duck_conn: Kết nối DuckDB
        """
        self.duck_conn = duck_conn
    
    def generate_fact_records(self, staging_records: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """
        Tạo bản ghi fact và bridge từ dữ liệu staging với cơ chế ngăn chặn duplicate
        
        Args:
            staging_records: Dữ liệu từ staging
        
        Returns:
            Tuple chứa (fact_records, bridge_records)
        """
        fact_records = []
        bridge_records = []
        
        for _, job in staging_records.iterrows():
            try:
                # Lookup dimension keys
                job_sk = lookup_dimension_key(
                    self.duck_conn, 'DimJob', 'job_id', job.job_id, 'job_sk'
                )
                
                company_name = job.company_name_standardized if pd.notna(job.company_name_standardized) else job.company_name
                company_sk = lookup_dimension_key(
                    self.duck_conn, 'DimCompany', 'company_name_standardized', company_name, 'company_sk'
                )
                
                # Check if required keys exist
                if not job_sk or not company_sk:
                    logger.warning(f"Bỏ qua job_id={job.job_id}: Không tìm thấy dimension key (job_sk={job_sk}, company_sk={company_sk})")
                    continue
                
                # Xử lý ngày
                due_date = pd.to_datetime(job.due_date) if pd.notna(job.due_date) else None
                posted_time = pd.to_datetime(job.posted_time) if pd.notna(job.posted_time) else None
                crawled_at = pd.to_datetime(job.crawled_at) if pd.notna(job.crawled_at) else datetime.now()
                
                # Tạo danh sách các ngày cần tạo fact records
                daily_dates = generate_daily_fact_records(posted_time, due_date)
                
                # Tính load_month
                load_month = calculate_load_month(crawled_at)
                
                # Tạo fact records cho từng ngày
                for date_id in daily_dates:
                    # Kiểm tra xem fact record đã tồn tại chưa
                    fact_id = self._process_single_fact_record(
                        job, job_sk, company_sk, date_id, due_date, posted_time, crawled_at, load_month
                    )
                    
                    if fact_id:
                        # Thêm fact_id vào danh sách fact_records
                        fact_records.append({
                            'fact_id': fact_id,
                            'job_sk': job_sk,
                            'date_id': date_id,
                            'load_month': load_month
                        })
                        
                        # Xử lý bridge records
                        location_bridges = self._process_location_bridges(job, fact_id)
                        bridge_records.extend(location_bridges)
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý fact record cho job_id={job.job_id}: {e}")
                continue
        
        return fact_records, bridge_records
    
    def _process_single_fact_record(
        self,
        job: pd.Series,
        job_sk: int,
        company_sk: int,
        date_id: datetime,
        due_date: Optional[datetime],
        posted_time: Optional[datetime],
        crawled_at: datetime,
        load_month: str
    ) -> Optional[int]:
        """
        Xử lý một fact record đơn lẻ
        
        Args:
            job: Bản ghi job từ staging
            job_sk: Surrogate key của job
            company_sk: Surrogate key của company
            date_id: Ngày cần tạo fact record
            due_date: Ngày hết hạn job
            posted_time: Thời gian đăng job
            crawled_at: Thời gian crawl
            load_month: Tháng load dữ liệu
            
        Returns:
            fact_id hoặc None nếu có lỗi
        """
        try:
            # Kiểm tra xem fact record đã tồn tại chưa
            check_query = """
                SELECT fact_id FROM FactJobPostingDaily 
                WHERE job_sk = ? AND date_id = ?
            """
            existing_fact = self.duck_conn.execute(check_query, [job_sk, date_id]).fetchone()
            
            if existing_fact:
                # Đã tồn tại, chỉ cập nhật một số field quan trọng
                fact_id = existing_fact[0]
                
                update_query = """
                    UPDATE FactJobPostingDaily 
                    SET 
                        time_remaining = ?,
                        crawled_at = ?,
                        load_month = ?
                    WHERE fact_id = ?
                """
                
                update_values = [
                    job.time_remaining if pd.notna(job.time_remaining) else None,
                    crawled_at,
                    load_month,
                    fact_id
                ]
                
                try:
                    self.duck_conn.execute(update_query, update_values)
                    logger.debug(f"Updated existing fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                except Exception as e:
                    logger.error(f"Lỗi khi update fact record cho job_id={job.job_id}, date={date_id}: {e}")
                    return None
                    
            else:
                # Chưa tồn tại, tạo mới với transaction để đảm bảo tính nhất quán
                try:
                    # Bắt đầu transaction
                    self.duck_conn.execute("BEGIN TRANSACTION")
                    
                    # Kiểm tra lại một lần nữa để tránh race condition
                    double_check = self.duck_conn.execute(check_query, [job_sk, date_id]).fetchone()
                    if double_check:
                        # Có thể đã được tạo bởi một process khác
                        self.duck_conn.execute("ROLLBACK")
                        return double_check[0]
                    
                    # Tạo fact record mới
                    fact_record = {
                        'job_sk': job_sk,
                        'company_sk': company_sk,
                        'date_id': date_id,
                        'salary_min': job.salary_min if pd.notna(job.salary_min) else None,
                        'salary_max': job.salary_max if pd.notna(job.salary_max) else None,
                        'salary_type': job.salary_type if pd.notna(job.salary_type) else None,
                        'due_date': due_date,
                        'time_remaining': job.time_remaining if pd.notna(job.time_remaining) else None,
                        'verified_employer': job.verified_employer if pd.notna(job.verified_employer) else False,
                        'posted_time': posted_time,
                        'crawled_at': crawled_at,
                        'load_month': load_month
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
                    
                    result = self.duck_conn.execute(insert_query, values).fetchone()
                    if not result:
                        self.duck_conn.execute("ROLLBACK")
                        logger.warning(f"Không thể insert fact record cho job_id={job.job_id}, date={date_id}")
                        return None
                        
                    fact_id = result[0]
                    
                    # Commit transaction
                    self.duck_conn.execute("COMMIT")
                    
                    logger.debug(f"Inserted new fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                    
                except Exception as e:
                    # Rollback nếu có lỗi
                    self.duck_conn.execute("ROLLBACK")
                    logger.error(f"Lỗi khi insert fact record cho job_id={job.job_id}, date={date_id}: {e}")
                    return None
            
            return fact_id
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý fact record cho job_id={job.job_id}, date={date_id}: {e}")
            return None
    
    def _process_location_bridges(self, job: pd.Series, fact_id: int) -> List[Dict]:
        """
        Xử lý location bridges cho một fact record
        
        Args:
            job: Bản ghi job từ staging
            fact_id: ID của fact record
            
        Returns:
            List các bridge records
        """
        bridge_records = []
        
        try:
            # Xóa bridge records cũ cho fact_id này
            self.duck_conn.execute("DELETE FROM FactJobLocationBridge WHERE fact_id = ?", [fact_id])
            
            # Lấy location string
            location_str = None
            
            # Ưu tiên sử dụng location_pairs nếu có
            if hasattr(job, 'location_pairs'):
                try:
                    location_pairs_value = getattr(job, 'location_pairs')
                    if location_pairs_value is not None and str(location_pairs_value).lower() not in ['nan', 'none', '']:
                        location_str = str(location_pairs_value)
                except:
                    pass
                    
            # Fallback về location nếu không có location_pairs
            if not location_str and hasattr(job, 'location'):
                try:
                    location_value = getattr(job, 'location')
                    if location_value is not None and str(location_value).lower() not in ['nan', 'none', '']:
                        location_str = str(location_value)
                except:
                    pass
            
            if location_str:
                # Parse location string thành các tuple (province, city, district)
                parsed_locations = parse_job_location(location_str)
                
                location_sks_added = set()  # Tránh duplicate locations cho cùng 1 fact_id
                
                for province, city, district in parsed_locations:
                    location_sk = lookup_location_key(self.duck_conn, province, city, district)
                    
                    if location_sk and location_sk not in location_sks_added:
                        bridge_records.append({'fact_id': fact_id, 'location_sk': location_sk})
                        location_sks_added.add(location_sk)
                    elif not location_sk:
                        # Nếu không tìm thấy exact match, thử tìm Unknown
                        unknown_location_sk = lookup_location_key(self.duck_conn, None, 'Unknown', None)
                        if unknown_location_sk and unknown_location_sk not in location_sks_added:
                            bridge_records.append({'fact_id': fact_id, 'location_sk': unknown_location_sk})
                            location_sks_added.add(unknown_location_sk)
            else:
                # Không có location, sử dụng Unknown
                unknown_location_sk = lookup_location_key(self.duck_conn, None, 'Unknown', None)
                if unknown_location_sk:
                    bridge_records.append({'fact_id': fact_id, 'location_sk': unknown_location_sk})
            
            # Batch insert bridge records
            if bridge_records:
                batch_insert_records(
                    self.duck_conn, 
                    'FactJobLocationBridge', 
                    bridge_records, 
                    on_conflict="ON CONFLICT (fact_id, location_sk) DO NOTHING"
                )
                
        except Exception as e:
            logger.error(f"Lỗi khi xử lý location bridges cho fact_id={fact_id}: {e}")
        
        return bridge_records
    
    def cleanup_duplicate_fact_records(self):
        """
        Dọn dẹp các duplicate records trong FactJobPostingDaily và FactJobLocationBridge
        """
        logger.info("Bắt đầu dọn dẹp duplicate fact records...")
        
        try:
            # 1. Backup bridge records trước khi xóa
            logger.info("Backup bridge records...")
            backup_bridge_query = """
                CREATE OR REPLACE TEMP TABLE bridge_backup AS
                SELECT DISTINCT fact_id, location_sk 
                FROM FactJobLocationBridge
            """
            self.duck_conn.execute(backup_bridge_query)
            
            # 2. Tìm và xóa duplicate fact records, giữ lại record có fact_id nhỏ nhất
            logger.info("Tìm duplicate fact records...")
            find_duplicates_query = """
                SELECT job_sk, date_id, COUNT(*) as count, MIN(fact_id) as keep_fact_id
                FROM FactJobPostingDaily
                GROUP BY job_sk, date_id
                HAVING COUNT(*) > 1
            """
            
            duplicates = self.duck_conn.execute(find_duplicates_query).fetchdf()
            
            if not duplicates.empty:
                logger.info(f"Tìm thấy {len(duplicates)} nhóm duplicate fact records")
                
                total_deleted = 0
                for _, dup in duplicates.iterrows():
                    job_sk, date_id, count, keep_fact_id = dup['job_sk'], dup['date_id'], dup['count'], dup['keep_fact_id']
                    
                    # Lấy danh sách fact_id cần xóa (tất cả trừ keep_fact_id)
                    get_delete_ids_query = """
                        SELECT fact_id FROM FactJobPostingDaily
                        WHERE job_sk = ? AND date_id = ? AND fact_id != ?
                    """
                    delete_ids = self.duck_conn.execute(get_delete_ids_query, [job_sk, date_id, keep_fact_id]).fetchall()
                    
                    if delete_ids:
                        fact_ids_to_delete = [row[0] for row in delete_ids]
                        placeholders = ','.join(['?'] * len(fact_ids_to_delete))
                        
                        # Xóa bridge records trước
                        delete_bridge_query = f"""
                            DELETE FROM FactJobLocationBridge 
                            WHERE fact_id IN ({placeholders})
                        """
                        self.duck_conn.execute(delete_bridge_query, fact_ids_to_delete)
                        
                        # Xóa fact records
                        delete_fact_query = f"""
                            DELETE FROM FactJobPostingDaily 
                            WHERE fact_id IN ({placeholders})
                        """
                        self.duck_conn.execute(delete_fact_query, fact_ids_to_delete)
                        
                        total_deleted += len(fact_ids_to_delete)
                        logger.debug(f"Đã xóa {len(fact_ids_to_delete)} duplicate records cho job_sk={job_sk}, date_id={date_id}")
                
                logger.info(f"Đã xóa tổng cộng {total_deleted} duplicate fact records")
                
                # 3. Restore bridge records cho các fact_id còn lại
                logger.info("Restore bridge records...")
                restore_bridge_query = """
                    INSERT INTO FactJobLocationBridge (fact_id, location_sk)
                    SELECT DISTINCT b.fact_id, b.location_sk
                    FROM bridge_backup b
                    JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM FactJobLocationBridge fb 
                        WHERE fb.fact_id = b.fact_id AND fb.location_sk = b.location_sk
                    )
                """
                self.duck_conn.execute(restore_bridge_query)
                
            else:
                logger.info("Không tìm thấy duplicate fact records")
                
            # 4. Thống kê sau khi dọn dẹp
            stats_query = """
                SELECT 
                    COUNT(*) as total_facts,
                    (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
                FROM FactJobPostingDaily
            """
            stats = self.duck_conn.execute(stats_query).fetchone()
            logger.info(f"Sau dọn dẹp: {stats[0]} fact records, {stats[1]} unique combinations")
            
            if stats[0] != stats[1]:
                logger.warning(f"Vẫn còn {stats[0] - stats[1]} duplicate records!")
            else:
                logger.info("✅ Đã dọn dẹp thành công tất cả duplicate records")
                
        except Exception as e:
            logger.error(f"Lỗi khi dọn dẹp duplicate records: {e}")
            raise