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

# Thiết lập logging
logger = logging.getLogger(__name__)

from .etl_utils import batch_insert_records, lookup_dimension_key, lookup_location_key
from src.common.decorators import retry

try:
    from src.processing.data_prepare import (
        generate_daily_fact_records, calculate_load_month, parse_job_location
    )
except ImportError:
    # Fallback cho trường hợp chạy trực tiếp script này
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
    
    @retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=[Exception])
    def generate_fact_records(self, staging_records: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """
        Tạo fact records và bridge records từ staging data
        
        Args:
            staging_records: DataFrame chứa các bản ghi từ staging
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (fact_records, bridge_records)
        """
        fact_records = []
        bridge_records = []
        
        # Lọc các bản ghi có đủ thông tin
        if staging_records.empty:
            logger.warning("Không có dữ liệu staging để xử lý!")
            return [], []
            
        # Lấy thời gian hiện tại cho crawled_at và load_month
        crawled_at = datetime.now()
        load_month = crawled_at.strftime('%Y-%m')
        
        # Danh sách các ngày cần tạo fact record (hôm nay và vài ngày tới)
        # Do jobs thường hiển thị trong nhiều ngày
        today = datetime.now().date()
        dates_to_create = [today + timedelta(days=i) for i in range(5)]  # Hôm nay và 4 ngày tiếp theo
        
        # Tạo một bản sao để tránh SettingWithCopyWarning
        staging_df = staging_records.copy()
        
        # Đảm bảo các cột cần thiết tồn tại
        for col in ['job_id', 'title_clean', 'company_name_standardized', 'due_date']:
            if col not in staging_df.columns:
                logger.error(f"Cột {col} không tồn tại trong dữ liệu staging!")
                return [], []
                
        # Xử lý từng bản ghi staging
        job_skipped = 0
        bridge_skipped = 0
        
        # Lấy toàn bộ DimJob current
        dim_jobs = self.duck_conn.execute("""
            SELECT job_id, job_sk FROM DimJob WHERE is_current = TRUE
        """).fetchdf()
        job_id_to_sk = dict(zip(dim_jobs['job_id'], dim_jobs['job_sk']))
        
        # Lấy toàn bộ DimCompany current
        dim_companies = self.duck_conn.execute("""
            SELECT company_name_standardized, company_sk FROM DimCompany WHERE is_current = TRUE
        """).fetchdf()
        company_to_sk = dict(zip(dim_companies['company_name_standardized'], dim_companies['company_sk']))
        
        # Lấy toàn bộ DimLocation current để cache
        dim_locations = self.duck_conn.execute("""
            SELECT location_sk, province, city, district 
            FROM DimLocation 
            WHERE is_current = TRUE
        """).fetchdf()
        
        # Tạo cache cho location lookups
        location_cache = {}
        for _, loc in dim_locations.iterrows():
            # Tạo key từ province, city, district
            cache_key = f"{loc['province'] or 'None'}:{loc['city']}:{loc['district'] or 'None'}"
            location_cache[cache_key] = loc['location_sk']
        
        # Tạo các fact records và bridge records
        for _, job in staging_df.iterrows():
            # Lấy surrogate keys
            job_id = str(job['job_id'])
            job_sk = job_id_to_sk.get(job_id)
            
            company_name = job['company_name_standardized']
            company_sk = company_to_sk.get(company_name)
            
            # Kiểm tra xem job và company có tồn tại trong dimension tables không
            if not job_sk:
                logger.warning(f"Job ID {job_id} không tồn tại trong DimJob, bỏ qua...")
                job_skipped += 1
                continue
                
            if not company_sk:
                logger.warning(f"Company {company_name} không tồn tại trong DimCompany, bỏ qua...")
                job_skipped += 1
                continue
                
            # Parse posted_time và due_date
            posted_time = None
            if hasattr(job, 'posted_time') and pd.notna(job.posted_time):
                try:
                    posted_time = pd.to_datetime(job.posted_time)
                except:
                    pass
                    
            due_date = None
            if hasattr(job, 'due_date') and pd.notna(job.due_date):
                try:
                    due_date = pd.to_datetime(job.due_date)
                except:
                    pass
                    
            # Tạo fact records cho mỗi ngày trong khoảng hiển thị
            for date_id in dates_to_create:
                # Xử lý một fact record
                fact_id = self._process_single_fact_record(
                    job, job_sk, company_sk, date_id, due_date, 
                    posted_time, crawled_at, load_month
                )
                
                if fact_id:
                    # Thêm vào danh sách kết quả
                    fact_records.append({
                        'fact_id': fact_id,
                        'job_sk': job_sk,
                        'company_sk': company_sk,
                        'date_id': date_id
                    })
                    
                    # Xử lý location bridges
                    bridges = self._process_location_bridges(job, fact_id, location_cache)
                    if bridges:
                        bridge_records.extend(bridges)
                    else:
                        bridge_skipped += 1
                
        logger.info(f"Đã tạo {len(fact_records)} fact records và {len(bridge_records)} bridge records")
        logger.info(f"Đã bỏ qua {job_skipped} jobs và {bridge_skipped} bridges")
        
        return fact_records, bridge_records
    
    @retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=[Exception])
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
        Xử lý một fact record duy nhất cho một job và một ngày
        
        Args:
            job: Bản ghi job từ staging
            job_sk: Surrogate key của job
            company_sk: Surrogate key của company
            date_id: Ngày cần tạo fact record
            due_date: Ngày hết hạn job
            posted_time: Thời điểm đăng job
            crawled_at: Thời điểm crawl dữ liệu
            load_month: Tháng load dữ liệu (YYYY-MM)
            
        Returns:
            fact_id nếu thành công, None nếu thất bại
        """
        try:
            # Kiểm tra xem đã có fact record cho job_sk và date_id này chưa
            check_query = """
                SELECT fact_id FROM FactJobPostingDaily
                WHERE job_sk = ? AND date_id = ?
            """
            
            existing = self.duck_conn.execute(check_query, [job_sk, date_id]).fetchone()
            
            if existing:
                # Đã tồn tại, cập nhật thông tin
                fact_id = existing[0]
                
                # Sử dụng transaction để đảm bảo tính nhất quán khi update
                try:
                    # Bắt đầu transaction
                    self.duck_conn.execute("BEGIN TRANSACTION")
                    
                    # Kiểm tra lại xem fact_id có tồn tại không
                    check_fact_id = self.duck_conn.execute(
                        "SELECT 1 FROM FactJobPostingDaily WHERE fact_id = ?", 
                        [fact_id]
                    ).fetchone()
                    
                    if not check_fact_id:
                        # fact_id không tồn tại, có thể đã bị xóa
                        logger.warning(f"fact_id {fact_id} không tồn tại trong bảng FactJobPostingDaily, thử tạo mới...")
                        self.duck_conn.execute("ROLLBACK")
                        
                        # Chuyển sang tạo mới với fact_id mới
                        return self._create_new_fact_record(job, job_sk, company_sk, date_id, due_date, posted_time, crawled_at, load_month)
                    
                    # Cập nhật các thông tin có thể thay đổi
                    update_query = """
                        UPDATE FactJobPostingDaily
                        SET 
                            company_sk = ?,
                            salary_min = ?,
                            salary_max = ?,
                            salary_type = ?,
                            due_date = ?,
                            time_remaining = ?,
                            crawled_at = ?,
                            load_month = ?
                        WHERE fact_id = ?
                    """
                    
                    update_values = [
                        company_sk,
                        job.salary_min if pd.notna(job.salary_min) else None,
                        job.salary_max if pd.notna(job.salary_max) else None,
                        job.salary_type if pd.notna(job.salary_type) else None,
                        due_date,
                        job.time_remaining if pd.notna(job.time_remaining) else None,
                        crawled_at,
                        load_month,
                        fact_id
                    ]
                    
                    try:
                        self.duck_conn.execute(update_query, update_values)
                        logger.debug(f"Updated existing fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                        self.duck_conn.execute("COMMIT")
                        return fact_id  # Trả về fact_id sau khi update thành công
                    except Exception as e:
                        self.duck_conn.execute("ROLLBACK")
                        logger.error(f"Lỗi khi update fact record cho job_id={job.job_id}, date={date_id}: {e}")
                        
                        # Thử tạo mới với fact_id mới
                        logger.info(f"Thử tạo mới fact record cho job_id={job.job_id}, date={date_id} với fact_id mới...")
                        return self._create_new_fact_record(job, job_sk, company_sk, date_id, due_date, posted_time, crawled_at, load_month)
                        
                except Exception as e:
                    try:
                        self.duck_conn.execute("ROLLBACK")
                    except:
                        pass
                    logger.error(f"Lỗi transaction khi update fact record cho job_id={job.job_id}, date={date_id}: {e}")
                    return None
            else:
                # Chưa tồn tại, tạo mới
                return self._create_new_fact_record(job, job_sk, company_sk, date_id, due_date, posted_time, crawled_at, load_month)
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý fact record cho job_id={job.job_id}, date={date_id}: {e}")
            return None
    
    @retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=[Exception])
    def _process_location_bridges(self, job: pd.Series, fact_id: int, location_cache: Dict) -> List[Dict]:
        """
        Xử lý location bridges cho một fact record
        
        Args:
            job: Bản ghi job từ staging
            fact_id: ID của fact record
            location_cache: Cache cho location lookups
            
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
                    # Sử dụng cache thay vì truy vấn database
                    cache_key = f"{province or 'None'}:{city}:{district or 'None'}"
                    location_sk = location_cache.get(cache_key)
                    
                    if location_sk and location_sk not in location_sks_added:
                        bridge_records.append({'fact_id': fact_id, 'location_sk': location_sk})
                        location_sks_added.add(location_sk)
                    elif not location_sk:
                        # Nếu không tìm thấy trong cache, thử tìm Unknown
                        unknown_key = f"None:Unknown:None"
                        unknown_location_sk = location_cache.get(unknown_key)
                        
                        # Nếu không có trong cache, truy vấn database
                        if not unknown_location_sk:
                            unknown_location_sk = lookup_location_key(self.duck_conn, None, 'Unknown', None)
                            # Cập nhật cache
                            if unknown_location_sk:
                                location_cache[unknown_key] = unknown_location_sk
                        
                        if unknown_location_sk and unknown_location_sk not in location_sks_added:
                            bridge_records.append({'fact_id': fact_id, 'location_sk': unknown_location_sk})
                            location_sks_added.add(unknown_location_sk)
            else:
                # Không có location, sử dụng Unknown
                unknown_key = f"None:Unknown:None"
                unknown_location_sk = location_cache.get(unknown_key)
                
                # Nếu không có trong cache, truy vấn database
                if not unknown_location_sk:
                    unknown_location_sk = lookup_location_key(self.duck_conn, None, 'Unknown', None)
                    # Cập nhật cache
                    if unknown_location_sk:
                        location_cache[unknown_key] = unknown_location_sk
                
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
        Dọn dẹp các fact records bị trùng lặp (job_sk + date_id)
        
        Returns:
            Dict thông tin kết quả
        """
        logger.info("Bắt đầu dọn dẹp duplicate fact records...")
        
        try:
            # Tạo transaction để đảm bảo tính nhất quán
            self.duck_conn.execute("BEGIN TRANSACTION")
            
            try:
                # Tắt foreign key constraints nếu có thể
                try:
                    self.duck_conn.execute("PRAGMA foreign_keys = OFF")
                except Exception as e:
                    logger.warning(f"Không thể tắt foreign keys - có thể DuckDB không hỗ trợ")
                
                # Tạo bảng backup
                logger.info("Tạo bảng backup...")
                self.duck_conn.execute("DROP TABLE IF EXISTS temp_fact_backup")
                self.duck_conn.execute("CREATE TEMP TABLE temp_fact_backup AS SELECT * FROM FactJobPostingDaily")
                
                # Tìm các bản ghi trùng lặp và lưu vào bảng tạm để tránh các vấn đề với cursor
                logger.info("Tìm duplicate fact records...")
                self.duck_conn.execute("""
                    DROP TABLE IF EXISTS temp_duplicates;
                    CREATE TEMP TABLE temp_duplicates AS
                    WITH duplicates AS (
                        SELECT job_sk, date_id, COUNT(*) as count, MIN(fact_id) as min_fact_id
                        FROM FactJobPostingDaily
                        GROUP BY job_sk, date_id
                        HAVING COUNT(*) > 1
                    )
                    SELECT f.fact_id, f.job_sk, f.date_id, d.min_fact_id
                    FROM FactJobPostingDaily f
                    JOIN duplicates d ON f.job_sk = d.job_sk AND f.date_id = d.date_id
                    WHERE f.fact_id != d.min_fact_id
                    ORDER BY f.job_sk, f.date_id
                """)
                
                # Đếm số lượng bản ghi trùng lặp
                duplicate_count = self.duck_conn.execute("SELECT COUNT(*) FROM temp_duplicates").fetchone()[0]
                
                if duplicate_count > 0:
                    logger.warning(f"Tìm thấy {duplicate_count} duplicate fact records")
                    
                    # Tạo bảng backup cho bridge
                    self.duck_conn.execute("DROP TABLE IF EXISTS temp_bridge_backup")
                    self.duck_conn.execute("CREATE TEMP TABLE temp_bridge_backup AS SELECT * FROM FactJobLocationBridge")
                    
                    # Cập nhật các bridge records để trỏ đến fact_id mới
                    logger.info("Cập nhật bridge records...")
                    self.duck_conn.execute("""
                        -- Chuyển các bridge records từ fact_id cũ sang fact_id mới
                        INSERT INTO FactJobLocationBridge (fact_id, location_sk)
                        SELECT DISTINCT d.min_fact_id, b.location_sk
                        FROM FactJobLocationBridge b
                        JOIN temp_duplicates d ON b.fact_id = d.fact_id
                        WHERE NOT EXISTS (
                            SELECT 1 FROM FactJobLocationBridge b2
                            WHERE b2.fact_id = d.min_fact_id AND b2.location_sk = b.location_sk
                        );
                        
                        -- Xóa các bridge records trỏ đến fact_id cũ
                        DELETE FROM FactJobLocationBridge
                        WHERE fact_id IN (SELECT fact_id FROM temp_duplicates);
                    """)
                    
                    # Xóa các fact records trùng lặp
                    logger.info("Xóa duplicate fact records...")
                    self.duck_conn.execute("""
                        DELETE FROM FactJobPostingDaily
                        WHERE fact_id IN (SELECT fact_id FROM temp_duplicates)
                    """)
                    
                else:
                    logger.info("Không tìm thấy duplicate fact records")
                
                # Kiểm tra kết quả
                check_after = self.duck_conn.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM FactJobPostingDaily) as total_facts,
                        (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
                """).fetchone()
                
                # Xóa các bảng tạm
                self.duck_conn.execute("DROP TABLE IF EXISTS temp_duplicates")
                
                # Bật lại foreign key constraints
                try:
                    self.duck_conn.execute("PRAGMA foreign_keys = ON")
                except:
                    pass
                
                # Commit transaction
                self.duck_conn.execute("COMMIT")
                
                logger.info(f"Sau dọn dẹp: {check_after[0]} fact records, {check_after[1]} unique combinations")
                
                # Kiểm tra xem còn duplicate records không
                if check_after[0] != check_after[1]:
                    logger.warning(f"Vẫn còn {check_after[0] - check_after[1]} duplicate records sau khi dọn dẹp!")
                
                return {
                    "success": True,
                    "duplicate_count": duplicate_count,
                    "remaining_facts": check_after[0],
                    "unique_combinations": check_after[1],
                    "is_clean": check_after[0] == check_after[1]
                }
                
            except Exception as e:
                # Rollback transaction nếu có lỗi
                self.duck_conn.execute("ROLLBACK")
                logger.error(f"Lỗi khi dọn dẹp duplicate fact records, đã rollback: {e}")
                return {
                    "success": False,
                    "error": str(e)
                }
                
        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng khi dọn dẹp duplicate fact records: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _create_new_fact_record(
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
        Tạo mới một fact record
        
        Args:
            job: Bản ghi job từ staging
            job_sk: Surrogate key của job
            company_sk: Surrogate key của company
            date_id: Ngày cần tạo fact record
            due_date: Ngày hết hạn job
            posted_time: Thời điểm đăng job
            crawled_at: Thời điểm crawl dữ liệu
            load_month: Tháng load dữ liệu (YYYY-MM)
            
        Returns:
            fact_id nếu thành công, None nếu thất bại
        """
        try:
            # Bắt đầu transaction
            self.duck_conn.execute("BEGIN TRANSACTION")
            
            try:
                # Kiểm tra xem đã có fact record với job_sk và date_id này chưa
                double_check = self.duck_conn.execute(
                    "SELECT fact_id FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", 
                    [job_sk, date_id]
                ).fetchone()
                
                if double_check:
                    # Có thể đã được tạo bởi một process khác
                    logger.debug(f"Đã tìm thấy fact record trong lần kiểm tra thứ hai: job_sk={job_sk}, date_id={date_id}")
                    self.duck_conn.execute("COMMIT")
                    return double_check[0]
                
                # Import các module cần thiết
                import time
                import uuid
                
                # Tạo fact_id duy nhất sử dụng UUID và timestamp để đảm bảo tính duy nhất cao
                unique_id = uuid.uuid4().int
                timestamp_ms = int(time.time() * 1000)
                
                # Sử dụng một phạm vi giá trị mới cho fact_id để tránh xung đột với các giá trị cũ
                # Bắt đầu từ 5000000 để tránh xung đột với các giá trị cũ (1000000-4999999)
                fact_id = 5000000 + (abs(hash(f"{job_sk}_{date_id}_{uuid.uuid4()}")) + timestamp_ms) % (10**7)
                
                # Kiểm tra xem fact_id đã tồn tại chưa
                check_id_query = "SELECT 1 FROM FactJobPostingDaily WHERE fact_id = ?"
                id_exists = self.duck_conn.execute(check_id_query, [fact_id]).fetchone() is not None
                
                # Nếu fact_id đã tồn tại, tạo một giá trị khác
                retry_count = 0
                while id_exists and retry_count < 10:  # Tăng số lần retry lên 10
                    retry_count += 1
                    # Tạo một fact_id mới với offset khác nhau cho mỗi lần thử
                    fact_id = 5000000 + (abs(hash(f"{job_sk}_{date_id}_{uuid.uuid4()}_{retry_count}")) + timestamp_ms + retry_count * 10000) % (10**7)
                    id_exists = self.duck_conn.execute(check_id_query, [fact_id]).fetchone() is not None
                
                if id_exists:
                    logger.warning(f"Không thể tạo fact_id duy nhất sau 10 lần thử cho job_sk={job_sk}, date_id={date_id}")
                    self.duck_conn.execute("ROLLBACK")
                    return None
                
                # Tạo fact record mới với fact_id chỉ định để tránh xung đột
                insert_query = """
                    INSERT INTO FactJobPostingDaily (
                        fact_id, job_sk, company_sk, date_id, 
                        salary_min, salary_max, salary_type, 
                        due_date, time_remaining, verified_employer,
                        posted_time, crawled_at, load_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                insert_values = [
                    fact_id,
                    job_sk, company_sk, date_id,
                    job.salary_min if pd.notna(job.salary_min) else None,
                    job.salary_max if pd.notna(job.salary_max) else None,
                    job.salary_type if pd.notna(job.salary_type) else None,
                    due_date,
                    job.time_remaining if pd.notna(job.time_remaining) else None,
                    job.verified_employer if pd.notna(job.verified_employer) else False,
                    posted_time,
                    crawled_at,
                    load_month
                ]
                
                try:
                    self.duck_conn.execute(insert_query, insert_values)
                    logger.debug(f"Inserted new fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                    self.duck_conn.execute("COMMIT")
                    return fact_id
                except Exception as e1:
                    # Rollback transaction
                    self.duck_conn.execute("ROLLBACK")
                    
                    # Nếu gặp lỗi, có thể là do xung đột với fact_id đã tồn tại
                    logger.warning(f"Lỗi khi insert với fact_id chỉ định: {e1}")
                    
                    # Thử lại với transaction mới và fact_id trong range khác
                    self.duck_conn.execute("BEGIN TRANSACTION")
                    try:
                        # Kiểm tra lại một lần nữa
                        final_check = self.duck_conn.execute(
                            "SELECT fact_id FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", 
                            [job_sk, date_id]
                        ).fetchone()
                        
                        if final_check:
                            # Đã được tạo bởi process khác
                            self.duck_conn.execute("COMMIT")
                            return final_check[0]
                        
                        # Tạo fact_id mới với range khác hoàn toàn (8000000+)
                        unique_id = uuid.uuid4().int
                        timestamp_ms = int(time.time() * 1000)
                        fact_id = 8000000 + (abs(hash(f"{job_sk}_{date_id}_{uuid.uuid4()}")) + timestamp_ms) % (10**6)
                        
                        # Kiểm tra xem fact_id mới đã tồn tại chưa
                        check_id_query = "SELECT 1 FROM FactJobPostingDaily WHERE fact_id = ?"
                        id_exists = self.duck_conn.execute(check_id_query, [fact_id]).fetchone() is not None
                        
                        # Nếu fact_id đã tồn tại, tạo một giá trị khác
                        retry_count = 0
                        while id_exists and retry_count < 10:
                            retry_count += 1
                            fact_id = 8000000 + (abs(hash(f"{job_sk}_{date_id}_{uuid.uuid4()}_{retry_count}")) + timestamp_ms + retry_count * 50000) % (10**6)
                            id_exists = self.duck_conn.execute(check_id_query, [fact_id]).fetchone() is not None
                        
                        if id_exists:
                            logger.warning(f"Không thể tạo fact_id duy nhất sau 10 lần thử trong retry cho job_sk={job_sk}, date_id={date_id}")
                            self.duck_conn.execute("ROLLBACK")
                            return None
                        
                        # Thử lại với fact_id mới
                        insert_values[0] = fact_id
                        self.duck_conn.execute(insert_query, insert_values)
                        logger.debug(f"Inserted new fact record (retry): job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                        self.duck_conn.execute("COMMIT")
                        return fact_id
                    except Exception as e2:
                        self.duck_conn.execute("ROLLBACK")
                        logger.error(f"Lỗi khi thử insert lần 2: {e2}")
                        return None
            
            except Exception as e:
                # Rollback transaction
                self.duck_conn.execute("ROLLBACK")
                logger.error(f"Lỗi khi xử lý transaction cho fact record: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng khi tạo fact record cho job_id={job.job_id}, date={date_id}: {e}")
            # Đảm bảo transaction được rollback nếu có lỗi
            try:
                self.duck_conn.execute("ROLLBACK")
            except:
                pass
            return None