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
                    bridges = self._process_location_bridges(job, fact_id)
                    if bridges:
                        bridge_records.extend(bridges)
                    else:
                        bridge_skipped += 1
                
        logger.info(f"Đã tạo {len(fact_records)} fact records và {len(bridge_records)} bridge records")
        logger.info(f"Đã bỏ qua {job_skipped} jobs và {bridge_skipped} bridges")
        
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
        Xử lý một fact record duy nhất (cập nhật hoặc tạo mới)
        
        Args:
            job: Bản ghi job từ staging
            job_sk: Surrogate key của job
            company_sk: Surrogate key của company
            date_id: Ngày hiển thị job
            due_date: Ngày hết hạn
            posted_time: Thời gian đăng job
            crawled_at: Thời gian crawl
            load_month: Tháng load dữ liệu (YYYY-MM)
            
        Returns:
            fact_id nếu thành công, None nếu thất bại
        """
        try:
            # Kiểm tra xem đã có fact record cho job_sk và date_id hay chưa
            check_query = """
                SELECT fact_id FROM FactJobPostingDaily 
                WHERE job_sk = ? AND date_id = ?
            """
            
            existing = self.duck_conn.execute(check_query, [job_sk, date_id]).fetchone()
            
            if existing:
                # Đã tồn tại, cập nhật thông tin
                fact_id = existing[0]
                
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
                except Exception as e:
                    logger.error(f"Lỗi khi update fact record cho job_id={job.job_id}, date={date_id}: {e}")
                    return None
                    
            else:
                # Chưa tồn tại, tạo mới với transaction để đảm bảo tính nhất quán
                try:
                    # Không bắt đầu transaction mới - transaction có thể được bắt đầu ở cấp cao hơn
                    # Kiểm tra xem đã có fact record với job_sk và date_id này chưa
                    double_check = self.duck_conn.execute(
                        "SELECT fact_id FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", 
                        [job_sk, date_id]
                    ).fetchone()
                    
                    if double_check:
                        # Có thể đã được tạo bởi một process khác
                        logger.debug(f"Đã tìm thấy fact record trong lần kiểm tra thứ hai: job_sk={job_sk}, date_id={date_id}")
                        return double_check[0]
                    
                    # Tìm giá trị fact_id lớn nhất để đảm bảo tính duy nhất
                    max_fact_id = self.duck_conn.execute("SELECT COALESCE(MAX(fact_id), 0) + 1000 FROM FactJobPostingDaily").fetchone()[0]
                    
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
                        max_fact_id,
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
                        fact_id = max_fact_id
                        logger.debug(f"Inserted new fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                    except Exception as e1:
                        # Nếu gặp lỗi, có thể là do xung đột với sequence tự tăng
                        logger.warning(f"Lỗi khi insert với fact_id chỉ định: {e1}")
                        
                        # Thử insert lại không chỉ định fact_id
                        try:
                            default_insert_query = """
                                INSERT INTO FactJobPostingDaily (
                                    job_sk, company_sk, date_id, 
                                    salary_min, salary_max, salary_type, 
                                    due_date, time_remaining, verified_employer,
                                    posted_time, crawled_at, load_month
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                RETURNING fact_id
                            """
                            
                            default_values = insert_values[1:]  # Bỏ qua fact_id
                            result = self.duck_conn.execute(default_insert_query, default_values).fetchone()
                            if result:
                                fact_id = result[0]
                                logger.debug(f"Inserted new fact record (phương pháp 2): job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                            else:
                                logger.warning(f"Không thể insert fact record cho job_id={job.job_id}, date={date_id}")
                                return None
                        except Exception as e2:
                            logger.error(f"Lỗi khi thử insert lần 2: {e2}")
                            
                            # Kiểm tra một lần nữa xem record đã được tạo chưa
                            final_check = self.duck_conn.execute(
                                "SELECT fact_id FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", 
                                [job_sk, date_id]
                            ).fetchone()
                            
                            if final_check:
                                logger.info(f"Đã tìm thấy fact record sau lỗi: job_sk={job_sk}, date_id={date_id}")
                                return final_check[0]
                            else:
                                return None
                    
                except Exception as e:
                    # Không rollback vì transaction có thể được quản lý ở cấp cao hơn
                    # Kiểm tra một lần nữa xem record đã tồn tại chưa
                    try:
                        recovery_check = self.duck_conn.execute(
                            "SELECT fact_id FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", 
                            [job_sk, date_id]
                        ).fetchone()
                        
                        if recovery_check:
                            # Nếu đã có record với (job_sk, date_id) này rồi, sử dụng fact_id đó
                            logger.warning(f"Recovered existing fact record after error: job_sk={job_sk}, date_id={date_id}")
                            return recovery_check[0]
                    except:
                        pass
                        
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
        Đây là một quá trình quan trọng để đảm bảo tính toàn vẹn dữ liệu
        """
        logger.info("Bắt đầu dọn dẹp duplicate fact records...")
        
        try:
            # Đảm bảo kết nối ổn định
            try:
                self.duck_conn.execute("PRAGMA foreign_keys = OFF")
            except:
                logger.warning("Không thể tắt foreign keys - có thể DuckDB không hỗ trợ")
            
            # Bắt đầu transaction
            self.duck_conn.execute("BEGIN TRANSACTION")
            
            try:
                # 1. Tạo bảng backup cho FactJobPostingDaily và FactJobLocationBridge
                logger.info("Tạo bảng backup...")
                self.duck_conn.execute("CREATE TEMP TABLE IF NOT EXISTS fact_backup AS SELECT * FROM FactJobPostingDaily")
                self.duck_conn.execute("CREATE TEMP TABLE IF NOT EXISTS bridge_backup AS SELECT * FROM FactJobLocationBridge")
                
                # 2. Tìm các duplicate records trong FactJobPostingDaily
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
                    
                    # 3. Xóa tất cả các FactJobLocationBridge liên quan đến duplicate fact records
                    for _, dup in duplicates.iterrows():
                        job_sk, date_id, count, keep_fact_id = dup['job_sk'], dup['date_id'], dup['count'], dup['keep_fact_id']
                        
                        # Lấy tất cả fact_id cho job_sk và date_id này
                        get_all_fact_ids_query = """
                            SELECT fact_id FROM FactJobPostingDaily
                            WHERE job_sk = ? AND date_id = ?
                        """
                        all_fact_ids = self.duck_conn.execute(get_all_fact_ids_query, [job_sk, date_id]).fetchall()
                        all_fact_ids = [row[0] for row in all_fact_ids]
                        
                        if len(all_fact_ids) > 1:
                            # Xóa tất cả bridge records liên quan đến các fact_id này
                            placeholders = ','.join(['?'] * len(all_fact_ids))
                            self.duck_conn.execute(f"DELETE FROM FactJobLocationBridge WHERE fact_id IN ({placeholders})", all_fact_ids)
                            
                            # Xóa tất cả fact records liên quan đến job_sk và date_id này
                            self.duck_conn.execute("DELETE FROM FactJobPostingDaily WHERE job_sk = ? AND date_id = ?", [job_sk, date_id])
                            
                            # Tạo lại fact record mới với fact_id nhỏ nhất
                            get_original_fact_query = """
                                SELECT * FROM fact_backup 
                                WHERE fact_id = ?
                            """
                            original_fact = self.duck_conn.execute(get_original_fact_query, [keep_fact_id]).fetchone()
                            
                            if original_fact:
                                # Tạo câu lệnh INSERT mới cho fact record
                                columns = self.duck_conn.execute("PRAGMA table_info(FactJobPostingDaily)").fetchdf()['name'].tolist()
                                placeholders = ','.join(['?'] * len(columns))
                                insert_query = f"INSERT INTO FactJobPostingDaily ({','.join(columns)}) VALUES ({placeholders})"
                                
                                self.duck_conn.execute(insert_query, original_fact)
                                logger.debug(f"Đã tạo lại fact record cho job_sk={job_sk}, date_id={date_id} với fact_id={keep_fact_id}")
                                
                                # Tạo lại bridge records cho fact_id này
                                get_bridge_records_query = """
                                    SELECT location_sk FROM bridge_backup 
                                    WHERE fact_id = ?
                                """
                                bridge_records = self.duck_conn.execute(get_bridge_records_query, [keep_fact_id]).fetchall()
                                
                                for bridge in bridge_records:
                                    location_sk = bridge[0]
                                    self.duck_conn.execute(
                                        "INSERT INTO FactJobLocationBridge (fact_id, location_sk) VALUES (?, ?)",
                                        [keep_fact_id, location_sk]
                                    )
                    
                    # 4. Kiểm tra kết quả sau khi xử lý
                    check_duplicates_query = """
                        SELECT COUNT(*) FROM (
                            SELECT job_sk, date_id, COUNT(*) 
                            FROM FactJobPostingDaily 
                            GROUP BY job_sk, date_id 
                            HAVING COUNT(*) > 1
                        )
                    """
                    remaining_duplicates = self.duck_conn.execute(check_duplicates_query).fetchone()[0]
                    
                    if remaining_duplicates > 0:
                        logger.warning(f"Vẫn còn {remaining_duplicates} nhóm duplicate sau khi xử lý!")
                    else:
                        logger.info("✅ Đã xử lý tất cả duplicate records thành công")
                
                else:
                    logger.info("Không tìm thấy duplicate fact records")
                
                # 5. Xóa bảng tạm
                self.duck_conn.execute("DROP TABLE IF EXISTS fact_backup")
                self.duck_conn.execute("DROP TABLE IF EXISTS bridge_backup")
                
                # 6. Commit transaction
                self.duck_conn.execute("COMMIT")
                
                # 7. Phân tích lại bảng để tối ưu hiệu suất
                try:
                    self.duck_conn.execute("ANALYZE FactJobPostingDaily")
                    self.duck_conn.execute("ANALYZE FactJobLocationBridge")
                except:
                    logger.warning("Không thể thực hiện ANALYZE - có thể DuckDB không hỗ trợ")
                
                # 8. Kiểm tra lại và báo cáo
                stats_query = """
                    SELECT 
                        COUNT(*) as total_facts,
                        (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
                    FROM FactJobPostingDaily
                """
                stats = self.duck_conn.execute(stats_query).fetchone()
                logger.info(f"Sau dọn dẹp: {stats[0]} fact records, {stats[1]} unique combinations")
                
                # Reset PRAGMA settings
                try:
                    self.duck_conn.execute("PRAGMA foreign_keys = ON")
                except:
                    pass
                
                return {
                    "success": True,
                    "message": f"Đã dọn dẹp duplicate records, còn lại {stats[0]} records"
                }
                
            except Exception as e:
                # Rollback nếu có lỗi
                self.duck_conn.execute("ROLLBACK")
                logger.error(f"Lỗi khi dọn dẹp duplicate records: {e}")
                
                # Reset PRAGMA settings
                try:
                    self.duck_conn.execute("PRAGMA foreign_keys = ON")
                except:
                    pass
                
                return {
                    "success": False,
                    "message": f"Lỗi: {str(e)}"
                }
                
        except Exception as e:
            logger.error(f"Lỗi critical khi dọn dẹp duplicate records: {e}")
            return {
                "success": False,
                "message": f"Critical error: {str(e)}"
            }