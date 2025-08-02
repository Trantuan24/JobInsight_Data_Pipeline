#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL module chính cho việc chuyển dữ liệu từ Staging sang Data Warehouse (DuckDB)
Phiên bản cải tiến với cấu trúc module rõ ràng
"""
import pandas as pd
import logging
import os
import sys
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import duckdb

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Đảm bảo thư mục backup tồn tại
BACKUP_DIR = os.path.join(PROJECT_ROOT, "data", "duck_db", "backup")
os.makedirs(BACKUP_DIR, exist_ok=True)

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

# Import các module cần thiết
from src.utils.config import DUCKDB_PATH, DWH_STAGING_SCHEMA, STAGING_JOBS_TABLE
from src.utils.db import get_dataframe, execute_query
from src.common.decorators import retry

from .etl_utils import get_duckdb_connection, setup_duckdb_schema, batch_insert_records
from .dimension_handler import DimensionHandler
from .fact_handler import FactHandler
from .partitioning import PartitionManager

try:
    from src.processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range
    )
except ImportError:
    # Fallback cho trường hợp chạy trực tiếp script này
    from processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range
    )

def backup_dwh_database():
    """
    Tạo bản backup của DuckDB trước khi chạy ETL
    
    Returns:
        str: Đường dẫn đến file backup hoặc None nếu không thể backup
    """
    if not os.path.exists(DUCKDB_PATH):
        logger.info(f"Không tìm thấy DuckDB để backup: {DUCKDB_PATH}")
        return None
        
    try:
        # Tạo tên file backup với timestamp và process ID để tránh xung đột
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pid = os.getpid()
        backup_filename = f"jobinsight_warehouse_{timestamp}_{pid}.duckdb.bak"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)
        
        # Đảm bảo thư mục backup tồn tại
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        # Đóng tất cả kết nối đến DuckDB trước khi backup
        # Đây là bước quan trọng để tránh file bị lock
        try:
            # FIXED: DuckDB doesn't support shutdown_transactions pragma
            # Simply ensure any existing connections are properly closed
            temp_conn = duckdb.connect(DUCKDB_PATH)
            # Just test connection and close - no need for shutdown_transactions
            temp_conn.execute("SELECT 1")
            temp_conn.close()
            logger.info("Đã kiểm tra và đóng kết nối DuckDB")
        except Exception as e:
            logger.warning(f"Không thể kiểm tra kết nối DuckDB: {e}")
        
        # Copy file DuckDB hiện tại sang file backup
        shutil.copy2(DUCKDB_PATH, backup_path)
        
        # Kiểm tra xem file backup có tồn tại không
        if os.path.exists(backup_path):
            backup_size = os.path.getsize(backup_path) / (1024 * 1024)  # MB
            logger.info(f"✅ Đã tạo backup DuckDB: {backup_path} ({backup_size:.2f} MB)")
            
            # Dọn dẹp các backup cũ (giữ lại 5 backup gần nhất)
            cleanup_old_backups()
            
            return backup_path
        else:
            logger.warning(f"❌ Không thể tạo backup DuckDB")
            return None
    except Exception as e:
        logger.error(f"❌ Lỗi khi tạo backup DuckDB: {e}")
        return None

def cleanup_old_backups(keep_count=5):
    """
    Dọn dẹp các file backup cũ, chỉ giữ lại số lượng file mới nhất
    
    Args:
        keep_count: Số lượng file backup mới nhất cần giữ lại
    """
    try:
        # Lấy danh sách tất cả các file backup
        backup_files = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) 
                       if f.endswith('.duckdb.bak')]
        
        # Sắp xếp theo thời gian sửa đổi (mới nhất đầu tiên)
        backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        
        # Xóa các file cũ
        if len(backup_files) > keep_count:
            for old_file in backup_files[keep_count:]:
                os.remove(old_file)
                logger.info(f"Đã xóa backup cũ: {os.path.basename(old_file)}")
    except Exception as e:
        logger.warning(f"Lỗi khi dọn dẹp backup cũ: {e}")

def restore_dwh_from_backup(backup_path):
    """
    Khôi phục DuckDB từ file backup
    
    Args:
        backup_path: Đường dẫn đến file backup
        
    Returns:
        bool: True nếu khôi phục thành công, False nếu thất bại
    """
    if not os.path.exists(backup_path):
        logger.error(f"Không tìm thấy file backup: {backup_path}")
        return False
        
    try:
        # Đóng tất cả kết nối đến DuckDB trước khi khôi phục
        try:
            # Tạo kết nối mới và đóng tất cả các kết nối hiện có
            temp_conn = duckdb.connect(DUCKDB_PATH)
            temp_conn.execute("PRAGMA shutdown_transactions")
            temp_conn.close()
            logger.info("Đã đóng tất cả kết nối hiện có đến DuckDB")
        except Exception as e:
            logger.warning(f"Không thể đóng kết nối hiện có: {e}")
        
        # Tạo backup của file hiện tại trước khi khôi phục (phòng trường hợp khôi phục thất bại)
        if os.path.exists(DUCKDB_PATH):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pid = os.getpid()
            pre_restore_backup = os.path.join(BACKUP_DIR, f"pre_restore_{timestamp}_{pid}.duckdb.bak")
            shutil.copy2(DUCKDB_PATH, pre_restore_backup)
            logger.info(f"Đã tạo backup trước khi khôi phục: {pre_restore_backup}")
        
        # Copy file backup sang file DuckDB
        shutil.copy2(backup_path, DUCKDB_PATH)
        
        # Kiểm tra xem file DuckDB có tồn tại không
        if os.path.exists(DUCKDB_PATH):
            logger.info(f"✅ Đã khôi phục DuckDB từ backup: {backup_path}")
            
            # Kiểm tra tính toàn vẹn của database sau khi khôi phục
            try:
                test_conn = duckdb.connect(DUCKDB_PATH)
                test_conn.execute("SELECT 1")  # Thử thực hiện một truy vấn đơn giản
                test_conn.close()
                logger.info("✅ Kiểm tra tính toàn vẹn database thành công")
            except Exception as e:
                logger.error(f"❌ Database không hoạt động sau khi khôi phục: {e}")
                
                # Thử khôi phục từ pre-restore backup nếu có
                if os.path.exists(pre_restore_backup):
                    logger.warning("Thử khôi phục từ pre-restore backup...")
                    shutil.copy2(pre_restore_backup, DUCKDB_PATH)
                    logger.info("Đã khôi phục từ pre-restore backup")
                
                return False
            
            return True
        else:
            logger.error(f"❌ Không thể khôi phục DuckDB từ backup")
            return False
    except Exception as e:
        logger.error(f"❌ Lỗi khi khôi phục DuckDB từ backup: {e}")
        return False

@retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=[Exception])
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
        
        # REMOVED: Debug column logging - not needed in production
        
        return df
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu từ staging: {e}", exc_info=True)
        # Trả về DataFrame rỗng trong trường hợp lỗi
        return pd.DataFrame()

def verify_etl_integrity(staging_count: int, fact_count: int, threshold: float = 0.9) -> bool:
    """
    Kiểm tra tính toàn vẹn của quá trình ETL Staging to DWH
    
    Args:
        staging_count: Số bản ghi staging đầu vào
        fact_count: Số bản ghi fact đã tạo
        threshold: Ngưỡng chấp nhận (% dữ liệu được xử lý thành công)
        
    Returns:
        bool: True nếu tỷ lệ dữ liệu chuyển đổi đạt threshold
    """
    if staging_count == 0:
        logger.warning("Không có dữ liệu nguồn để xử lý")
        return True
    
    # Mỗi staging record có thể tạo ra nhiều fact record (mỗi ngày một record)
    # Nên kiểm tra xem có fact records được tạo không, không so sánh số lượng
    if fact_count == 0:
        logger.error("Không có fact record nào được tạo từ staging data!")
        return False
    
    logger.info(f"Đã tạo {fact_count} fact records từ {staging_count} staging records")
    return True

@retry(max_tries=3, delay_seconds=2, backoff_factor=1.5, exceptions=[Exception])
def process_dimension_batch(dim_handler, staging_batch, dim_name, prepare_function, natural_key=None, surrogate_key=None, compare_columns=None):
    """
    Xử lý một dimension batch với retry
    
    Args:
        dim_handler: DimensionHandler instance
        staging_batch: DataFrame staging data
        dim_name: Tên dimension table
        prepare_function: Hàm chuẩn bị dữ liệu
        natural_key: Natural key column (None cho DimLocation)
        surrogate_key: Surrogate key column (None cho DimLocation)
        compare_columns: Columns để so sánh thay đổi (None cho DimLocation)
        
    Returns:
        Dict thống kê kết quả
    """
    logger.info(f"Xử lý dimension {dim_name} với retry...")
    
    try:
        # Xử lý đặc biệt cho DimLocation
        if dim_name == 'DimLocation':
            stats = dim_handler.process_location_dimension(staging_batch, prepare_function)
        else:
            # Xử lý thông thường cho các dimension khác
            stats = dim_handler.process_dimension_with_scd2(
                staging_batch,
                dim_name,
                prepare_function,
                natural_key,
                surrogate_key,
                compare_columns
            )
        logger.info(f"Đã xử lý {dim_name}: {stats}")
        return stats
    except Exception as e:
        logger.error(f"Lỗi khi xử lý dimension {dim_name}: {e}", exc_info=True)
        # Re-raise để retry decorator có thể xử lý
        raise

@retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=[Exception])
def generate_fact_records_with_retry(fact_handler, staging_batch):
    """
    Tạo fact records với retry
    
    Args:
        fact_handler: FactHandler instance
        staging_batch: DataFrame staging data
        
    Returns:
        Tuple chứa (fact_records, bridge_records)
    """
    logger.info("Tạo fact records với retry...")
    
    try:
        fact_records, bridge_records = fact_handler.generate_fact_records(staging_batch)
        logger.info(f"Đã tạo {len(fact_records)} fact records và {len(bridge_records)} bridge records")
        return fact_records, bridge_records
    except Exception as e:
        logger.error(f"Lỗi khi tạo fact records: {e}", exc_info=True)
        # Re-raise để retry decorator có thể xử lý
        raise

def run_staging_to_dwh_etl(last_etl_date: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Chạy ETL từ staging sang DWH
    
    Args:
        last_etl_date: Ngày chạy ETL gần nhất, lấy dữ liệu từ ngày này đến hiện tại
        
    Returns:
        Dict thông tin về kết quả ETL
    """
    start_time = datetime.now()
    logger.info(f"🚀 Bắt đầu ETL Staging to DWH...")
    
    # Tạo backup trước khi chạy ETL
    logger.info("📦 Tạo backup database trước khi chạy ETL...")
    backup_path = backup_dwh_database()
    if backup_path:
        logger.info(f"✅ Backup thành công: {backup_path}")
    else:
        logger.warning("⚠️ Không thể tạo backup database")
    
    try:
        # 1. Lấy dữ liệu từ staging
        if last_etl_date is None:
            last_etl_date = datetime.now() - timedelta(days=7)
            
        logger.info(f"📥 Lấy dữ liệu từ staging từ {last_etl_date}...")
        staging_batch = get_staging_batch(last_etl_date)
        
        if staging_batch.empty:
            logger.info("ℹ️ Không có bản ghi nào để xử lý từ staging")
            return {
                "success": True,
                "message": "Không có dữ liệu để xử lý",
                "source_count": 0,
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
            
        logger.info(f"✅ Đã lấy {len(staging_batch)} bản ghi từ staging")
        
        # 2. Kiểm tra file DuckDB
        if os.path.exists(DUCKDB_PATH):
            logger.info(f"📁 Sử dụng DuckDB hiện có: {DUCKDB_PATH}")
            file_size_mb = os.path.getsize(DUCKDB_PATH) / (1024 * 1024)
            logger.info(f"📊 Kích thước file DuckDB: {file_size_mb:.2f} MB")
        else:
            logger.info(f"🆕 Tạo DuckDB mới: {DUCKDB_PATH}")
        
        # 3. Thiết lập schema và bảng (giữ nguyên dữ liệu cũ)
        logger.info("🔧 Thiết lập schema và bảng DuckDB...")
        if not setup_duckdb_schema():
            logger.error("❌ Không thể thiết lập schema DuckDB")
            if backup_path:
                logger.warning("🔄 Khôi phục từ backup do lỗi thiết lập schema...")
                restore_dwh_from_backup(backup_path)
                
            return {
                "success": False,
                "message": "Không thể thiết lập schema DuckDB",
                "source_count": len(staging_batch),
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        
        # 4. Kết nối DuckDB và thực hiện ETL với SCD Type 2
        logger.info("🔌 Kết nối DuckDB và bắt đầu xử lý ETL...")
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            try:
                # Khởi tạo các handler
                logger.info("🔧 Khởi tạo các handler xử lý...")
                dim_handler = DimensionHandler(duck_conn)
                fact_handler = FactHandler(duck_conn)
                partition_manager = PartitionManager(duck_conn)
                
                # Kiểm tra tính toàn vẹn dữ liệu ban đầu
                logger.info("🔍 Kiểm tra tính toàn vẹn dữ liệu ban đầu...")
                initial_integrity_check = duck_conn.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM FactJobPostingDaily) as total_facts,
                        (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
                """).fetchone()
                
                if initial_integrity_check and initial_integrity_check[0] != initial_integrity_check[1]:
                    logger.warning(f"⚠️ Phát hiện vấn đề trùng lặp dữ liệu: {initial_integrity_check[0]} facts, {initial_integrity_check[1]} unique combinations")
                else:
                    logger.info(f"✅ Dữ liệu ban đầu không có vấn đề trùng lặp: {initial_integrity_check[0] if initial_integrity_check else 0} facts")
                
                # Dọn dẹp duplicate records hiện có
                logger.info("🧹 Dọn dẹp duplicate records hiện có...")
                cleanup_start = datetime.now()
                cleanup_result = fact_handler.cleanup_duplicate_fact_records()
                cleanup_duration = (datetime.now() - cleanup_start).total_seconds()
                logger.info(f"✅ Dọn dẹp hoàn tất trong {cleanup_duration:.2f} giây. Kết quả: {cleanup_result}")
                
                # Xử lý dimension tables
                logger.info("🔄 Xử lý dimension tables với SCD Type 2...")
                
                # DimJob
                logger.info("⏳ Xử lý DimJob...")
                job_start = datetime.now()
                job_stats = process_dimension_batch(dim_handler, staging_batch, 'DimJob', prepare_dim_job, 'job_id', 'job_sk', ['title_clean', 'job_url', 'skills', 'last_update', 'logo_url'])
                job_duration = (datetime.now() - job_start).total_seconds()
                logger.info(f"✅ DimJob xử lý hoàn tất trong {job_duration:.2f} giây. Kết quả: {job_stats}")
                
                # DimCompany
                logger.info("⏳ Xử lý DimCompany...")
                company_start = datetime.now()
                company_stats = process_dimension_batch(dim_handler, staging_batch, 'DimCompany', prepare_dim_company, 'company_name_standardized', 'company_sk', ['company_url', 'verified_employer'])
                company_duration = (datetime.now() - company_start).total_seconds()
                logger.info(f"✅ DimCompany xử lý hoàn tất trong {company_duration:.2f} giây. Kết quả: {company_stats}")
                
                # DimLocation (xử lý đặc biệt vì có composite key)
                logger.info("⏳ Xử lý DimLocation...")
                location_start = datetime.now()
                location_stats = process_dimension_batch(dim_handler, staging_batch, 'DimLocation', prepare_dim_location, None, None, None)
                location_duration = (datetime.now() - location_start).total_seconds()
                logger.info(f"✅ DimLocation xử lý hoàn tất trong {location_duration:.2f} giây. Kết quả: {location_stats}")
                
                # Xử lý DimDate
                logger.info("⏳ Xử lý DimDate...")
                date_start = datetime.now()
                
                # Tìm ngày bắt đầu và kết thúc từ staging data
                min_date = datetime.now().date() - timedelta(days=30)  # Mặc định 30 ngày trước
                max_date = datetime.now().date() + timedelta(days=270)  # Mặc định 270 ngày sau
                
                # Tạo date range
                date_range_df = generate_date_range(min_date, max_date)
                
                # Insert vào DimDate
                date_records = []
                for _, date_record in date_range_df.iterrows():
                    date_records.append(date_record.to_dict())
                
                date_count = batch_insert_records(
                    duck_conn, 
                    'DimDate', 
                    date_records, 
                    on_conflict='ON CONFLICT (date_id) DO NOTHING'
                )
                
                date_duration = (datetime.now() - date_start).total_seconds()
                logger.info(f"✅ DimDate xử lý hoàn tất trong {date_duration:.2f} giây. Đã thêm {date_count} ngày mới.")
                
                # Xử lý fact table
                logger.info("⏳ Xử lý FactJobPostingDaily và FactJobLocationBridge...")
                fact_start = datetime.now()
                fact_records, bridge_records = generate_fact_records_with_retry(fact_handler, staging_batch)
                fact_duration = (datetime.now() - fact_start).total_seconds()
                logger.info(f"✅ Fact tables xử lý hoàn tất trong {fact_duration:.2f} giây. Đã tạo {len(fact_records)} fact records và {len(bridge_records)} bridge records.")
                
                # REMOVED: processed_ids not used - staging marking handled elsewhere
                
                # Tóm tắt kết quả
                dim_stats = {
                    'DimJob': job_stats,
                    'DimCompany': company_stats,
                    'DimLocation': location_stats,
                    'DimDate': {'inserted': date_count, 'updated': 0, 'unchanged': 0}
                }
                
                # Quản lý partition và export sang Parquet
                logger.info("📊 Quản lý partition và export sang Parquet")
                partition_start = datetime.now()
                partition_result = partition_manager.manage_partitions('FactJobPostingDaily', 'load_month')
                partition_duration = (datetime.now() - partition_start).total_seconds()
                logger.info(f"✅ Partition xử lý hoàn tất trong {partition_duration:.2f} giây. Kết quả: {partition_result}")
                
                # Tổng kết
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.info("============================================================")
                logger.info(f"📊 TỔNG KẾT ETL STAGING TO DWH")
                logger.info("============================================================")
                
                # Log thống kê dimension
                for dim_name, stats in dim_stats.items():
                    logger.info(f"{dim_name:<15} - Insert: {stats['inserted']:5d}, Update: {stats['updated']:5d}, Unchanged: {stats['unchanged']:5d}")
                
                logger.info("------------------------------------------------------------")
                total_inserted = sum(stats['inserted'] for stats in dim_stats.values())
                total_updated = sum(stats['updated'] for stats in dim_stats.values())
                total_unchanged = sum(stats['unchanged'] for stats in dim_stats.values())
                logger.info(f"TỔNG DIM        - Insert: {total_inserted:5d}, Update: {total_updated:5d}, Unchanged: {total_unchanged:5d}")
                
                # Log thống kê partition
                if partition_result['success']:
                    logger.info(f"PARTITION       - Thành công: {partition_result['message']}")
                else:
                    logger.warning(f"PARTITION       - Thất bại: {partition_result['message']}")
                
                # Validation ETL
                logger.info("🔍 Bắt đầu validation ETL...")
                validation_start = datetime.now()
                try:
                    from src.utils.etl_validator import generate_etl_report, log_validation_results
                    validation_result = generate_etl_report(duck_conn)
                    log_validation_results(validation_result)
                    validation_duration = (datetime.now() - validation_start).total_seconds()
                    logger.info(f"✅ Validation hoàn tất trong {validation_duration:.2f} giây.")
                except ImportError as e:
                    logger.warning(f"⚠️ Không thể import etl_validator - bỏ qua validation: {e}")
                except Exception as e:
                    logger.error(f"❌ Lỗi khi thực hiện validation: {str(e)}")
                
                # Kết thúc
                logger.info("============================================================")
                logger.info(f"✅ ETL HOÀN THÀNH TRONG {duration:.2f} GIÂY!")
                logger.info(f"🕒 Thời gian bắt đầu: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"🕒 Thời gian kết thúc: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("============================================================")
                
                return {
                    "success": True,
                    "message": f"ETL hoàn thành thành công trong {duration:.2f} giây",
                    "source_count": len(staging_batch),
                    "fact_count": len(fact_records),
                    "dim_stats": dim_stats,
                    "partition_success": partition_result['success'],
                    "duration_seconds": duration,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat()
                }
            except Exception as e:
                logger.error(f"❌ Lỗi trong quá trình ETL: {e}", exc_info=True)
                if backup_path:
                    logger.warning("🔄 Khôi phục từ backup do lỗi trong quá trình ETL...")
                    restore_result = restore_dwh_from_backup(backup_path)
                    logger.info(f"Kết quả khôi phục: {'✅ Thành công' if restore_result else '❌ Thất bại'}")
                
                return {
                    "success": False,
                    "message": f"Lỗi trong quá trình ETL: {str(e)}",
                    "source_count": len(staging_batch),
                    "fact_count": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds(),
                    "error": str(e),
                    "error_type": type(e).__name__
                }
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng trong quá trình ETL: {e}", exc_info=True)
        if backup_path:
            logger.warning("🔄 Khôi phục từ backup do lỗi nghiêm trọng...")
            restore_result = restore_dwh_from_backup(backup_path)
            logger.info(f"Kết quả khôi phục: {'✅ Thành công' if restore_result else '❌ Thất bại'}")
            
        return {
            "success": False,
            "message": f"Lỗi nghiêm trọng: {str(e)}",
            "source_count": 0,
            "fact_count": 0,
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "error": str(e),
            "error_type": type(e).__name__
        }

def run_incremental_etl(last_etl_date: Optional[datetime] = None, batch_size: int = 1000) -> Dict[str, Any]:
    """
    Chạy ETL từ staging sang DWH theo batch để giảm áp lực lên hệ thống
    
    Args:
        last_etl_date: Ngày chạy ETL gần nhất, lấy dữ liệu từ ngày này đến hiện tại
        batch_size: Số bản ghi tối đa xử lý trong mỗi batch
        
    Returns:
        Dict thông tin về kết quả ETL
    """
    start_time = datetime.now()
    logger.info(f"🚀 Bắt đầu Incremental ETL Staging to DWH với batch_size={batch_size}...")
    
    # Tạo backup trước khi chạy ETL
    backup_path = backup_dwh_database()
    
    try:
        # 1. Lấy dữ liệu từ staging
        if last_etl_date is None:
            last_etl_date = datetime.now() - timedelta(days=7)
            
        logger.info(f"Lấy dữ liệu từ staging từ {last_etl_date}...")
        full_staging_batch = get_staging_batch(last_etl_date)
        
        if full_staging_batch.empty:
            logger.info("Không có bản ghi nào để xử lý từ staging")
            return {
                "success": True,
                "message": "Không có dữ liệu để xử lý",
                "source_count": 0,
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
            
        total_records = len(full_staging_batch)
        logger.info(f"Đã lấy tổng cộng {total_records} bản ghi từ staging")
        
        # Chia thành các batch
        num_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
        logger.info(f"Chia thành {num_batches} batch, mỗi batch {batch_size} bản ghi")
        
        # Thống kê tổng hợp
        total_stats = {
            "source_count": total_records,
            "processed_count": 0,
            "fact_count": 0,
            "dim_stats": {
                "DimJob": {"inserted": 0, "updated": 0, "unchanged": 0},
                "DimCompany": {"inserted": 0, "updated": 0, "unchanged": 0},
                "DimLocation": {"inserted": 0, "updated": 0, "unchanged": 0},
                "DimDate": {"inserted": 0, "updated": 0, "unchanged": 0}
            },
            "batch_results": []
        }
        
        # Xử lý từng batch
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_records)
            
            logger.info(f"Xử lý batch {batch_idx+1}/{num_batches} (records {start_idx+1}-{end_idx}/{total_records})...")
            
            # Lấy batch hiện tại
            current_batch = full_staging_batch.iloc[start_idx:end_idx].copy()
            
            # Chạy ETL cho batch này
            try:
                batch_result = run_staging_to_dwh_etl_batch(current_batch)
                
                # Cập nhật thống kê tổng hợp
                total_stats["processed_count"] += len(current_batch)
                total_stats["fact_count"] += batch_result.get("fact_count", 0)
                
                # Cập nhật dim_stats
                for dim_name, stats in batch_result.get("dim_stats", {}).items():
                    if dim_name in total_stats["dim_stats"]:
                        for key in ["inserted", "updated", "unchanged"]:
                            total_stats["dim_stats"][dim_name][key] += stats.get(key, 0)
                
                # Lưu kết quả batch
                total_stats["batch_results"].append({
                    "batch_idx": batch_idx,
                    "records": len(current_batch),
                    "success": batch_result.get("success", False),
                    "fact_count": batch_result.get("fact_count", 0)
                })
                
                logger.info(f"Batch {batch_idx+1}/{num_batches} hoàn thành: {len(current_batch)} records, {batch_result.get('fact_count', 0)} facts")
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý batch {batch_idx+1}/{num_batches}: {e}", exc_info=True)
                # Tiếp tục với batch tiếp theo
                total_stats["batch_results"].append({
                    "batch_idx": batch_idx,
                    "records": len(current_batch),
                    "success": False,
                    "error": str(e)
                })
        
        # Quản lý partition và export sang Parquet
        logger.info("📊 Quản lý partition và export sang Parquet cho tất cả dữ liệu...")
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            partition_manager = PartitionManager(duck_conn)
            partition_result = partition_manager.manage_partitions('FactJobPostingDaily', 'load_month')
            total_stats["partition_success"] = partition_result.get("success", False)
        
        # Tổng kết
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("============================================================")
        logger.info(f"📊 TỔNG KẾT INCREMENTAL ETL STAGING TO DWH")
        logger.info("============================================================")
        logger.info(f"Tổng số bản ghi: {total_records}")
        logger.info(f"Đã xử lý: {total_stats['processed_count']}")
        logger.info(f"Tạo facts: {total_stats['fact_count']}")
        
        # Log thống kê dimension
        for dim_name, stats in total_stats["dim_stats"].items():
            logger.info(f"{dim_name:<15} - Insert: {stats['inserted']:5d}, Update: {stats['updated']:5d}, Unchanged: {stats['unchanged']:5d}")
        
        # Log thống kê batch
        success_batches = sum(1 for batch in total_stats["batch_results"] if batch["success"])
        logger.info(f"Batches thành công: {success_batches}/{num_batches}")
        
        # Log thống kê partition
        if total_stats["partition_success"]:
            logger.info(f"PARTITION       - Thành công: {partition_result.get('message', '')}")
        else:
            logger.warning(f"PARTITION       - Thất bại: {partition_result.get('message', '')}")
        
        logger.info("============================================================")
        logger.info(f"✅ INCREMENTAL ETL HOÀN THÀNH TRONG {duration:.2f} GIÂY!")
        logger.info("============================================================")
        
        return {
            "success": total_stats["processed_count"] > 0,
            "message": f"Incremental ETL hoàn thành trong {duration:.2f} giây",
            "source_count": total_records,
            "processed_count": total_stats["processed_count"],
            "fact_count": total_stats["fact_count"],
            "dim_stats": total_stats["dim_stats"],
            "batch_results": total_stats["batch_results"],
            "partition_success": total_stats["partition_success"],
            "duration_seconds": duration
        }
        
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong quá trình Incremental ETL: {e}", exc_info=True)
        if backup_path:
            logger.warning("Khôi phục từ backup do lỗi nghiêm trọng...")
            restore_dwh_from_backup(backup_path)
            
        return {
            "success": False,
            "message": f"Lỗi nghiêm trọng trong Incremental ETL: {str(e)}",
            "source_count": 0,
            "processed_count": 0,
            "fact_count": 0,
            "duration_seconds": (datetime.now() - start_time).total_seconds()
        }

def run_staging_to_dwh_etl_batch(staging_batch: pd.DataFrame) -> Dict[str, Any]:
    """
    Chạy ETL từ staging sang DWH cho một batch dữ liệu
    
    Args:
        staging_batch: DataFrame chứa dữ liệu staging cần xử lý
        
    Returns:
        Dict thông tin về kết quả ETL
    """
    start_time = datetime.now()
    logger.info(f"Bắt đầu ETL batch với {len(staging_batch)} bản ghi...")
    
    try:
        # REMOVED: Debug DuckDB file logging - not needed
        
        # Thiết lập schema và bảng (giữ nguyên dữ liệu cũ)
        if not setup_duckdb_schema():
            logger.error("Không thể thiết lập schema DuckDB")
            return {
                "success": False,
                "message": "Không thể thiết lập schema DuckDB",
                "source_count": len(staging_batch),
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        
        # Kết nối DuckDB và thực hiện ETL với SCD Type 2
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            try:
                # Bắt đầu transaction để đảm bảo tính nhất quán
                duck_conn.execute("BEGIN TRANSACTION")
                
                try:
                    # Khởi tạo các handler
                    dim_handler = DimensionHandler(duck_conn)
                    fact_handler = FactHandler(duck_conn)
                    
                    # Dọn dẹp duplicate records hiện có
                    fact_handler.cleanup_duplicate_fact_records()
                    
                    # Xử lý dimension tables
                    logger.info("Xử lý dimension tables với SCD Type 2...")
                    
                    # DimJob
                    job_stats = process_dimension_batch(dim_handler, staging_batch, 'DimJob', prepare_dim_job, 'job_id', 'job_sk', ['title_clean', 'job_url', 'skills', 'last_update', 'logo_url'])
                    
                    # DimCompany
                    company_stats = process_dimension_batch(dim_handler, staging_batch, 'DimCompany', prepare_dim_company, 'company_name_standardized', 'company_sk', ['company_url', 'verified_employer'])
                    
                    # DimLocation (xử lý đặc biệt vì có composite key)
                    location_stats = process_dimension_batch(dim_handler, staging_batch, 'DimLocation', prepare_dim_location)
                    
                    # Xử lý DimDate
                    logger.info("Xử lý DimDate")
                    
                    # Tìm ngày bắt đầu và kết thúc từ staging data
                    min_date = datetime.now().date() - timedelta(days=30)  # Mặc định 30 ngày trước
                    max_date = datetime.now().date() + timedelta(days=270)  # Mặc định 270 ngày sau
                    
                    # Tạo date range
                    date_range_df = generate_date_range(min_date, max_date)
                    
                    # Insert vào DimDate
                    date_records = []
                    for _, date_record in date_range_df.iterrows():
                        date_records.append(date_record.to_dict())
                    
                    date_count = batch_insert_records(
                        duck_conn, 
                        'DimDate', 
                        date_records, 
                        on_conflict='ON CONFLICT (date_id) DO NOTHING'
                    )
                    
                    # Xử lý fact table
                    logger.info("Xử lý FactJobPostingDaily và FactJobLocationBridge")
                    fact_records, _ = generate_fact_records_with_retry(fact_handler, staging_batch)

                    # REMOVED: processed_ids not used
                    
                    # Tóm tắt kết quả
                    dim_stats = {
                        'DimJob': job_stats,
                        'DimCompany': company_stats,
                        'DimLocation': location_stats,
                        'DimDate': {'inserted': date_count, 'updated': 0, 'unchanged': 0}
                    }
                    
                    # Commit transaction
                    duck_conn.execute("COMMIT")
                    logger.info("Transaction đã được commit thành công")
                    
                    # Kết thúc
                    duration = (datetime.now() - start_time).total_seconds()
                    logger.info(f"✅ ETL batch hoàn thành trong {duration:.2f} giây")
                    
                    return {
                        "success": True,
                        "message": f"ETL batch hoàn thành thành công trong {duration:.2f} giây",
                        "source_count": len(staging_batch),
                        "fact_count": len(fact_records),
                        "dim_stats": dim_stats,
                        "duration_seconds": duration
                    }
                
                except Exception as e:
                    # Rollback transaction nếu có lỗi
                    duck_conn.execute("ROLLBACK")
                    logger.error(f"Lỗi trong quá trình ETL batch, đã rollback: {e}", exc_info=True)
                    return {
                        "success": False,
                        "message": f"Lỗi trong quá trình ETL batch: {str(e)}",
                        "source_count": len(staging_batch),
                        "fact_count": 0,
                        "duration_seconds": (datetime.now() - start_time).total_seconds()
                    }
                    
            except Exception as e:
                # Đảm bảo transaction được rollback nếu có lỗi
                try:
                    duck_conn.execute("ROLLBACK")
                except:
                    pass
                    
                logger.error(f"Lỗi khi xử lý transaction ETL batch: {e}", exc_info=True)
                return {
                    "success": False,
                    "message": f"Lỗi khi xử lý transaction ETL batch: {str(e)}",
                    "source_count": len(staging_batch),
                    "fact_count": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds()
                }
                
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng trong quá trình ETL batch: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Lỗi nghiêm trọng: {str(e)}",
            "source_count": len(staging_batch),
            "fact_count": 0,
            "duration_seconds": (datetime.now() - start_time).total_seconds()
        }

def main():
    """
    Hàm main để chạy ETL từ command line
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL từ Staging sang Data Warehouse')
    parser.add_argument('--days', type=int, default=7, help='Số ngày dữ liệu cần lấy (mặc định: 7)')
    args = parser.parse_args()
    
    # Tính ngày bắt đầu ETL
    last_etl_date = datetime.now() - timedelta(days=args.days)
    
    # Chạy ETL
    etl_result = run_staging_to_dwh_etl(last_etl_date)
    
    # Kiểm tra kết quả
    if etl_result.get("success", False):
        logger.info("✅ ETL HOÀN THÀNH THÀNH CÔNG!")
        sys.exit(0)
    else:
        logger.error(f"❌ ETL THẤT BẠI: {etl_result.get('message', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main() 