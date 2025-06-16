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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

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

from src.etl.etl_utils import get_duckdb_connection, setup_duckdb_schema, batch_insert_records
from src.etl.dimension_handler import DimensionHandler
from src.etl.fact_handler import FactHandler
from src.etl.partitioning import PartitionManager

try:
    from src.processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range
    )
except ImportError:
    from processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range
    )

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
        
        # Log các cột để debug
        if not df.empty:
            logger.info(f"Các cột có trong dữ liệu: {list(df.columns)}")
        
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
    
    try:
        # 1. Lấy dữ liệu từ staging
        if last_etl_date is None:
            last_etl_date = datetime.now() - timedelta(days=7)
            
        logger.info(f"Lấy dữ liệu từ staging từ {last_etl_date}...")
        staging_batch = get_staging_batch(last_etl_date)
        
        if staging_batch.empty:
            logger.info("Không có bản ghi nào để xử lý từ staging")
            return {
                "success": True,
                "message": "Không có dữ liệu để xử lý",
                "source_count": 0,
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
            
        logger.info(f"Đã lấy {len(staging_batch)} bản ghi từ staging")
        
        # 2. Kiểm tra file DuckDB
        if os.path.exists(DUCKDB_PATH):
            logger.info(f"📁 Sử dụng DuckDB hiện có: {DUCKDB_PATH}")
        else:
            logger.info(f"🆕 Tạo DuckDB mới: {DUCKDB_PATH}")
        
        # 3. Thiết lập schema và bảng (giữ nguyên dữ liệu cũ)
        if not setup_duckdb_schema():
            return {
                "success": False,
                "message": "Không thể thiết lập schema DuckDB",
                "source_count": len(staging_batch),
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        
        # 4. Kết nối DuckDB và thực hiện ETL với SCD Type 2
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            # Khởi tạo các handler
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
            
            # Dọn dẹp duplicate records hiện có
            logger.info("🧹 Dọn dẹp duplicate records hiện có...")
            cleanup_result = fact_handler.cleanup_duplicate_fact_records()
            if not cleanup_result.get('success', False):
                logger.warning(f"⚠️ Có vấn đề trong quá trình dọn dẹp: {cleanup_result.get('message', 'Unknown error')}")
            
            # Kiểm tra lại sau khi dọn dẹp
            post_cleanup_check = duck_conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT job_sk, date_id, COUNT(*) 
                    FROM FactJobPostingDaily 
                    GROUP BY job_sk, date_id 
                    HAVING COUNT(*) > 1
                )
            """).fetchone()
            
            if post_cleanup_check and post_cleanup_check[0] > 0:
                logger.warning(f"⚠️ Vẫn còn {post_cleanup_check[0]} nhóm duplicate sau khi dọn dẹp - cần phân tích sâu hơn!")
                
                # Thử sửa chữa bằng cách force cleanup
                logger.info("Thực hiện force cleanup để sửa chữa dữ liệu...")
                duck_conn.execute("""
                    BEGIN TRANSACTION;
                    
                    -- Tạo bảng tạm để lưu unique records
                    CREATE TEMP TABLE unique_facts AS
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_sk, date_id ORDER BY fact_id) as rn
                        FROM FactJobPostingDaily
                    ) WHERE rn = 1;
                    
                    -- Xóa hết bridge tables
                    DELETE FROM FactJobLocationBridge;
                    
                    -- Xóa hết facts
                    DELETE FROM FactJobPostingDaily;
                    
                    -- Insert lại unique facts (bỏ cột rn)
                    INSERT INTO FactJobPostingDaily 
                    SELECT fact_id, job_sk, company_sk, date_id, 
                           salary_min, salary_max, salary_type, 
                           due_date, time_remaining, verified_employer, 
                           posted_time, crawled_at, load_month
                    FROM unique_facts;
                    
                    -- Xóa bảng tạm
                    DROP TABLE unique_facts;
                    
                    COMMIT;
                """)
                
                # Kiểm tra lại sau khi force cleanup
                final_check = duck_conn.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM FactJobPostingDaily) as total_facts,
                        (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
                """).fetchone()
                
                if final_check and final_check[0] == final_check[1]:
                    logger.info(f"✅ Force cleanup thành công: {final_check[0]} facts, {final_check[1]} unique combinations")
                else:
                    logger.error(f"❌ Force cleanup thất bại: {final_check[0]} facts, {final_check[1]} unique combinations")
            
            # 5. Xử lý và insert dữ liệu với SCD Type 2
            dim_stats = {}
            
            # 5.1 DimJob với SCD Type 2
            dim_stats['DimJob'] = dim_handler.process_dimension_with_scd2(
                staging_batch, 'DimJob', prepare_dim_job,
                'job_id', 'job_sk', ['title_clean', 'skills', 'job_url']
            )
        
            # 5.2. DimCompany với SCD Type 2
            dim_stats['DimCompany'] = dim_handler.process_dimension_with_scd2(
                staging_batch, 'DimCompany', prepare_dim_company,
                'company_name_standardized', 'company_sk', ['company_url', 'verified_employer']
            )
        
            # 5.3. DimLocation - xử lý đặc biệt vì composite key
            dim_stats['DimLocation'] = dim_handler.process_location_dimension(
                staging_batch, prepare_dim_location
            )
        
            # 5.4. Đảm bảo bảng DimDate có đầy đủ các ngày cần thiết
            logger.info("Xử lý DimDate")
            start_date = (datetime.now() - timedelta(days=60)).date()
            end_date = (datetime.now() + timedelta(days=240)).date()
            
            date_df = generate_date_range(start_date, end_date)
            
            # Filter out existing dates
            new_date_records = []
            for _, date_record in date_df.iterrows():
                date_dict = date_record.to_dict()
                exists = duck_conn.execute(f"SELECT 1 FROM DimDate WHERE date_id = ?", [date_dict['date_id']]).fetchone()
                if not exists:
                    new_date_records.append(date_dict)
            
            dim_stats['DimDate'] = {
                'inserted': batch_insert_records(duck_conn, 'DimDate', new_date_records),
                'updated': 0,
                'unchanged': len(date_df) - len(new_date_records)
            }
        
            # 5.5. Insert dữ liệu vào FactJobPostingDaily và FactJobLocationBridge
            logger.info("Xử lý FactJobPostingDaily và FactJobLocationBridge")
            
            # Thực hiện transaction riêng cho việc tạo fact records
            try:
                # Tạo fact records mà không sử dụng transaction nội bộ
                fact_records, bridge_records = fact_handler.generate_fact_records(staging_batch)
                logger.info(f"Đã xử lý {len(fact_records)} bản ghi fact và {len(bridge_records)} bản ghi bridge")
            except Exception as e:
                logger.error(f"Lỗi khi tạo fact records: {str(e)}")
                raise
            
            # Kiểm tra tính toàn vẹn của dữ liệu
            if not verify_etl_integrity(len(staging_batch), len(fact_records)):
                logger.warning("⚠️ Phát hiện vấn đề về tính toàn vẹn dữ liệu trong quá trình ETL!")
            
            # Kiểm tra duplicate một lần nữa
            duplicate_check = duck_conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT job_sk, date_id, COUNT(*) 
                    FROM FactJobPostingDaily 
                    GROUP BY job_sk, date_id 
                    HAVING COUNT(*) > 1
                )
            """).fetchone()
            
            if duplicate_check and duplicate_check[0] > 0:
                logger.warning(f"⚠️ Phát hiện {duplicate_check[0]} nhóm duplicate sau khi insert - cần chạy lại cleanup!")
                cleanup_result_2 = fact_handler.cleanup_duplicate_fact_records()
                if not cleanup_result_2.get('success', False):
                    logger.warning(f"⚠️ Vẫn có vấn đề trong cleanup: {cleanup_result_2.get('message', 'Unknown error')}")
            
            # 6. Quản lý partition và export sang Parquet
            logger.info("📊 Quản lý partition và export sang Parquet")
            partition_result = partition_manager.manage_partitions()
            
            # 7. Tổng kết ETL
            logger.info("="*60)
            logger.info("📊 TỔNG KẾT ETL STAGING TO DWH")
            logger.info("="*60)
            
            total_inserted = sum(stats.get('inserted', 0) for stats in dim_stats.values())
            total_updated = sum(stats.get('updated', 0) for stats in dim_stats.values())
            total_unchanged = sum(stats.get('unchanged', 0) for stats in dim_stats.values())
            
            for table, stats in dim_stats.items():
                logger.info(f"{table:15} - Insert: {stats['inserted']:5}, Update: {stats['updated']:5}, Unchanged: {stats['unchanged']:5}")
            
            logger.info("-"*60)
            logger.info(f"TỔNG DIM        - Insert: {total_inserted:5}, Update: {total_updated:5}, Unchanged: {total_unchanged:5}")
            
            # Log partition stats
            if partition_result.get('success', False):
                logger.info(f"PARTITION       - Thành công: {partition_result.get('message', '')}")
            else:
                logger.warning(f"PARTITION       - Thất bại: {partition_result.get('message', '')}")
            
            # 8. Validation và Data Quality Check
            logger.info("🔍 Bắt đầu validation ETL...")
            validation_success = True
            validation_message = ""
            
            try:
                from src.utils.etl_validator import generate_etl_report, log_validation_results
                validation_results = generate_etl_report(duck_conn)
                log_validation_results(validation_results)
                
                # Kiểm tra các vấn đề nghiêm trọng
                if validation_results.get('issues', {}).get('critical', 0) > 0:
                    validation_success = False
                    validation_message = f"Phát hiện {validation_results['issues']['critical']} vấn đề nghiêm trọng trong validation"
                    logger.error(validation_message)
            except ImportError:
                logger.warning("Không thể import etl_validator - bỏ qua validation")
            except Exception as e:
                validation_success = False
                validation_message = f"Lỗi khi thực hiện validation: {str(e)}"
                logger.error(validation_message)
        
        # Tính thời gian chạy
        duration = (datetime.now() - start_time).total_seconds()
        
        # Tổng kết
        logger.info("="*60)
        logger.info(f"✅ ETL HOÀN THÀNH TRONG {duration:.2f} GIÂY!")
        logger.info("="*60)
        
        # Thống kê kết quả ETL
        etl_stats = {
            "success": True,
            "source_count": len(staging_batch),
            "fact_count": len(fact_records),
            "bridge_count": len(bridge_records),
            "dim_stats": dim_stats,
            "total_dim_inserted": total_inserted,
            "total_dim_updated": total_updated,
            "partition_success": partition_result.get('success', False),
            "duration_seconds": duration,
            "validation_success": validation_success,
            "validation_message": validation_message
        }
        
        return etl_stats
    
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Lỗi trong quá trình ETL: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "success": False,
            "message": error_msg,
            "duration_seconds": duration
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