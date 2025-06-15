#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import logging
import sys
from datetime import datetime
import pandas as pd
from typing import Dict, Any, List

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
        logging.FileHandler(os.path.join(LOGS_DIR, "db_ingest.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import modules từ utils
try:
    from src.utils.config import DB_CONFIG, RAW_JOBS_TABLE, RAW_BATCH_SIZE
    from src.utils.db import get_connection, execute_query, table_exists, get_engine
    from src.utils.logger import get_logger
    from src.ingestion.data_processor import prepare_job_data
    from src.ingestion.db_operations import batch_insert_records, ensure_table_exists
    from src.ingestion.cdc import save_cdc_record
except ImportError as e:
    # Thử cách thay thế
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))
    from utils.config import DB_CONFIG, RAW_JOBS_TABLE, RAW_BATCH_SIZE
    from utils.db import get_connection, execute_query, table_exists, get_engine
    from utils.logger import get_logger
    from ingestion.data_processor import prepare_job_data
    from ingestion.db_operations import batch_insert_records, ensure_table_exists
    from ingestion.cdc import save_cdc_record

# Đường dẫn đến thư mục SQL
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.getcwd(), "sql")
    os.makedirs(SQL_DIR, exist_ok=True)

# Constants
BATCH_SIZE = 1000  # Insert theo batch để tối ưu performance
CDC_DIR = "data/cdc"
os.makedirs(CDC_DIR, exist_ok=True)

def bulk_upsert_jobs(df: pd.DataFrame, batch_size: int = BATCH_SIZE) -> Dict[str, int]:
    """Bulk UPSERT jobs vào database"""
    if df.empty:
        logger.warning("No data to ingest")
        return {'inserted': 0, 'updated': 0, 'errors': 0}
    
    # Ensure job_id is string
    df['job_id'] = df['job_id'].astype(str)
    
    # Prepare records
    records = []
    for _, row in df.iterrows():
        data = prepare_job_data(row, False)  # Assume new records for simplicity
        records.append(data)
    
    # Batch insert
    inserted, updated, errors = batch_insert_records(records, batch_size)
    
    # Log CDC records
    for record in records:
        job_id = record['job_id']
        # Đơn giản hóa: coi tất cả là insert
        try:
            save_cdc_record(job_id, 'insert', record)
        except Exception as e:
            logger.warning(f"Không thể lưu CDC record cho job {job_id}: {str(e)}")
    
    logger.info(f"Ingestion completed: {inserted} inserted, {updated} updated, {errors} errors")
    return {'inserted': inserted, 'updated': updated, 'errors': errors}

def ingest_dataframe(df: pd.DataFrame) -> Dict[str, int]:
    """Main function to ingest DataFrame into database"""
    logger.info(f"Starting ingestion of {len(df)} records")
    
    # Ensure table exists
    ensure_table_exists()
    
    # Perform bulk upsert
    return bulk_upsert_jobs(df)

def replay_cdc_records(cdc_file_path: str) -> Dict[str, int]:
    """Replay CDC records from a file"""
    logger.info(f"Replaying CDC file: {cdc_file_path}")
    
    if not os.path.exists(cdc_file_path):
        logger.error(f"CDC file not found: {cdc_file_path}")
        return {'processed': 0, 'success': 0, 'error': 0}
    
    stats = {'processed': 0, 'success': 0, 'error': 0}
    records_to_process = []
    
    # Đọc records từ file CDC
    with open(cdc_file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                record = json.loads(line.strip())
                stats['processed'] += 1
                
                # Kiểm tra xem record có hợp lệ không
                if 'job_id' not in record.get('data', {}) or 'action' not in record:
                    logger.warning(f"CDC record không hợp lệ ở dòng {line_num}, bỏ qua")
                    stats['error'] += 1
                    continue
                
                # Lấy dữ liệu và action
                job_id = record['data']['job_id']
                action = record['action']
                timestamp = record.get('timestamp')
                
                logger.info(f"Đang xử lý CDC record dòng {line_num}, job_id={job_id}, action={action}, time={timestamp}")
                
                # Thêm vào danh sách để xử lý batch
                records_to_process.append(record['data'])
                
            except Exception as e:
                logger.error(f"Lỗi xử lý dòng {line_num} trong CDC file: {str(e)}")
                stats['error'] += 1
    
    # Xử lý các records đã đọc được
    if records_to_process:
        try:
            inserted, updated, errors = batch_insert_records(records_to_process, batch_size=10)
            stats['success'] = inserted + updated
            stats['error'] += errors
            logger.info(f"Replayed {len(records_to_process)} records: {inserted} inserted, {updated} updated, {errors} errors")
        except Exception as e:
            logger.error(f"Lỗi khi replay CDC records: {str(e)}")
            stats['error'] += len(records_to_process)
    
    logger.info(f"CDC replay completed: {stats['processed']} records processed, " 
                f"{stats['success']} successful, {stats['error']} errors")
    return stats

def list_cdc_files(days_back: int = 7) -> List[str]:
    """Liệt kê các CDC files trong khoảng thời gian nhất định"""
    result = []
    
    # Tính toán các ngày cần kiểm tra
    current_date = datetime.now()
    dates_to_check = []
    for i in range(days_back):
        try:
            date = current_date.replace(day=current_date.day - i)
            year_month = date.strftime('%Y%m')
            day = date.strftime('%d')
            dates_to_check.append((year_month, day))
        except ValueError:
            # Xử lý trường hợp ngày không hợp lệ (ví dụ: 31/2)
            continue
    
    # Tìm các file CDC
    for year_month, day in dates_to_check:
        cdc_dir_dated = os.path.join(CDC_DIR, year_month, day)
        if os.path.exists(cdc_dir_dated):
            for file in os.listdir(cdc_dir_dated):
                if file.endswith('.jsonl'):
                    result.append(os.path.join(cdc_dir_dated, file))
    
    return sorted(result)

if __name__ == "__main__":
    # Test ingestion
    test_data = {
        'job_id': ['test001', 'test002'],
        'title': ['Python Developer', 'Data Engineer'],
        'company_name': ['ABC Corp', 'XYZ Ltd'],
        'skills': [['Python', 'Django'], ['Python', 'Spark', 'SQL']],
        'location': ['Hà Nội', 'TP.HCM'],
        'salary': ['20-30 triệu', 'Thỏa thuận'],
        'last_update': ['1 ngày trước', '2 giờ trước']
    }
    
    df = pd.DataFrame(test_data)
    result = ingest_dataframe(df)
    print(f"Test result: {result}")

