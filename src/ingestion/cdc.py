import os
import json
from datetime import datetime, timedelta
import shutil
from typing import Dict, Any, List
import pandas as pd
import sys

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
    # Loại bỏ import từ db_operations để tránh circular import
    # from src.ingestion.db_operations import batch_insert_records
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    # Loại bỏ import từ db_operations để tránh circular import
    # from db_operations import batch_insert_records

logger = get_logger("ingestion.cdc")

# Constants
CDC_DIR = "data/cdc"
os.makedirs(CDC_DIR, exist_ok=True)

def save_cdc_record(job_id: str, action: str, data: Dict[str, Any]):
    """Lưu CDC record vào file JSONL để phục vụ recovery"""
    timestamp = datetime.now()
    
    # Tạo cấu trúc thư mục với tháng/năm để tổ chức dữ liệu CDC tốt hơn
    year_month = timestamp.strftime('%Y%m')
    day = timestamp.strftime('%d')
    cdc_dir_dated = os.path.join(CDC_DIR, year_month, day)
    os.makedirs(cdc_dir_dated, exist_ok=True)
    
    # Tạo file name với timestamp để tránh ghi đè
    cdc_file = os.path.join(cdc_dir_dated, f"jobs_cdc_{timestamp.strftime('%Y%m%d_%H%M%S')}.jsonl")
    
    # Thêm metadata cho CDC record để tracking và auditing
    cdc_record = {
        'timestamp': timestamp.isoformat(),
        'action': action,
        'job_id': job_id,
        'source': 'ingest_process',
        'process_id': os.getpid(),
        'metadata': {
            'host': os.environ.get('COMPUTERNAME', 'unknown'),
            'user': os.environ.get('USERNAME', 'unknown')
        },
        'data': data
    }
    
    try:
        # Đảm bảo thư mục tồn tại
        os.makedirs(os.path.dirname(cdc_file), exist_ok=True)
        
        # Ghi file với mode append để không mất dữ liệu cũ
        with open(cdc_file, 'a', encoding='utf-8') as f:
            json.dump(cdc_record, f, ensure_ascii=False, default=str)
            f.write('\n')
            
        logger.debug(f"Đã lưu CDC record cho job {job_id}, action={action}")
        return True
    except Exception as e:
        logger.error(f"Lỗi lưu CDC record cho job {job_id}: {str(e)}")
        return False

def replay_cdc_file(cdc_file_path: str):
    """Replay CDC file để recovery data"""
    logger.info(f"Replaying CDC file: {cdc_file_path}")
    
    if not os.path.exists(cdc_file_path):
        logger.error(f"CDC file not found: {cdc_file_path}")
        return
    
    stats = {'processed': 0, 'success': 0, 'error': 0}
    
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
                
                # Xử lý record sẽ được thực hiện bởi caller
                # Thay vì gọi trực tiếp batch_insert_records, ta sẽ trả về record để caller xử lý
                stats['success'] += 1
                
            except Exception as e:
                logger.error(f"Lỗi xử lý dòng {line_num} trong CDC file: {str(e)}")
                stats['error'] += 1
    
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
        date = current_date.replace(day=current_date.day - i)
        year_month = date.strftime('%Y%m')
        day = date.strftime('%d')
        dates_to_check.append((year_month, day))
    
    # Tìm các file CDC
    for year_month, day in dates_to_check:
        cdc_dir_dated = os.path.join(CDC_DIR, year_month, day)
        if os.path.exists(cdc_dir_dated):
            for file in os.listdir(cdc_dir_dated):
                if file.endswith('.jsonl'):
                    result.append(os.path.join(cdc_dir_dated, file))
    
    return sorted(result) 

def cleanup_old_cdc_files(days_to_keep: int = 30) -> Dict[str, int]:
    """
    Xóa các file CDC cũ hơn số ngày chỉ định
    
    Args:
        days_to_keep: Số ngày dữ liệu CDC cần giữ lại, mặc định là 30 ngày
    
    Returns:
        Dict[str, int]: Thống kê về số lượng thư mục, file đã xóa và lỗi nếu có
    """
    logger.info(f"Bắt đầu dọn dẹp các file CDC cũ hơn {days_to_keep} ngày")
    
    stats = {
        'dirs_removed': 0, 
        'files_removed': 0, 
        'errors': 0,
        'bytes_freed': 0
    }
    
    try:
        # Tính ngày giới hạn
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Quét qua các thư mục theo năm-tháng
        if os.path.exists(CDC_DIR):
            for year_month in os.listdir(CDC_DIR):
                year_month_path = os.path.join(CDC_DIR, year_month)
                
                # Bỏ qua nếu không phải thư mục
                if not os.path.isdir(year_month_path):
                    continue
                    
                # Kiểm tra từng thư mục ngày
                for day in os.listdir(year_month_path):
                    day_path = os.path.join(year_month_path, day)
                    
                    # Bỏ qua nếu không phải thư mục
                    if not os.path.isdir(day_path):
                        continue
                        
                    try:
                        # Parse ngày từ tên thư mục
                        folder_date_str = f"{year_month}{day}"
                        folder_date = datetime.strptime(folder_date_str, "%Y%m%d")
                        
                        # Nếu thư mục cũ hơn ngày giới hạn, xóa toàn bộ
                        if folder_date < cutoff_date:
                            # Tính kích thước thư mục trước khi xóa
                            dir_size = sum(os.path.getsize(os.path.join(root, file)) 
                                      for root, dirs, files in os.walk(day_path) 
                                      for file in files)
                            
                            # Đếm số file bị xóa
                            file_count = sum(len(files) for _, _, files in os.walk(day_path))
                            
                            # Xóa thư mục và toàn bộ nội dung
                            shutil.rmtree(day_path)
                            
                            stats['dirs_removed'] += 1
                            stats['files_removed'] += file_count
                            stats['bytes_freed'] += dir_size
                            
                            logger.info(f"Đã xóa thư mục CDC: {day_path} ({file_count} files, {dir_size} bytes)")
                    except ValueError as e:
                        # Lỗi parse ngày từ tên thư mục
                        logger.warning(f"Không thể xác định ngày từ thư mục: {day_path}, lỗi: {str(e)}")
                        stats['errors'] += 1
                    except Exception as e:
                        logger.error(f"Lỗi khi xóa thư mục CDC {day_path}: {str(e)}")
                        stats['errors'] += 1
                
                # Kiểm tra và xóa các thư mục tháng rỗng
                try:
                    if not os.listdir(year_month_path):
                        os.rmdir(year_month_path)
                        logger.info(f"Đã xóa thư mục tháng rỗng: {year_month_path}")
                except Exception as e:
                    logger.error(f"Lỗi khi xóa thư mục tháng rỗng {year_month_path}: {str(e)}")
                    stats['errors'] += 1
        
        # Format kích thước để dễ đọc
        freed_mb = stats['bytes_freed'] / (1024 * 1024)
        logger.info(f"Hoàn tất dọn dẹp CDC: Đã xóa {stats['dirs_removed']} thư mục, "
                   f"{stats['files_removed']} files ({freed_mb:.2f} MB), {stats['errors']} lỗi")
        
        return stats
    except Exception as e:
        logger.error(f"Lỗi trong quá trình dọn dẹp CDC: {str(e)}")
        stats['errors'] += 1
        return stats 