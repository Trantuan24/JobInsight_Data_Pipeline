import os
import json
from datetime import datetime, timedelta
import shutil
from typing import Dict, Any, List, Tuple
import pandas as pd
import sys
import filelock

# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.path_helpers import ensure_path, ensure_dir
    from src.utils.retry import retry
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

    # Fallback path helpers
    def ensure_path(path):
        from pathlib import Path
        return Path(path) if isinstance(path, str) else path

    def ensure_dir(path):
        dir_path = ensure_path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path

    # Fallback retry decorator
    def retry(max_tries=3, delay_seconds=1.0, **kwargs):
        def decorator(func):
            return func
        return decorator

# CDC Configuration
CDC_DIR = ensure_dir(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "cdc"))

logger = get_logger("ingestion.cdc")

def get_cdc_filepath(timestamp: datetime) -> str:
    """Get CDC file path based on timestamp"""
    date_str = timestamp.strftime("%Y%m%d")
    year_month = timestamp.strftime("%Y%m")

    # Create directory structure: CDC_DIR/YYYYMM/YYYYMMDD/
    day_dir = ensure_dir(CDC_DIR / year_month / date_str)

    # File name: cdc_YYYYMMDD.jsonl
    filename = f"cdc_{date_str}.jsonl"
    return str(day_dir / filename)

def prepare_cdc_record(job_id: str, action: str, data: Dict[str, Any]) -> Tuple[Dict[str, Any], datetime]:
    """Prepare CDC record for saving"""
    timestamp = datetime.now()

    cdc_record = {
        "timestamp": timestamp.isoformat(),
        "job_id": job_id,
        "action": action,
        "data": data,
        "metadata": {
            "source": "crawler",
            "version": "1.0"
        }
    }

    return cdc_record, timestamp

@retry(max_tries=3, delay_seconds=1.0, backoff_factor=2.0,
       exceptions=[IOError, filelock.Timeout])
def save_cdc_record(job_id: str, action: str, data: Dict[str, Any]):
    """Lưu CDC record vào file JSONL để phục vụ recovery"""
    # Prepare CDC record
    cdc_record, timestamp = prepare_cdc_record(job_id, action, data)

    # Get file path
    cdc_file = get_cdc_filepath(timestamp)
    
    try:
        # Sử dụng file locking để xử lý concurrency
        with filelock.FileLock(f"{cdc_file}.lock", timeout=10):
            # Optimize I/O: combine json serialization và write trong single operation
            json_line = json.dumps(cdc_record, ensure_ascii=False, default=str) + '\n'
            with open(cdc_file, 'a', encoding='utf-8') as f:
                f.write(json_line)
            
        logger.debug(f"Đã lưu CDC record cho job {job_id}, action={action}")
        return True
    except Exception as e:
        logger.error(f"Lỗi lưu CDC record cho job {job_id}: {str(e)}")
        return False

@retry(max_tries=2, delay_seconds=1.0, exceptions=[IOError, json.JSONDecodeError])
def replay_cdc_file(cdc_file_path: str):
    """
    DEPRECATED: Replay CDC file để recovery data

    WARNING: Function này không được sử dụng trong luồng hiện tại và sẽ bị loại bỏ.
    Chỉ giữ lại để tương thích với docs và có thể sử dụng trong tương lai.
    """
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
    """
    DEPRECATED: Liệt kê các CDC files trong khoảng thời gian nhất định

    WARNING: Function này không được sử dụng trong luồng hiện tại và sẽ bị loại bỏ.
    Chỉ giữ lại để tương thích với docs và có thể sử dụng trong tương lai.
    """
    result = []
    
    # Tính toán các ngày cần kiểm tra
    current_date = datetime.now()
    dates_to_check = []
    for i in range(days_back):
        try:
            date = current_date - timedelta(days=i)
            year_month = date.strftime('%Y%m')
            day = date.strftime('%d')
            dates_to_check.append((year_month, day))
        except ValueError:
            # Xử lý trường hợp ngày không hợp lệ
            continue
    
    # Tìm các file CDC
    for year_month, day in dates_to_check:
        cdc_dir_dated = os.path.join(CDC_DIR, year_month, day)
        if os.path.exists(cdc_dir_dated):
            for file in os.listdir(cdc_dir_dated):
                if file.endswith('.jsonl'):
                    result.append(os.path.join(cdc_dir_dated, file))
    
    return sorted(result) 

@retry(max_tries=2, delay_seconds=2.0, exceptions=[IOError, OSError, shutil.Error])
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
                        logger.warning(f"Không thể parse ngày từ thư mục {day_path}: {str(e)}")
                        stats['errors'] += 1
                    except Exception as e:
                        logger.error(f"Lỗi khi xóa thư mục {day_path}: {str(e)}")
                        stats['errors'] += 1
                        
                # Kiểm tra xem thư mục năm-tháng có rỗng không, nếu rỗng thì xóa
                if os.path.exists(year_month_path) and not os.listdir(year_month_path):
                    try:
                        os.rmdir(year_month_path)
                        logger.info(f"Đã xóa thư mục rỗng: {year_month_path}")
                    except Exception as e:
                        logger.error(f"Lỗi khi xóa thư mục rỗng {year_month_path}: {str(e)}")
                    
        logger.info(f"Hoàn thành dọn dẹp CDC: {stats['dirs_removed']} thư mục, "
                    f"{stats['files_removed']} files, {stats['bytes_freed'] / (1024*1024):.2f} MB")
        
    except Exception as e:
        logger.error(f"Lỗi khi dọn dẹp CDC files: {str(e)}")
        stats['errors'] += 1
        
    return stats

# CDC_Handler class removed - use save_cdc_record function directly


# CDC_Handler singleton removed - use save_cdc_record function directly
