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
    from src.ingestion.cdc_utils import get_cdc_filepath, prepare_cdc_record, CDC_DIR
    from src.utils.path_helpers import ensure_path, ensure_dir
    from src.common.decorators import retry
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from src.ingestion.cdc_utils import get_cdc_filepath, prepare_cdc_record, CDC_DIR
    from src.utils.path_helpers import ensure_path, ensure_dir
    # Fallback cho retry decorator
    def retry(max_tries=3, delay_seconds=1.0, **kwargs):
        def decorator(func):
            return func
        return decorator

logger = get_logger("ingestion.cdc")

@retry(max_tries=3, delay_seconds=1.0, backoff_factor=2.0, logger=logger, 
       exceptions=[IOError, filelock.Timeout])
def save_cdc_record(job_id: str, action: str, data: Dict[str, Any]):
    """Lưu CDC record vào file JSONL để phục vụ recovery"""
    # Sử dụng hàm từ cdc_utils
    cdc_record, timestamp = prepare_cdc_record(job_id, action, data)
    
    # Lấy đường dẫn file từ cdc_utils
    cdc_file = get_cdc_filepath(timestamp)
    
    try:
        # Sử dụng file locking để xử lý concurrency
        with filelock.FileLock(f"{cdc_file}.lock", timeout=10):
            # Ghi file với mode append để không mất dữ liệu cũ
            with open(cdc_file, 'a', encoding='utf-8') as f:
                json.dump(cdc_record, f, ensure_ascii=False, default=str)
                f.write('\n')
            
        logger.debug(f"Đã lưu CDC record cho job {job_id}, action={action}")
        return True
    except Exception as e:
        logger.error(f"Lỗi lưu CDC record cho job {job_id}: {str(e)}")
        return False

@retry(max_tries=2, delay_seconds=1.0, logger=logger, exceptions=[IOError, json.JSONDecodeError])
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

@retry(max_tries=2, delay_seconds=2.0, logger=logger, exceptions=[IOError, OSError, shutil.Error])
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

# Thêm class CDC_Handler từ cdc_handler.py
class CDC_Handler:
    """
    Class quản lý tập trung các thao tác CDC (Change Data Capture)
    - Đảm bảo ghi log CDC khi có thay đổi dữ liệu
    - Cung cấp interface đơn giản cho các module khác
    """
    
    def __init__(self):
        """Khởi tạo CDC_Handler"""
        # Đảm bảo thư mục CDC tồn tại
        self.cdc_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "cdc")
        ensure_dir(self.cdc_dir)
        logger.info(f"Khởi tạo CDC_Handler với thư mục: {self.cdc_dir}")
    
    def log_record(self, job_id: str, action: str, data: Dict[str, Any]) -> bool:
        """
        Ghi log CDC cho một record
        
        Args:
            job_id: ID của job
            action: Loại hành động (insert, update, delete)
            data: Dữ liệu của record
            
        Returns:
            bool: True nếu thành công
        """
        logger.debug(f"Ghi CDC record cho job {job_id}, action={action}")
        
        try:
            # Sử dụng hàm save_cdc_record từ module cdc
            success = save_cdc_record(job_id, action, data)
            if success:
                logger.debug(f"Đã lưu CDC record thành công cho job {job_id}")
            else:
                logger.warning(f"Lưu CDC record không thành công cho job {job_id}")
            return success
            
        except Exception as e:
            logger.error(f"Lỗi khi ghi CDC record cho job {job_id}: {str(e)}")
            return False
    
    def log_batch(self, records: List[Dict[str, Any]], action: str) -> Dict[str, int]:
        """
        Ghi log CDC cho một batch records
        
        Args:
            records: Danh sách các record
            action: Loại hành động (insert, update, delete)
            
        Returns:
            Dict[str, int]: Thống kê kết quả {'total': int, 'success': int, 'failed': int}
        """
        stats = {'total': len(records), 'success': 0, 'failed': 0}
        
        if not records:
            logger.warning("Không có records để lưu CDC")
            return stats
        
        logger.info(f"Bắt đầu ghi CDC cho {len(records)} records, action={action}")
        
        for record in records:
            job_id = record.get('job_id')
            
            if not job_id:
                logger.warning(f"Record không có job_id, bỏ qua: {record}")
                stats['failed'] += 1
                continue
            
            success = self.log_record(job_id, action, record)
            
            if success:
                stats['success'] += 1
            else:
                stats['failed'] += 1
        
        logger.info(f"Hoàn thành ghi CDC: {stats['success']}/{stats['total']} thành công")
        return stats


# Singleton instance
_instance = None

def get_cdc_handler() -> CDC_Handler:
    """
    Lấy singleton instance của CDC_Handler
    
    Returns:
        CDC_Handler: Instance của CDC_Handler
    """
    global _instance
    if _instance is None:
        _instance = CDC_Handler()
    return _instance
