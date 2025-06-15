import json
import re
from datetime import datetime
import sys
import os

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("crawler.utils")

def log_action(action, page_num=None, status="success", error=None, attempt=1, user_agent=None):
    """Log mỗi hành động dưới dạng JSON"""
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "action": action,
        "page": page_num,
        "status": status,
        "attempt": attempt,
        "user_agent": user_agent,
        "error": str(error) if error else None
    }
    logger.info(json.dumps(log_entry))
    return log_entry

def parse_last_update(update_text):
    """Phân tích chuỗi ngày cập nhật và chuyển đổi thành số giây"""
    if not update_text:
        return 0
        
    try:
        # Xử lý cả "Cập nhật xxx trước" và "xxx trước"
        if "Cập nhật" in update_text:
            time_part = update_text.replace("Cập nhật", "").strip()
        else:
            time_part = update_text.strip()
            
        # Xử lý các định dạng chuỗi thời gian
        if "tháng" in time_part:
            months = int(re.search(r'(\d+)', time_part).group(1))
            return months * 30 * 24 * 60 * 60  # Giả định 1 tháng = 30 ngày
        elif "tuần" in time_part:
            weeks = int(re.search(r'(\d+)', time_part).group(1))
            return weeks * 7 * 24 * 60 * 60  # 1 tuần = 7 ngày
        elif "ngày" in time_part:
            days = int(re.search(r'(\d+)', time_part).group(1))
            return days * 24 * 60 * 60  # Chuyển thành số giây
        elif "giờ" in time_part:
            hours = int(re.search(r'(\d+)', time_part).group(1))
            return hours * 60 * 60  # Chuyển thành số giây
        elif "phút" in time_part:
            minutes = int(re.search(r'(\d+)', time_part).group(1))
            return minutes * 60  # Chuyển thành số giây
        elif "giây" in time_part:
            seconds = int(re.search(r'(\d+)', time_part).group(1))
            return seconds
        else:
            logger.warning(f"Không nhận dạng được định dạng thời gian: '{update_text}'")
            return 0
    except Exception as e:
        logger.error(f"Lỗi xử lý chuỗi thời gian '{update_text}': {str(e)}")
        return 0 