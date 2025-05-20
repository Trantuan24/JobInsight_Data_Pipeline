import json
import shutil
import re
import asyncio
import random
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional

# Thêm đường dẫn gốc dự án vào sys.path để import được các module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Thử import logger từ cả hai cách
try:
    from src.utils.logger import get_logger
except ImportError:
    try:
        # Khi chạy trực tiếp file này
        from utils.logger import get_logger
    except ImportError:
        import logging
        logging.basicConfig(level=logging.INFO)
        def get_logger(name):
            return logging.getLogger(name)

# Khởi tạo logger
logger = get_logger("crawler.utils")

# Khóa để đồng bộ hóa truy cập vào dữ liệu chung
job_data_lock = threading.Lock()

# Lưu trữ job_id và thông tin cần kiểm tra
job_data_crawled = {}
job_id_crawled = set()

def get_tracking_filename():
    """Trả về file tracking JSON theo tháng hiện tại"""
    current_month = datetime.now().strftime('%Y%m')
    return f'data/job_tracking/jobs_tracking_{current_month}.json'

def backup_tracking_file():
    """Tạo bản backup cho file tracking hiện tại"""
    current_file = get_tracking_filename()
    if shutil.os.path.exists(current_file):
        timestamp = datetime.now().strftime('%Y%m%d')
        backup_file = f'data/job_tracking/backups/jobs_tracking_backup_{timestamp}.json'
        try:
            shutil.copy2(current_file, backup_file)
            logger.info(f"Created backup: {backup_file}")
            return True
        except Exception as e:
            logger.error(f"Error creating backup: {str(e)}")
    return False

def save_crawled_data():
    """Lưu dữ liệu tracking jobs"""
    # Tạo backup trước khi lưu
    backup_tracking_file()
    
    crawled_jobs_path = get_tracking_filename()
    with open(crawled_jobs_path, 'w', encoding='utf-8') as f:
        json.dump(job_data_crawled, f, ensure_ascii=False, indent=4)
    logger.info(f"Saved {len(job_data_crawled)} tracked jobs to {crawled_jobs_path}")

def load_tracking_data():
    """Tải dữ liệu tracking từ file"""
    global job_data_crawled, job_id_crawled
    
    crawled_jobs_path = get_tracking_filename()
    try:
        with open(crawled_jobs_path, 'r', encoding='utf-8') as f:
            job_data_crawled = json.load(f)
        logger.info(f"Loaded {len(job_data_crawled)} tracked jobs from {crawled_jobs_path}")
    except FileNotFoundError:
        logger.info(f"No tracking file found. Creating new tracking at {crawled_jobs_path}")
    
    # Cập nhật set job ID
    job_id_crawled = set(job_data_crawled.keys())

def parse_last_update(update_text):
    """Phân tích chuỗi ngày cập nhật và chuyển đổi thành số giây"""
    try:
        # Xử lý các định dạng chuỗi thời gian như "3 ngày trước", "1 giờ trước", vv.
        if "tháng" in update_text:
            months = int(re.search(r'(\d+)', update_text).group(1))
            return months * 30 * 24 * 60 * 60  # Giả định 1 tháng = 30 ngày
        elif "tuần" in update_text:
            weeks = int(re.search(r'(\d+)', update_text).group(1))
            return weeks * 7 * 24 * 60 * 60  # 1 tuần = 7 ngày
        elif "ngày" in update_text:
            days = int(re.search(r'(\d+)', update_text).group(1))
            return days * 24 * 60 * 60  # Chuyển thành số giây
        elif "giờ" in update_text:
            hours = int(re.search(r'(\d+)', update_text).group(1))
            return hours * 60 * 60  # Chuyển thành số giây
        elif "phút" in update_text:
            minutes = int(re.search(r'(\d+)', update_text).group(1))
            return minutes * 60  # Chuyển thành số giây
        elif "giây" in update_text:
            seconds = int(re.search(r'(\d+)', update_text).group(1))
            return seconds
        else:
            return 0
    except Exception:
        return 0

async def random_delay(min_seconds=1, max_seconds=3):
    """Tạo độ trễ ngẫu nhiên để mô phỏng hành vi người dùng thực (async version)"""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)
    return delay

# Khởi tạo dữ liệu tracking khi import module
load_tracking_data() 