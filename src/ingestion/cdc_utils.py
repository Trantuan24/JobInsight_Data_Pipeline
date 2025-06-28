#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module chứa các hàm tiện ích liên quan đến CDC (Change Data Capture) cho ingestion package.
"""

import os
from datetime import datetime
from typing import Dict, Any, Tuple

# Thiết lập đường dẫn
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

# Import logger
try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("ingestion.cdc_utils")

# Constants
CDC_DIR = os.path.join(PROJECT_ROOT, "data/cdc")
os.makedirs(CDC_DIR, exist_ok=True)

def get_cdc_filepath(timestamp=None):
    """
    Tạo đường dẫn file CDC dựa trên timestamp.
    Được sử dụng bởi cả ingest và cdc modules.
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    # Tạo cấu trúc thư mục với tháng/năm để tổ chức dữ liệu CDC tốt hơn
    year_month = timestamp.strftime('%Y%m')
    day = timestamp.strftime('%d')
    cdc_dir_dated = os.path.join(CDC_DIR, year_month, day)
    os.makedirs(cdc_dir_dated, exist_ok=True)
    
    # Tạo file name với timestamp để tránh ghi đè
    return os.path.join(cdc_dir_dated, f"jobs_cdc_{timestamp.strftime('%Y%m%d_%H%M%S')}.jsonl")

def prepare_cdc_record(job_id: str, action: str, data: Dict[str, Any]):
    """
    Tạo đối tượng CDC record với các thông tin cần thiết.
    Được sử dụng bởi cdc module.
    """
    timestamp = datetime.now()
    
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
    
    return cdc_record, timestamp 