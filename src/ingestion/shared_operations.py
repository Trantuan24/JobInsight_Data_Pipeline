#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module chứa các hàm dùng chung giữa các module trong ingestion package
để tránh circular import.

DEPRECATED: Module này sẽ bị loại bỏ trong tương lai. Vui lòng sử dụng các module chuyên biệt:
- db_operations_utils.py: Cho các hàm liên quan đến database
- cdc_utils.py: Cho các hàm liên quan đến CDC
- path_utils.py: Cho các hằng số liên quan đến đường dẫn
"""

import os
import json
import logging
import sys
import warnings
from typing import Dict, Any, List, Tuple
from datetime import datetime

# Hiển thị warning về việc sử dụng module deprecated
warnings.warn(
    "Module shared_operations.py đã bị deprecated và sẽ bị loại bỏ trong tương lai. "
    "Vui lòng sử dụng các module chuyên biệt: db_operations_utils.py, cdc_utils.py, path_utils.py",
    DeprecationWarning, stacklevel=2
)

# Import từ các module mới
try:
    from src.ingestion.path_utils import CURRENT_DIR, PROJECT_ROOT
    from src.ingestion.cdc_utils import get_cdc_filepath, prepare_cdc_record, CDC_DIR
    from src.ingestion.db_operations_utils import execute_db_batch
    from src.utils.logger import get_logger
except ImportError:
    # Fallback khi không import được
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    
    # Thiết lập đường dẫn
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
    
    # Import từ utils
    from src.utils.db import get_connection
    
    # Constants
    CDC_DIR = os.path.join(PROJECT_ROOT, "data/cdc")
    os.makedirs(CDC_DIR, exist_ok=True)
    
    # Định nghĩa lại các hàm để đảm bảo tương thích ngược
    def execute_db_batch(batch_data, batch_size, upsert_query):
        """
        Hàm chung để thực hiện batch upsert vào database.
        Được sử dụng bởi cả db_operations và cdc modules.
        """
        inserted = 0
        updated = 0
        errors = 0
        
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                
                # Thực hiện batch upsert với execute_values
                from psycopg2.extras import execute_values
                execute_values(cursor, upsert_query, batch_data, template=None, page_size=batch_size)
                
                # Commit sau mỗi batch
                conn.commit()
                logger.debug(f"Đã xử lý batch với {len(batch_data)} records")
                
                # Kết quả thành công, đánh dấu là inserted
                inserted = len(batch_data)
                
        except Exception as e:
            logger.error(f"Lỗi khi execute_db_batch: {str(e)}")
            errors += len(batch_data)
        
        return inserted, updated, errors

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

logger = get_logger("ingestion.shared_operations") 