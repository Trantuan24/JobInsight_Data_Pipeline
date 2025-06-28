#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module chứa các hàm tiện ích liên quan đến thao tác database cho ingestion package.
"""

import logging
from typing import Dict, Any, List, Tuple

# Import từ utils
try:
    from src.utils.logger import get_logger
    from src.utils.db import get_connection
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from src.utils.db import get_connection

logger = get_logger("ingestion.db_operations_utils")

def execute_db_batch(batch_data, batch_size, upsert_query):
    """
    Hàm chung để thực hiện batch upsert vào database.
    
    Hàm này thực hiện batch upsert vào database sử dụng psycopg2.extras.execute_values
    để tối ưu hiệu suất. Nó xử lý kết nối, thực hiện truy vấn và commit trong một
    transaction duy nhất.
    
    Args:
        batch_data (List[Tuple]): Danh sách các tuple dữ liệu cần upsert
        batch_size (int): Kích thước của mỗi batch khi thực hiện upsert
        upsert_query (str): Câu truy vấn SQL upsert
    
    Returns:
        Tuple[int, int, int]: Tuple chứa số lượng bản ghi đã inserted, updated và errors
        
    Raises:
        Exception: Khi có lỗi xảy ra trong quá trình thực hiện batch upsert
    
    Examples:
        >>> upsert_query = "INSERT INTO jobs (id, title) VALUES %s ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
        >>> batch_data = [(1, "Job 1"), (2, "Job 2")]
        >>> inserted, updated, errors = execute_db_batch(batch_data, 100, upsert_query)
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