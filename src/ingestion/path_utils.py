#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module chứa các hằng số và hàm tiện ích liên quan đến đường dẫn cho ingestion package.
"""

import os
import sys

# Thiết lập đường dẫn
# Đường dẫn đến thư mục hiện tại của module
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Đường dẫn đến thư mục gốc của dự án
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))

# Đảm bảo thư mục data tồn tại
# Đường dẫn đến thư mục data của dự án
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

def get_data_dir(subdir=None):
    """
    Trả về đường dẫn đến thư mục data hoặc thư mục con của nó.
    
    Hàm này đảm bảo thư mục tồn tại trước khi trả về đường dẫn. Nếu thư mục
    chưa tồn tại, nó sẽ được tạo tự động.
    
    Args:
        subdir (str, optional): Tên thư mục con trong thư mục data.
            Nếu None, sẽ trả về đường dẫn đến thư mục data gốc.
        
    Returns:
        str: Đường dẫn đầy đủ đến thư mục
    
    Examples:
        >>> get_data_dir()
        '/path/to/project/data'
        >>> get_data_dir('raw')
        '/path/to/project/data/raw'
    """
    if subdir:
        path = os.path.join(DATA_DIR, subdir)
        os.makedirs(path, exist_ok=True)
        return path
    return DATA_DIR 