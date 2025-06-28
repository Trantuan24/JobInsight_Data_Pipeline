#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module cung cấp các hàm để dọn dẹp file tạm
Hỗ trợ dọn dẹp file CDC và HTML backup
"""

import os
import sys
import glob
import time
import datetime
from pathlib import Path
from typing import Dict, List, Union, Optional, Any
import logging
import shutil


# Import các module
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config
    from src.utils.path_helpers import ensure_path
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    
    # Default config nếu không import được
    class Config:
        class Dirs:
            BACKUP_DIR = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "raw_backup"))
            CDC_DIR = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "cdc"))
        class CDC:
            DAYS_TO_KEEP = 15
    
    def ensure_path(path):
        if isinstance(path, str):
            return Path(path)
        return path

logger = get_logger("utils.cleanup")

def cleanup_html_backups(days_to_keep=None, backup_dir=None):
    """
    Xóa các file HTML backup cũ hơn số ngày cho trước
    
    Args:
        days_to_keep: Số ngày giữ lại file HTML
        backup_dir: Thư mục chứa HTML backup
        
    Returns:
        int: Số lượng file đã xóa
    """
    try:
        # Lấy config
        days_to_keep = days_to_keep or 7  # Default: giữ 7 ngày
        backup_dir = backup_dir or Config.Dirs.BACKUP_DIR
        
        # Đảm bảo backup_dir là Path object
        backup_dir = ensure_path(backup_dir)
        
        if not backup_dir.exists():
            logger.warning(f"Thư mục backup không tồn tại: {backup_dir}")
            return 0
            
        # Tính thời gian cutoff
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
        cutoff_timestamp = cutoff_date.timestamp()
        
        logger.info(f"Tìm các file HTML cũ hơn {days_to_keep} ngày (trước {cutoff_date.strftime('%Y-%m-%d')})")
        
        # Tìm các file html
        files_to_delete = []
        html_files = list(backup_dir.glob("*.html"))
        
        for file_path in html_files:
            if os.path.getmtime(file_path) < cutoff_timestamp:
                files_to_delete.append(file_path)
        
        # Xóa file
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                logger.debug(f"Đã xóa file {file_path}")
            except Exception as e:
                logger.error(f"Lỗi khi xóa file {file_path}: {str(e)}")
        
        logger.info(f"Đã xóa {len(files_to_delete)} file HTML cũ")
        return len(files_to_delete)
        
    except Exception as e:
        logger.error(f"Lỗi khi dọn dẹp HTML backup: {str(e)}")
        return 0

def cleanup_cdc_files(days_to_keep=None, cdc_dir=None):
    """
    Xóa các file CDC cũ hơn số ngày cho trước
    
    Args:
        days_to_keep: Số ngày giữ lại file CDC
        cdc_dir: Thư mục chứa CDC logs
        
    Returns:
        int: Số lượng file đã xóa
    """
    try:
        # Lấy config
        days_to_keep = days_to_keep or Config.CDC.DAYS_TO_KEEP
        cdc_dir = cdc_dir or Config.Dirs.CDC_DIR
        
        # Đảm bảo cdc_dir là Path object
        cdc_dir = ensure_path(cdc_dir)
        
        if not cdc_dir.exists():
            logger.warning(f"Thư mục CDC không tồn tại: {cdc_dir}")
            return 0
            
        # Tính thời gian cutoff
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days_to_keep)
        cutoff_timestamp = cutoff_date.timestamp()
        
        logger.info(f"Tìm các file CDC cũ hơn {days_to_keep} ngày (trước {cutoff_date.strftime('%Y-%m-%d')})")
        
        # Tìm các file json
        files_to_delete = []
        cdc_files = list(cdc_dir.glob("*.json"))
        
        for file_path in cdc_files:
            if os.path.getmtime(file_path) < cutoff_timestamp:
                files_to_delete.append(file_path)
        
        # Xóa file
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                logger.debug(f"Đã xóa file {file_path}")
            except Exception as e:
                logger.error(f"Lỗi khi xóa file {file_path}: {str(e)}")
        
        logger.info(f"Đã xóa {len(files_to_delete)} file CDC cũ")
        return len(files_to_delete)
        
    except Exception as e:
        logger.error(f"Lỗi khi dọn dẹp CDC files: {str(e)}")
        return 0

def cleanup_all_temp_files(html_days_to_keep=7, cdc_days_to_keep=None):
    """
    Dọn dẹp tất cả các loại file tạm
    
    Args:
        html_days_to_keep: Số ngày giữ lại file HTML
        cdc_days_to_keep: Số ngày giữ lại file CDC
        
    Returns:
        Dict[str, int]: Dict với số lượng file đã xóa cho mỗi loại
    """
    html_count = cleanup_html_backups(days_to_keep=html_days_to_keep)
    cdc_count = cleanup_cdc_files(days_to_keep=cdc_days_to_keep)
    
    logger.info(f"Đã dọn dẹp tổng cộng {html_count + cdc_count} file (HTML: {html_count}, CDC: {cdc_count})")
    
    return {
        "html_files": html_count,
        "cdc_files": cdc_count,
        "total": html_count + cdc_count
    }


if __name__ == "__main__":
    # Test dọn dẹp
    result = cleanup_all_temp_files(html_days_to_keep=30, cdc_days_to_keep=15)
    print(f"Kết quả dọn dẹp: {result}") 