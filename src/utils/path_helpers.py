#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module cung cấp các hàm tiện ích để xử lý đường dẫn một cách nhất quán.
Giúp đảm bảo tất cả đường dẫn đều được xử lý theo cùng một cách trong dự án.
"""

import os
from pathlib import Path
from typing import Union, List, Optional

PathLike = Union[str, Path]

def ensure_path(path: PathLike) -> Path:
    """
    Đảm bảo đầu vào là một đối tượng Path.
    
    Args:
        path: Đường dẫn dạng chuỗi hoặc đối tượng Path
    
    Returns:
        Path: Đối tượng Path
    """
    if isinstance(path, str):
        return Path(path)
    return path

def ensure_str(path: PathLike) -> str:
    """
    Đảm bảo đầu vào là một chuỗi đường dẫn.
    
    Args:
        path: Đường dẫn dạng chuỗi hoặc đối tượng Path
    
    Returns:
        str: Chuỗi đường dẫn
    """
    if isinstance(path, Path):
        return str(path)
    return path

def ensure_dir(path: PathLike) -> Path:
    """
    Đảm bảo thư mục tồn tại và trả về đối tượng Path.
    
    Args:
        path: Đường dẫn dạng chuỗi hoặc đối tượng Path
        
    Returns:
        Path: Đối tượng Path cho thư mục đã tạo
    """
    path = ensure_path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def glob_files(directory: PathLike, pattern: str) -> List[Path]:
    """
    Tìm các file theo pattern và trả về danh sách đối tượng Path.
    Hoạt động với cả đường dẫn dạng chuỗi và đối tượng Path.
    
    Args:
        directory: Thư mục cần tìm
        pattern: Mẫu glob pattern (ví dụ: "*.html")
        
    Returns:
        List[Path]: Danh sách các đường dẫn tìm thấy
    """
    directory = ensure_path(directory)
    return sorted(directory.glob(pattern))

def join_paths(base: PathLike, *parts: str) -> Path:
    """
    Nối các phần đường dẫn lại với nhau và trả về đối tượng Path.
    
    Args:
        base: Đường dẫn cơ sở
        *parts: Các phần cần nối
        
    Returns:
        Path: Đường dẫn đã nối
    """
    base = ensure_path(base)
    return base.joinpath(*parts)

def get_timestamp_filename(base_dir: PathLike, prefix: str, suffix: str, timestamp_format: str = "%Y%m%d%H%M%S") -> Path:
    """
    Tạo tên file với timestamp.
    
    Args:
        base_dir: Thư mục cơ sở
        prefix: Tiền tố của tên file
        suffix: Hậu tố của tên file (phần mở rộng)
        timestamp_format: Định dạng timestamp
        
    Returns:
        Path: Đường dẫn đầy đủ với timestamp
    """
    from datetime import datetime
    timestamp = datetime.now().strftime(timestamp_format)
    filename = f"{prefix}_{timestamp}{suffix}"
    return join_paths(base_dir, filename) 