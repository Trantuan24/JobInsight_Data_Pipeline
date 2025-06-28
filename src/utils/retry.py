#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module cung cấp các decorator và tiện ích xử lý retry.
"""

import time
import random
import functools
import inspect
import asyncio
from typing import Callable, List, Optional, Type, Union, Any, Dict
import logging

# Import module logger nếu có thể
try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("utils.retry")

def retry(
    max_tries: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception
):
    """
    Decorator để retry các hàm thông thường khi gặp exception.
    
    Args:
        max_tries: Số lần thử tối đa (bao gồm lần đầu tiên)
        delay_seconds: Thời gian delay ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng delay (x2, x3, ...)
        jitter: Có thêm jitter ngẫu nhiên hay không để tránh thundering herd
        exceptions: Exception hoặc danh sách exceptions cần retry
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    tries += 1
                    if tries >= max_tries:
                        logger.warning(f"Hàm {func.__name__} thất bại sau {tries} lần thử: {str(e)}")
                        raise
                    
                    # Tính delay với backoff
                    delay = delay_seconds * (backoff_factor ** (tries - 1))
                    
                    # Thêm jitter nếu cần
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.info(f"Retry {tries}/{max_tries-1} cho hàm {func.__name__} sau {delay:.2f}s: {str(e)}")
                    time.sleep(delay)
        
        return wrapper
    
    return decorator

def async_retry(
    max_tries: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception
):
    """
    Decorator để retry các hàm async khi gặp exception.
    
    Args:
        max_tries: Số lần thử tối đa (bao gồm lần đầu tiên)
        delay_seconds: Thời gian delay ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng delay (x2, x3, ...)
        jitter: Có thêm jitter ngẫu nhiên hay không để tránh thundering herd
        exceptions: Exception hoặc danh sách exceptions cần retry
        
    Returns:
        Decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            tries = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    tries += 1
                    if tries >= max_tries:
                        logger.warning(f"Async hàm {func.__name__} thất bại sau {tries} lần thử: {str(e)}")
                        raise
                    
                    # Tính delay với backoff
                    delay = delay_seconds * (backoff_factor ** (tries - 1))
                    
                    # Thêm jitter nếu cần
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.info(f"Async retry {tries}/{max_tries-1} cho hàm {func.__name__} sau {delay:.2f}s: {str(e)}")
                    await asyncio.sleep(delay)
        
        return wrapper
    
    return decorator

class RetryManager:
    """
    Class quản lý các cơ chế retry khác nhau với cấu hình
    """
    
    def __init__(
        self, 
        max_tries: int = 3,
        delay_seconds: float = 1.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception
    ):
        """
        Khởi tạo RetryManager
        
        Args:
            max_tries: Số lần thử tối đa (bao gồm lần đầu tiên)
            delay_seconds: Thời gian delay ban đầu giữa các lần thử (giây)
            backoff_factor: Hệ số tăng delay (x2, x3, ...)
            jitter: Có thêm jitter ngẫu nhiên hay không
            exceptions: Exception hoặc danh sách exceptions cần retry
        """
        self.max_tries = max_tries
        self.delay_seconds = delay_seconds
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.exceptions = exceptions
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Thực thi một hàm với retry
        
        Args:
            func: Hàm cần thực thi
            *args: Arguments cho hàm
            **kwargs: Keyword arguments cho hàm
            
        Returns:
            Any: Kết quả từ hàm
            
        Raises:
            Exception: Nếu hàm vẫn thất bại sau số lần retry tối đa
        """
        tries = 0
        last_exception = None
        
        while tries < self.max_tries:
            try:
                return func(*args, **kwargs)
            except self.exceptions as e:
                tries += 1
                last_exception = e
                
                if tries >= self.max_tries:
                    logger.warning(f"Hàm {func.__name__} thất bại sau {tries} lần thử: {str(e)}")
                    break
                
                # Tính delay với backoff
                delay = self.delay_seconds * (self.backoff_factor ** (tries - 1))
                
                # Thêm jitter nếu cần
                if self.jitter:
                    delay = delay * (0.5 + random.random())
                
                logger.info(f"Retry {tries}/{self.max_tries-1} cho hàm {func.__name__} sau {delay:.2f}s: {str(e)}")
                time.sleep(delay)
        
        # Nếu tới đây, có nghĩa là đã retry hết số lần cho phép
        if last_exception:
            raise last_exception
        
        # Fallback nếu không có exception (không nên xảy ra)
        raise RuntimeError(f"Hàm {func.__name__} thất bại sau {self.max_tries} lần thử mà không có exception")

    async def execute_async_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Thực thi một hàm async với retry
        
        Args:
            func: Hàm async cần thực thi
            *args: Arguments cho hàm
            **kwargs: Keyword arguments cho hàm
            
        Returns:
            Any: Kết quả từ hàm
            
        Raises:
            Exception: Nếu hàm vẫn thất bại sau số lần retry tối đa
        """
        tries = 0
        last_exception = None
        
        while tries < self.max_tries:
            try:
                return await func(*args, **kwargs)
            except self.exceptions as e:
                tries += 1
                last_exception = e
                
                if tries >= self.max_tries:
                    logger.warning(f"Async hàm {func.__name__} thất bại sau {tries} lần thử: {str(e)}")
                    break
                
                # Tính delay với backoff
                delay = self.delay_seconds * (self.backoff_factor ** (tries - 1))
                
                # Thêm jitter nếu cần
                if self.jitter:
                    delay = delay * (0.5 + random.random())
                
                logger.info(f"Async retry {tries}/{self.max_tries-1} cho hàm {func.__name__} sau {delay:.2f}s: {str(e)}")
                await asyncio.sleep(delay)
        
        # Nếu tới đây, có nghĩa là đã retry hết số lần cho phép
        if last_exception:
            raise last_exception
        
        # Fallback nếu không có exception (không nên xảy ra)
        raise RuntimeError(f"Async hàm {func.__name__} thất bại sau {self.max_tries} lần thử mà không có exception") 