"""
Các decorators tiện ích cho dự án JobInsight
"""

import time
import logging
import functools
from typing import Callable, Any, Optional, Type, Union, List
import sys
import os
import traceback

# Thiết lập logger mặc định
default_logger = logging.getLogger("common.decorators")

def retry(
    max_tries: int = 3, 
    delay_seconds: float = 1.0, 
    backoff_factor: float = 2.0, 
    logger: Optional[logging.Logger] = None, 
    exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception
) -> Callable:
    """
    Decorator thực hiện retry cho một hàm khi gặp exception
    
    Args:
        max_tries: Số lần thử tối đa (bao gồm lần chạy đầu tiên)
        delay_seconds: Thời gian chờ ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng thời gian chờ mỗi lần retry
        logger: Logger để ghi lại các lỗi, nếu None sẽ sử dụng logger mặc định
        exceptions: Loại exceptions sẽ được retry, có thể là một Exception hoặc list các Exception
    
    Returns:
        Callable: Decorator function
        
    Example:
        @retry(max_tries=3, delay_seconds=2)
        def function_that_might_fail():
            # implementation...
    """
    # Thiết lập logger
    _logger = logger or default_logger
    
    # Chuyển đổi exception thành list nếu cần
    if not isinstance(exceptions, (list, tuple)):
        exceptions = [exceptions]
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            tries = 0
            delay = delay_seconds
            
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except tuple(exceptions) as e:
                    tries += 1
                    
                    if tries == max_tries:
                        _logger.error(
                            f"Hàm {func.__name__} không thành công sau {max_tries} lần thử: {str(e)}"
                        )
                        # Re-raise exception nếu đã hết số lần thử
                        raise
                    
                    _logger.warning(
                        f"Lần thử {tries}/{max_tries} của hàm {func.__name__} gặp lỗi: {str(e)}. "
                        f"Thử lại sau {delay:.1f}s..."
                    )
                    
                    # Log traceback ở cấp độ DEBUG
                    _logger.debug(f"Traceback: {traceback.format_exc()}")
                    
                    # Chờ trước khi thử lại
                    time.sleep(delay)
                    
                    # Tăng thời gian chờ cho lần thử tiếp theo theo hệ số backoff
                    delay *= backoff_factor
                
        return wrapper
    
    return decorator

def async_retry(
    max_tries: int = 3, 
    delay_seconds: float = 1.0, 
    backoff_factor: float = 2.0, 
    logger: Optional[logging.Logger] = None, 
    exceptions: Union[Type[Exception], List[Type[Exception]]] = Exception
) -> Callable:
    """
    Decorator thực hiện retry cho một coroutine (async function) khi gặp exception
    
    Args:
        max_tries: Số lần thử tối đa (bao gồm lần chạy đầu tiên)
        delay_seconds: Thời gian chờ ban đầu giữa các lần thử (giây)
        backoff_factor: Hệ số tăng thời gian chờ mỗi lần retry
        logger: Logger để ghi lại các lỗi, nếu None sẽ sử dụng logger mặc định
        exceptions: Loại exceptions sẽ được retry, có thể là một Exception hoặc list các Exception
    
    Returns:
        Callable: Decorator function cho async function
        
    Example:
        @async_retry(max_tries=3, delay_seconds=2)
        async def async_function_that_might_fail():
            # implementation...
    """
    # Thiết lập logger
    _logger = logger or default_logger
    
    # Chuyển đổi exception thành list nếu cần
    if not isinstance(exceptions, (list, tuple)):
        exceptions = [exceptions]
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            import asyncio  # Import locally to avoid issues if asyncio not available
            
            tries = 0
            delay = delay_seconds
            
            while tries < max_tries:
                try:
                    return await func(*args, **kwargs)
                except tuple(exceptions) as e:
                    tries += 1
                    
                    if tries == max_tries:
                        _logger.error(
                            f"Hàm async {func.__name__} không thành công sau {max_tries} lần thử: {str(e)}"
                        )
                        # Re-raise exception nếu đã hết số lần thử
                        raise
                    
                    _logger.warning(
                        f"Lần thử {tries}/{max_tries} của hàm async {func.__name__} gặp lỗi: {str(e)}. "
                        f"Thử lại sau {delay:.1f}s..."
                    )
                    
                    # Log traceback ở cấp độ DEBUG
                    _logger.debug(f"Traceback: {traceback.format_exc()}")
                    
                    # Chờ trước khi thử lại
                    await asyncio.sleep(delay)
                    
                    # Tăng thời gian chờ cho lần thử tiếp theo theo hệ số backoff
                    delay *= backoff_factor
                
        return wrapper
    
    return decorator 