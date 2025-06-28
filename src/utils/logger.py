"""
Logger configuration for the application
"""

import logging
import os
from pathlib import Path
import datetime

try:
    from src.utils.config import Config
except ImportError:
    # Fallback config
    from src.utils.config import LOGS_DIR, LOG_LEVEL
    
# Fallback nếu cả hai đều không import được
try:
    logs_dir = Config.Dirs.LOGS_DIR
    log_level = Config.Logging.LEVEL
except (NameError, AttributeError):
    try:
        logs_dir = LOGS_DIR
        log_level = LOG_LEVEL
    except NameError:
        logs_dir = Path("/opt/airflow/logs")
        log_level = "INFO"

def setup_logger(name="jobinsight", log_to_file=True):
    """
    Setup logger with appropriate handlers and formatters
    
    Args:
        name (str): Logger name
        log_to_file (bool): Whether to log to file
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level))
    
    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler if needed
    if log_to_file:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        log_file = logs_dir / f"{name}_{today}.log"
        
        # Đảm bảo thư mục logs tồn tại
        os.makedirs(logs_dir, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(module_name):
    """
    Get logger for a module
    
    Args:
        module_name (str): Module name
        
    Returns:
        logging.Logger: Logger for the module
    """
    return logging.getLogger(f"jobinsight.{module_name}")
