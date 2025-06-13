"""
Logger configuration for the application
"""

import logging
import os
from pathlib import Path
import datetime

from src.utils.config import LOGS_DIR, LOG_LEVEL
LOGS_DIR = Path("/opt/airflow/logs")

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
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
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
        log_file = LOGS_DIR / f"{name}_{today}.log"
        
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
