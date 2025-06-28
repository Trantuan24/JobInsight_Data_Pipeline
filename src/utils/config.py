#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Cấu hình trung tâm cho toàn bộ ứng dụng JobInsight.
Sử dụng cấu trúc phân cấp với các namespace rõ ràng.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

# Thử import dotenv, nếu không có thì bỏ qua
try:
    from dotenv import load_dotenv
    # Nạp biến môi trường từ .env
    load_dotenv()
except ImportError:
    # Thông báo cho người dùng biết không có dotenv
    logging.warning("Package 'python-dotenv' not found. Environment variables will not be loaded from .env file.")

# Import module path_helpers nếu có thể
try:
    from src.utils.path_helpers import ensure_dir, ensure_path
except ImportError:
    # Fallback implementation nếu chưa có module
    def ensure_dir(path):
        if isinstance(path, str):
            path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def ensure_path(path):
        if isinstance(path, str):
            return Path(path)
        return path

# First, define base paths outside of the Config class
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Config:
    """Cấu hình trung tâm với cấu trúc phân cấp"""
    
    # Base Paths - Now referencing the externally defined BASE_DIR
    BASE_DIR = BASE_DIR
    
    class Dirs:
        """Namespace cho các đường dẫn thư mục"""
        DATA_DIR = ensure_dir(BASE_DIR / "data")  # Now using the externally defined BASE_DIR
        LOGS_DIR = ensure_dir(BASE_DIR / "logs")
        CDC_DIR = ensure_dir(DATA_DIR / "cdc")
        BACKUP_DIR = ensure_dir(DATA_DIR / "raw_backup")
        DUCK_DIR = ensure_dir(DATA_DIR / "duck_db")
        PARQUET_DIR = ensure_dir(DATA_DIR / "parquet")
    
    class Database:
        """Namespace cho cấu hình cơ sở dữ liệu"""
        HOST = os.getenv("DB_HOST", "postgres")
        PORT = int(os.getenv("DB_PORT", "5432"))
        USER = os.getenv("DB_USER", "jobinsight")
        PASSWORD = os.getenv("DB_PASSWORD", "jobinsight")
        NAME = os.getenv("DB_NAME", "jobinsight")
        
        # Bảng
        RAW_JOBS_TABLE = "raw_jobs"
        STAGING_JOBS_TABLE = "jobinsight_staging.staging_jobs"

        # Schema
        DWH_SCHEMA = "jobinsight_dwh"
        DWH_STAGING_SCHEMA = "jobinsight_staging"
        
        # Bảng DWH
        DWH_TABLES = {
            "DimJob": f"{DWH_SCHEMA}.DimJob",
            "DimCompany": f"{DWH_SCHEMA}.DimCompany",
            "DimLocation": f"{DWH_SCHEMA}.DimLocation",
            "DimDate": f"{DWH_SCHEMA}.DimDate",
            "FactJobPostingDaily": f"{DWH_SCHEMA}.FactJobPostingDaily",
            "FactJobLocationBridge": f"{DWH_SCHEMA}.FactJobLocationBridge"
        }

        @classmethod
        def get_connection_params(cls) -> Dict[str, Any]:
            """Lấy thông số kết nối database"""
            return {
                "host": cls.HOST,
                "port": cls.PORT,
                "user": cls.USER,
                "password": cls.PASSWORD,
                "database": cls.NAME,
                "dbname": cls.NAME  # Alias cho PostgreSQL
            }
    
    class Crawler:
        """Namespace cho cấu hình crawler"""
        BASE_URL = os.getenv("CRAWLER_BASE_URL", "https://www.topcv.vn/viec-lam-it")
        NUM_PAGES = int(os.getenv("CRAWLER_NUM_PAGES", "5"))
        MIN_DELAY = float(os.getenv("CRAWLER_MIN_DELAY", "4"))
        MAX_DELAY = float(os.getenv("CRAWLER_MAX_DELAY", "8"))
        PAGE_LOAD_TIMEOUT = int(os.getenv("CRAWLER_PAGE_LOAD_TIMEOUT", "60000"))
        SELECTOR_TIMEOUT = int(os.getenv("CRAWLER_SELECTOR_TIMEOUT", "20000"))
        MAX_RETRY = int(os.getenv("CRAWLER_MAX_RETRY", "3"))
        RETRY_DELAYS = [2, 4, 8]  # Backoff delays in seconds
        
        # TopCV specific
        TOPCV_BASE_URL = "https://www.topcv.vn"
        TOPCV_SEARCH_URL = f"{TOPCV_BASE_URL}/tim-viec-lam-moi-nhat"
        CRAWL_DELAY = int(os.getenv("CRAWL_DELAY", "2"))  # seconds
        MAX_JOBS_PER_CRAWL = int(os.getenv("MAX_JOBS_PER_CRAWL", "50"))
        
        # Keywords
        CRAWL_KEYWORDS = [
            'Backend', 'Frontend', 'Fullstack', 'DevOps', 'QA', 
            'Python', 'Java', 'JavaScript', 'React', 'NodeJS',
            'Data', 'AI', 'Machine Learning', 'Senior', 'Junior'
        ]
    
    class CDC:
        """Namespace cho cấu hình CDC"""
        DAYS_TO_KEEP = int(os.getenv("CDC_DAYS_TO_KEEP", "15"))
        FILE_LOCK_TIMEOUT = int(os.getenv("CDC_LOCK_TIMEOUT", "10"))
    
    class Threading:
        """Namespace cho cấu hình đa luồng"""
        MAX_WORKERS = min(
            int(os.getenv("MAX_WORKERS", "32")),
            (os.cpu_count() or 1) * 5
        )
    
    class ETL:
        """Namespace cho cấu hình ETL"""
        BATCH_SIZE = int(os.getenv("ETL_BATCH_SIZE", "500"))
        MAX_WORKERS = int(os.getenv("ETL_MAX_WORKERS", "4"))
        TIMEOUT = int(os.getenv("ETL_TIMEOUT", "600"))  # seconds
        RAW_BATCH_SIZE = int(os.getenv("RAW_BATCH_SIZE", "20"))
    
    class Parquet:
        """Namespace cho cấu hình Parquet"""
        MONTHS_TO_KEEP = int(os.getenv("PARQUET_MONTHS_TO_KEEP", "12"))
    
    class DuckDB:
        """Namespace cho cấu hình DuckDB"""
        PATH = os.getenv("DUCKDB_PATH", str(BASE_DIR / "data/duck_db/jobinsight_warehouse.duckdb"))
    
    class Airflow:
        """Namespace cho cấu hình Airflow"""
        DAG_ID = "jobinsight_pipeline"
        SCHEDULE = "0 9 * * *"  # Run at 9:00 AM daily
        CATCHUP = False
        RETRIES = 3
        RETRY_DELAY = 5  # minutes
        FERNET_KEY = os.getenv("AIRFLOW_FERNET_KEY", "46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=")
        
        # Database
        DB_CONFIG = {
            "host": os.getenv("AIRFLOW_DB_HOST", "postgres"),
            "port": int(os.getenv("AIRFLOW_DB_PORT", "5432")),
            "user": os.getenv("AIRFLOW_DB_USER", "jobinsight"),
            "password": os.getenv("AIRFLOW_DB_PASSWORD", "jobinsight"),
            "database": os.getenv("AIRFLOW_DB_NAME", "jobinsight"),
        }
    
    class Logging:
        """Namespace cho cấu hình logging"""
        LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    class Filtering:
        """Namespace cho cấu hình lọc dữ liệu"""
        MIN_SALARY = 10_000_000  # VND
        MAX_SALARY = 15_000_000  # VND
        LOCATION = "Hà Nội"
        TOP_N_JOBS = 10

    class Discord:
        """Namespace cho cấu hình Discord"""
        WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Tải cấu hình từ file (nếu có)
CONFIG_FILE = Config.BASE_DIR / "config.json"
if CONFIG_FILE.exists():
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
            
            # TODO: Merge user_config vào Config
            # Hiện tại giữ lại để tương thích ngược
            
    except Exception as e:
        logging.warning(f"Error loading config file {CONFIG_FILE}: {str(e)}")

# Hàm trợ giúp để lấy cấu hình (legacy support)
def get_config(section, key=None, default=None):
    """Lấy cấu hình từ section và key (tương thích với code cũ)"""
    config_map = {
        'db': Config.Database.get_connection_params(),
        'crawler': {
            'num_pages': Config.Crawler.NUM_PAGES,
            'base_url': Config.Crawler.BASE_URL,
            'min_delay': Config.Crawler.MIN_DELAY,
            'max_delay': Config.Crawler.MAX_DELAY,
            'page_load_timeout': Config.Crawler.PAGE_LOAD_TIMEOUT,
            'selector_timeout': Config.Crawler.SELECTOR_TIMEOUT,
            'max_retry': Config.Crawler.MAX_RETRY,
            'retry_delays': Config.Crawler.RETRY_DELAYS,
        },
        'cdc': {
            'days_to_keep': Config.CDC.DAYS_TO_KEEP,
            'file_lock_timeout': Config.CDC.FILE_LOCK_TIMEOUT,
        },
        'threading': {
            'max_workers': Config.Threading.MAX_WORKERS,
        },
        'parquet': {
            'months_to_keep': Config.Parquet.MONTHS_TO_KEEP,
        },
    }
    
    if section not in config_map:
        return default
        
    if key is None:
        return config_map[section]
        
    return config_map[section].get(key, default)

# Legacy constants for backward compatibility
BACKUP_DIR = Config.Dirs.BACKUP_DIR
CDC_DIR = Config.Dirs.CDC_DIR
DATA_DIR = Config.Dirs.DATA_DIR
LOGS_DIR = Config.Dirs.LOGS_DIR
DB_CONFIG = Config.Database.get_connection_params()
CRAWLER_CONFIG = get_config('crawler')
RAW_JOBS_TABLE = Config.Database.RAW_JOBS_TABLE
STAGING_JOBS_TABLE = Config.Database.STAGING_JOBS_TABLE
DWH_SCHEMA = Config.Database.DWH_SCHEMA
DWH_STAGING_SCHEMA = Config.Database.DWH_STAGING_SCHEMA
DWH_TABLES = Config.Database.DWH_TABLES
DUCKDB_PATH = Config.DuckDB.PATH
RAW_BATCH_SIZE = Config.ETL.RAW_BATCH_SIZE
FILTER_MIN_SALARY = Config.Filtering.MIN_SALARY
FILTER_MAX_SALARY = Config.Filtering.MAX_SALARY
FILTER_LOCATION = Config.Filtering.LOCATION