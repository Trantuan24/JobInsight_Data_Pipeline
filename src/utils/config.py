"""
Configuration settings for the JobInsight pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
for dir_path in [DATA_DIR, LOGS_DIR]:
    dir_path.mkdir(exist_ok=True)

# Database configurations
# DB_CONFIG = {
#     "host": os.getenv("DB_HOST", "postgres"),
#     "port": int(os.getenv("DB_PORT", 5432)),
#     "user": os.getenv("DB_USER", "jobinsight"),
#     "password": os.getenv("DB_PASSWORD", "jobinsight"),
#     "database": os.getenv("DB_NAME", "jobinsight"),
# }
DB_CONFIG = {
    "database": "job_database",
    "user": "postgres",
    "password": "123456",  # Đổi thành mật khẩu của bạn
    "host": "localhost",
    "port": "5432"
}

# Airflow database config
AIRFLOW_DB_CONFIG = {
    "host": os.getenv("AIRFLOW_DB_HOST", "postgres"),
    "port": int(os.getenv("AIRFLOW_DB_PORT", 5432)),
    "user": os.getenv("AIRFLOW_DB_USER", "jobinsight"),
    "password": os.getenv("AIRFLOW_DB_PASSWORD", "jobinsight"),
    "database": os.getenv("AIRFLOW_DB_NAME", "jobinsight"),
}

# DuckDB warehouse config
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(DATA_DIR / "duck_db/jobinsight_warehouse.duckdb"))

# TopCV crawler settings
TOPCV_BASE_URL = "https://www.topcv.vn"
TOPCV_SEARCH_URL = f"{TOPCV_BASE_URL}/tim-viec-lam-moi-nhat"
TOPCV_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
CRAWL_DELAY = int(os.getenv("CRAWL_DELAY", 2))  # seconds
MAX_JOBS_PER_CRAWL = int(os.getenv("MAX_JOBS_PER_CRAWL", 50))
CRAWL_KEYWORDS = [
    'Backend', 'Frontend', 'Fullstack', 'DevOps', 'QA', 
    'Python', 'Java', 'JavaScript', 'React', 'NodeJS',
    'Data', 'AI', 'Machine Learning', 'Senior', 'Junior'
]

# Data Ingestion settings
RAW_JOBS_TABLE = "raw_jobs"
RAW_BATCH_SIZE = 100

# # Data Processing settings
# PROCESSED_JOBS_TABLE = "processed_jobs"
# DIMENSION_TABLES = {
#     "dim_company": "dim_company",
#     "dim_location": "dim_location", 
#     "dim_skills": "dim_skills",
#     "dim_date": "dim_date"
# }
# FACT_TABLE = "fact_jobs"

# ETL settings
ETL_BATCH_SIZE = 500
ETL_MAX_WORKERS = 4
ETL_TIMEOUT = 600  # seconds

# Data Warehouse settings 
DWH_SCHEMA = "jobinsight_dwh"
DWH_STAGING_SCHEMA = "jobinsight_staging"
STAGING_JOBS_TABLE = f"{DWH_STAGING_SCHEMA}.staging_jobs"
DWH_TABLES = {
    "DimJob": f"{DWH_SCHEMA}.DimJob",
    "DimCompany": f"{DWH_SCHEMA}.DimCompany",
    "DimLocation": f"{DWH_SCHEMA}.DimLocation",
    "DimDate": f"{DWH_SCHEMA}.DimDate",
    "FactJobPostingDaily": f"{DWH_SCHEMA}.FactJobPostingDaily",
    "FactJobLocationBridge": f"{DWH_SCHEMA}.FactJobLocationBridge"
}

# Job filtering settings
FILTER_MIN_SALARY = 10_000_000  # VND
FILTER_MAX_SALARY = 15_000_000  # VND
FILTER_LOCATION = "Hà Nội"
TOP_N_JOBS = 10

# Discord webhook settings
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Airflow settings
AIRFLOW_DAG_ID = "jobinsight_pipeline"
AIRFLOW_SCHEDULE = "0 9 * * *"  # Run at 9:00 AM daily
AIRFLOW_CATCHUP = False
AIRFLOW_RETRIES = 3
AIRFLOW_RETRY_DELAY = 5  # minutes
AIRFLOW_FERNET_KEY = "46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho="

# Logging settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO") 