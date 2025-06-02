#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Airflow DAG cho việc crawl dữ liệu công việc từ TopCV
Pipeline: Crawl Jobs → Ingest to Database

Author: JobInsight Team
Date: 2025-05-29
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Thêm đường dẫn project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))

# Import các modules cần thiết
try:
    from src.utils.config import (
        AIRFLOW_DAG_ID, AIRFLOW_SCHEDULE, AIRFLOW_CATCHUP, 
        AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY
    )
    from src.utils.logger import get_logger
    logger = get_logger("airflow.crawl_jobs")
except ImportError as e:
    print(f"Warning: Could not import config: {e}")
    # Fallback values
    AIRFLOW_DAG_ID = "jobinsight_crawl_jobs"
    AIRFLOW_SCHEDULE = "0 9 * * *"
    AIRFLOW_CATCHUP = False
    AIRFLOW_RETRIES = 3
    AIRFLOW_RETRY_DELAY = 5
    # Fallback logger
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("airflow.crawl_jobs")

# Default arguments cho DAG
default_args = {
    'owner': 'jobinsight_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': AIRFLOW_RETRIES,
    'retry_delay': timedelta(minutes=AIRFLOW_RETRY_DELAY),
    'catchup': AIRFLOW_CATCHUP,
}

# Tạo DAG instance
dag = DAG(
    'crawl_jobs_pipeline',
    default_args=default_args,
    description='Pipeline crawl dữ liệu công việc từ TopCV và ingest vào database',
    schedule_interval=AIRFLOW_SCHEDULE,
    start_date=days_ago(1),
    catchup=AIRFLOW_CATCHUP,
    tags=['jobinsight', 'crawler', 'topcv'],
    max_active_runs=1,  # Chỉ cho phép 1 run tại một thời điểm
)

def run_crawler(**context):
    """
    Task chạy crawler để crawl dữ liệu từ TopCV
    """
    try:
        logger.info("Bắt đầu crawl dữ liệu từ TopCV...")
        
        # Import crawler modules
        from crawler.crawler import crawl_multiple_keywords
        
        # Chạy crawler với 1 trang cho mỗi keyword
        df_result = crawl_multiple_keywords(num_pages=1)
        
        if df_result is not None and not df_result.empty:
            job_count = len(df_result)
            logger.info(f"Crawler hoàn thành: {job_count} jobs")
            return {
                'status': 'SUCCESS',
                'job_count': job_count,
                'dataframe_size': df_result.shape
            }
        else:
            logger.warning("Crawler không thu thập được dữ liệu")
            return {
                'status': 'WARNING',
                'job_count': 0,
                'message': 'No data collected'
            }
        
    except Exception as e:
        logger.error(f"Lỗi khi chạy crawler: {str(e)}")
        raise

def run_ingestion(**context):
    """
    Task ingest dữ liệu crawled vào database
    """
    try:
        logger.info("Bắt đầu ingest dữ liệu vào database...")
        
        # Import modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from crawler.crawler import crawl_multiple_keywords
        from ingestion.ingest import ingest_dataframe, setup_database_schema
        
        # Thiết lập database schema trước
        setup_result = setup_database_schema()
        if not setup_result:
            raise ValueError("Không thể thiết lập database schema!")
        
        # Lấy dữ liệu từ crawler (có thể sử dụng cache hoặc crawl lại)
        df = crawl_multiple_keywords(num_pages=1)
        
        if df is not None and not df.empty:
            # Ingest vào database
            imported_count = ingest_dataframe(df)
            
            logger.info(f"Ingestion hoàn thành: {imported_count} records")
            return {
                'status': 'SUCCESS',
                'imported_count': imported_count
            }
        else:
            raise ValueError("Không có dữ liệu để ingest!")
        
    except Exception as e:
        logger.error(f"Lỗi khi ingest dữ liệu: {str(e)}")
        raise

def validate_crawled_data(**context):
    """
    Task validate dữ liệu đã crawl
    """
    try:
        logger.info("Kiểm tra tính hợp lệ của dữ liệu đã crawl...")
        
        # Kiểm tra file tracking jobs có tồn tại không
        tracking_dir = os.path.join(PROJECT_ROOT, 'data', 'job_tracking')
        if not os.path.exists(tracking_dir):
            raise ValueError("Thư mục job_tracking không tồn tại!")
        
        # Kiểm tra có file tracking mới nhất không
        import glob
        tracking_files = glob.glob(os.path.join(tracking_dir, 'jobs_tracking_*.json'))
        if not tracking_files:
            raise ValueError("Không tìm thấy file tracking jobs!")
        
        latest_file = max(tracking_files, key=os.path.getctime)
        file_size = os.path.getsize(latest_file)
        
        logger.info(f"File tracking mới nhất: {latest_file}, Size: {file_size} bytes")
        
        # Kiểm tra file không rỗng
        if file_size < 100:  # Tối thiểu 100 bytes
            raise ValueError(f"File tracking quá nhỏ: {file_size} bytes")
        
        return {
            'latest_file': latest_file,
            'file_size': file_size,
            'validation_status': 'PASSED'
        }
        
    except Exception as e:
        logger.error(f"Validation thất bại: {str(e)}")
        raise

# Định nghĩa các tasks
task_validate_env = BashOperator(
    task_id='validate_environment',
    bash_command='''
    echo "Kiểm tra môi trường..."
    cd {{ params.project_root }}
    
    # Kiểm tra thư mục cần thiết
    if [ ! -d "src/crawler" ]; then
        echo "ERROR: Thư mục src/crawler không tồn tại!"
        exit 1
    fi
    
    if [ ! -d "data" ]; then
        echo "Tạo thư mục data..."
        mkdir -p data/job_tracking/backups
    fi
    
    if [ ! -d "logs" ]; then
        echo "Tạo thư mục logs..."
        mkdir -p logs
    fi
    
    echo "Môi trường OK!"
    ''',
    params={'project_root': PROJECT_ROOT},
    dag=dag,
)

task_run_crawler = PythonOperator(
    task_id='run_topcv_crawler',
    python_callable=run_crawler,
    dag=dag,
)

task_validate_data = PythonOperator(
    task_id='validate_crawled_data',
    python_callable=validate_crawled_data,
    dag=dag,
)

task_run_ingestion = PythonOperator(
    task_id='run_data_ingestion',
    python_callable=run_ingestion,
    dag=dag,
)

task_cleanup = BashOperator(
    task_id='cleanup_old_files',
    bash_command='''
    echo "Dọn dẹp files cũ..."
    cd {{ params.project_root }}/data/job_tracking/backups
    
    # Giữ lại 7 file backup gần nhất
    ls -t jobs_tracking_backup_*.json | tail -n +8 | xargs -r rm -f
    
    echo "Cleanup hoàn thành!"
    ''',
    params={'project_root': PROJECT_ROOT},
    dag=dag,
)

# Thiết lập dependencies
task_validate_env >> task_run_crawler >> task_validate_data >> task_run_ingestion >> task_cleanup

# Export DAG
globals()['crawl_jobs_pipeline'] = dag