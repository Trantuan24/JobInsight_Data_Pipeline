from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import sys
import os
import json
import time
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import modules
from src.crawler.crawler import TopCVCrawler
from src.utils.cleanup import cleanup_all_temp_files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Tăng số lần retry
    'retry_delay': timedelta(minutes=2),  # Giảm thời gian chờ retry
}

def crawl_and_process_task(**kwargs):
    """
    Task thực hiện cả quá trình crawl, parse và lưu vào database
    Sử dụng TopCVCrawler để quản lý toàn bộ quá trình
    """
    logger.info("Starting crawl and process task with TopCVCrawler")
    
    # Cấu hình crawler
    config = {
        'num_pages': 5,            # Số trang cần crawl
        'use_parallel': True,      # Sử dụng crawl song song
        'db_table': 'raw_jobs',    # Bảng để lưu dữ liệu
        'concurrent_backups': 3,   # Số lượng trang crawl song song
    }
    
    # Chạy crawler
    try:
        # TopCVCrawler.run() sẽ quản lý việc tạo event loop
        result = TopCVCrawler.run(config=config)
        
        if not result['success']:
            logger.error(f"Crawler failed: {result.get('error', 'Unknown error')}")
            raise Exception(f"Crawler failed: {result.get('error', 'Unknown error')}")
            
        logger.info(f"Crawl completed successfully in {result['execution_time']:.2f}s")
        logger.info(f"Backup: {result['backup']['successful']}/{result['backup']['total']} pages")
        logger.info(f"Parse: {result['parse']['total_jobs']} jobs parsed")
    
        if 'database' in result:
            logger.info(f"Database: {result['database']['inserted']} inserted, " + 
                        f"{result['database']['updated']} updated")
        
        # Ghi thêm thông tin về companies và locations
        if 'parse' in result:
            parse_result = result['parse']
            logger.info(f"Companies: {parse_result.get('company_count', 0)}")
            logger.info(f"Locations: {parse_result.get('location_count', 0)}")
            
            # Top skills (nếu có)
            if 'top_skills' in parse_result:
                logger.info(f"Top skills: {json.dumps(parse_result['top_skills'])}")
            
        return result
        
    except Exception as e:
        logger.error(f"Error during crawl and process task: {str(e)}")
        raise

def cleanup_temp_files_task(days_to_keep=15, **kwargs):
    """
    Dọn dẹp các file tạm thời như CDC và HTML backup
    
    Args:
        days_to_keep: Số ngày dữ liệu cần giữ lại, mặc định là 15 ngày
    """
    logger.info(f"Bắt đầu dọn dẹp dữ liệu tạm thời cũ hơn {days_to_keep} ngày")
    
    try:
        # Đo thời gian thực thi
        start_time = time.time()
        
        # Gọi hàm cleanup từ module mới
        cleanup_results = cleanup_all_temp_files(html_days_to_keep=days_to_keep, cdc_days_to_keep=days_to_keep)
        
        # Tính thời gian thực hiện
        execution_time = time.time() - start_time
        
        # Tính toán kết quả chi tiết
        stats = {
            'cdc': {
                'files_removed': cleanup_results.get('cdc_files', 0),
            },
            'html': {
                'files_removed': cleanup_results.get('html_files', 0),
            },
            'total_files_removed': cleanup_results.get('total', 0),
            'total_bytes_freed': 0,  # Không có thông tin về bytes nên để mặc định 0
            'execution_time': execution_time,
            'total_errors': 0  # Không có thông tin về errors nên để mặc định 0
        }
        
        # Ghi log kết quả
        logger.info(f"Kết quả dọn dẹp tổng thể:")
        logger.info(f"- CDC files đã xóa: {stats['cdc']['files_removed']}")
        logger.info(f"- HTML files đã xóa: {stats['html']['files_removed']}")
        logger.info(f"- Tổng files đã xóa: {stats['total_files_removed']}")
        logger.info(f"- Dung lượng giải phóng: {stats['total_bytes_freed'] / (1024*1024):.2f} MB")
        logger.info(f"- Thời gian thực hiện: {stats['execution_time']:.2f} giây")
        logger.info(f"- Lỗi: {stats['total_errors']}")
        
        # Trả về kết quả cho XCom
        return stats
    except Exception as e:
        logger.error(f"Lỗi khi dọn dẹp file tạm: {str(e)}")
        raise e

with DAG(
    'crawl_topcv_jobs',
    default_args=default_args,
    description='Crawl job data from TopCV and ingest to database',
    schedule_interval='40 17 * * *',  # Run at 11:00 AM Vietnam time (Asia/Ho_Chi_Minh)
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['jobinsight', 'crawler', 'etl'],
) as dag:

    start = DummyOperator(task_id='start')
    
    # Task mới kết hợp cả backup, parse và save
    crawl_and_process = PythonOperator(
        task_id='crawl_and_process',
        python_callable=crawl_and_process_task,
        provide_context=True,
    )
    
    # Task dọn dẹp cả CDC và HTML backup
    cleanup_temp = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files_task,
        op_kwargs={'days_to_keep': 15},  # Giữ 15 ngày dữ liệu
        provide_context=True,
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> crawl_and_process >> cleanup_temp >> end 