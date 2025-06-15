from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import sys
import os
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import modules
from src.crawler.crawler import backup_html_pages, parse_html_files
from src.ingestion.ingest import ingest_dataframe

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Tăng số lần retry
    'retry_delay': timedelta(minutes=2),  # Giảm thời gian chờ retry
}

def backup_html_task(**kwargs):
    """Task 1: Backup HTML pages từ TopCV"""
    import asyncio
    
    logger.info("Starting HTML backup task")
    results = asyncio.run(backup_html_pages(num_pages=5))
    
    # Count successful backups
    successful = sum(1 for r in results if r.get("success", False))
    total = len(results)
    
    logger.info(f"Backup completed: {successful}/{total} pages successful")
    
    if successful == 0:
        raise Exception("Failed to backup any HTML pages")
    
    # Save to XCom for monitoring
    return {
        "total": total,
        "successful": successful,
        "failed": total - successful,
        "timestamp": datetime.now().isoformat(),
        "filenames": [r.get("filename") for r in results if r.get("success", False)] 
    }

def parse_and_save_task(**kwargs):
    """Task 2: Parse HTML files và lưu vào database"""
    logger.info("Starting parse and save task")
    
    # Get previous task result from XCom
    ti = kwargs['ti']
    backup_result = ti.xcom_pull(task_ids='backup_html')
    if backup_result:
        logger.info(f"Processing {backup_result['successful']} HTML files")
    
    # Parse HTML files
    df = parse_html_files()
    
    if df.empty:
        logger.warning("No data parsed from HTML files")
        return {
            "status": "no_data",
            "message": "No data to ingest",
            "timestamp": datetime.now().isoformat()
        }
    
    logger.info(f"Parsed {len(df)} jobs from HTML files")
    
    # Ensure job_id is string type
    if 'job_id' in df.columns:
        df['job_id'] = df['job_id'].astype(str)
        
    # Log thông tin về skills để phân tích
    if 'skills' in df.columns:
        all_skills = []
        for skills_list in df['skills']:
            if isinstance(skills_list, list):
                all_skills.extend(skills_list)
        
        # Top 10 skills phổ biến nhất
        from collections import Counter
        top_skills = Counter(all_skills).most_common(10)
        logger.info(f"Top 10 skills trong data crawled: {json.dumps(top_skills)}")
    
    # Ingest to database
    try:
        result = ingest_dataframe(df)
        
        logger.info(f"Ingestion completed: {result['inserted']} inserted, {result['updated']} updated")
        
        result_data = {
            "status": "success",
            "total_processed": len(df),
            "inserted": result['inserted'],
            "updated": result['updated'],
            "timestamp": datetime.now().isoformat()
        }
        
        # Thêm thông tin về công ty và lĩnh vực
        if 'company_name' in df.columns:
            result_data['companies'] = df['company_name'].nunique()
        
        if 'location' in df.columns:
            result_data['locations'] = df['location'].nunique()
            
        return result_data
        
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        raise

with DAG(
    'crawl_topcv_jobs',
    default_args=default_args,
    description='Crawl job data from TopCV and ingest to database',
    schedule_interval='0 12 * * *',  # Run at 12:00 AM daily
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['jobinsight', 'crawler', 'etl'],
) as dag:

    start = DummyOperator(task_id='start')
    
    backup_html = PythonOperator(
        task_id='backup_html',
        python_callable=backup_html_task,
        provide_context=True,
    )
    
    parse_and_save = PythonOperator(
        task_id='parse_and_save',
        python_callable=parse_and_save_task,
        provide_context=True,
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> backup_html >> parse_and_save >> end 
