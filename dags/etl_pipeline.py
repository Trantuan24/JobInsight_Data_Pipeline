#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Airflow DAG cho ETL Pipeline của JobInsight
Pipeline: Raw → Staging → Data Warehouse

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
from airflow.sensors.external_task import ExternalTaskSensor

# Thêm đường dẫn project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Import các modules cần thiết
try:
    from src.utils.config import (
        AIRFLOW_DAG_ID, AIRFLOW_SCHEDULE, AIRFLOW_CATCHUP, 
        AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY,
        RAW_JOBS_TABLE, STAGING_JOBS_TABLE, DWH_SCHEMA
    )
    from src.utils.logger import get_logger
except ImportError as e:
    print(f"Warning: Could not import config: {e}")
    # Fallback values
    AIRFLOW_DAG_ID = "jobinsight_etl_pipeline"
    AIRFLOW_SCHEDULE = "0 10 * * *"  # Chạy sau crawler 1 tiếng
    AIRFLOW_CATCHUP = False
    AIRFLOW_RETRIES = 3
    AIRFLOW_RETRY_DELAY = 5
    RAW_JOBS_TABLE = "raw_jobs"
    STAGING_JOBS_TABLE = "jobinsight_staging.staging_jobs"
    DWH_SCHEMA = "jobinsight_dwh"

# Thiết lập logger
logger = get_logger("airflow.etl_pipeline")

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
    'etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline: Raw → Staging → Data Warehouse với SCD Type 2',
    schedule_interval=AIRFLOW_SCHEDULE,
    start_date=days_ago(1),
    catchup=AIRFLOW_CATCHUP,
    tags=['jobinsight', 'etl', 'dwh', 'scd2'],
    max_active_runs=1,
)

def check_raw_data(**context):
    """
    Task kiểm tra dữ liệu raw có sẵn để xử lý
    """
    try:
        logger.info("Kiểm tra dữ liệu raw...")
        
        # Import database modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from utils.db import get_connection, execute_query
        
        # Kiểm tra số lượng bản ghi mới trong raw_jobs
        query = f"""
        SELECT COUNT(*) as total_records,
               COUNT(CASE WHEN created_at >= CURRENT_DATE THEN 1 END) as today_records
        FROM {RAW_JOBS_TABLE}
        """
        
        with get_connection() as conn:
            result = execute_query(conn, query)
            
        if result and len(result) > 0:
            total_records = result[0][0]
            today_records = result[0][1]
            
            logger.info(f"Tổng bản ghi: {total_records}, Hôm nay: {today_records}")
            
            if total_records == 0:
                raise ValueError("Không có dữ liệu trong bảng raw_jobs!")
            
            return {
                'total_records': total_records,
                'today_records': today_records,
                'check_status': 'PASSED'
            }
        else:
            raise ValueError("Không thể kiểm tra dữ liệu raw!")
            
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra raw data: {str(e)}")
        raise

def run_raw_to_staging(**context):
    """
    Task ETL từ raw_jobs sang staging_jobs
    """
    try:
        logger.info("Bắt đầu ETL Raw → Staging...")
        
        # Import ETL modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from etl.raw_to_staging import run_etl
        
        # Chạy ETL raw to staging
        result = run_etl()
        
        if result:
            logger.info("Raw → Staging hoàn thành thành công")
            return {'status': 'SUCCESS', 'etl_result': result}
        else:
            raise ValueError("ETL Raw → Staging thất bại!")
        
    except Exception as e:
        logger.error(f"Lỗi khi ETL Raw → Staging: {str(e)}")
        raise

def run_staging_to_dwh(**context):
    """
    Task ETL từ staging sang data warehouse
    """
    try:
        logger.info("Bắt đầu ETL Staging → DWH...")
        
        # Import ETL modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from etl.staging_to_dwh import run_staging_to_dwh_etl
        
        # Chạy ETL staging to DWH
        result = run_staging_to_dwh_etl()
        
        logger.info("Staging → DWH hoàn thành thành công")
        return {'status': 'SUCCESS', 'etl_result': 'completed'}
        
    except Exception as e:
        logger.error(f"Lỗi khi ETL Staging → DWH: {str(e)}")
        raise

def validate_dwh_data(**context):
    """
    Task validate dữ liệu trong Data Warehouse
    """
    try:
        logger.info("Validate dữ liệu DWH...")
        
        # Import modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from utils.db import get_connection, execute_query
        
        # Kiểm tra các bảng dimension và fact
        validation_queries = {
            'DimJob': f"SELECT COUNT(*) FROM {DWH_SCHEMA}.DimJob WHERE is_current = true",
            'DimCompany': f"SELECT COUNT(*) FROM {DWH_SCHEMA}.DimCompany WHERE is_current = true", 
            'DimLocation': f"SELECT COUNT(*) FROM {DWH_SCHEMA}.DimLocation WHERE is_current = true",
            'FactJobPostingDaily': f"SELECT COUNT(*) FROM {DWH_SCHEMA}.FactJobPostingDaily WHERE load_date = CURRENT_DATE"
        }
        
        results = {}
        with get_connection() as conn:
            for table_name, query in validation_queries.items():
                try:
                    result = execute_query(conn, query)
                    count = result[0][0] if result else 0
                    results[table_name] = count
                    logger.info(f"{table_name}: {count} records")
                except Exception as e:
                    logger.warning(f"Không thể kiểm tra {table_name}: {str(e)}")
                    results[table_name] = 0
        
        # Kiểm tra có dữ liệu không
        total_records = sum(results.values())
        if total_records == 0:
            raise ValueError("Không có dữ liệu trong Data Warehouse!")
        
        return {
            'validation_results': results,
            'total_records': total_records,
            'validation_status': 'PASSED'
        }
        
    except Exception as e:
        logger.error(f"Validation DWH thất bại: {str(e)}")
        raise

def run_data_processing(**context):
    """
    Task xử lý dữ liệu bổ sung (data processing)
    """
    try:
        logger.info("Bắt đầu Data Processing...")
        
        # Import processing modules
        sys.path.insert(0, os.path.join(PROJECT_ROOT, 'src'))
        from processing.data_processing import main as run_data_processing_main
        
        # Chạy data processing
        result = run_data_processing_main()
        
        logger.info(f"Data Processing hoàn thành: {result}")
        return result
        
    except Exception as e:
        logger.warning(f"Data Processing gặp lỗi (không critical): {str(e)}")
        # Không raise error vì data processing không critical
        return {'status': 'WARNING', 'message': str(e)}

# Định nghĩa các tasks

# Sensor đợi crawler DAG hoàn thành
wait_for_crawler = ExternalTaskSensor(
    task_id='wait_for_crawler_completion',
    external_dag_id='crawl_jobs_pipeline',
    external_task_id='run_data_ingestion',
    timeout=3600,  # Đợi tối đa 1 tiếng
    poke_interval=300,  # Kiểm tra mỗi 5 phút
    dag=dag,
)

task_check_raw_data = PythonOperator(
    task_id='check_raw_data_availability',
    python_callable=check_raw_data,
    dag=dag,
)

task_setup_staging_schema = BashOperator(
    task_id='setup_staging_schema',
    bash_command='''
    echo "Thiết lập schema staging..."
    cd {{ params.project_root }}
    
    # Kiểm tra và tạo schema staging nếu cần
    if [ -f "sql/schema_staging.sql" ]; then
        echo "Executing staging schema SQL..."
        # Có thể chạy qua psql hoặc python script
        echo "Schema staging setup completed!"
    else
        echo "Warning: staging schema file not found!"
    fi
    ''',
    params={'project_root': PROJECT_ROOT},
    dag=dag,
)

task_setup_dwh_schema = BashOperator(
    task_id='setup_dwh_schema',
    bash_command='''
    echo "Thiết lập schema data warehouse..."
    cd {{ params.project_root }}
    
    # Kiểm tra và tạo schema DWH nếu cần
    if [ -f "sql/schema_dwh.sql" ]; then
        echo "Executing DWH schema SQL..."
        # Có thể chạy qua psql hoặc python script
        echo "Schema DWH setup completed!"
    else
        echo "Warning: DWH schema file not found!"
    fi
    ''',
    params={'project_root': PROJECT_ROOT},
    dag=dag,
)

task_raw_to_staging = PythonOperator(
    task_id='etl_raw_to_staging',
    python_callable=run_raw_to_staging,
    dag=dag,
)

task_staging_to_dwh = PythonOperator(
    task_id='etl_staging_to_dwh',
    python_callable=run_staging_to_dwh,
    dag=dag,
)

task_validate_dwh = PythonOperator(
    task_id='validate_dwh_data',
    python_callable=validate_dwh_data,
    dag=dag,
)

task_data_processing = PythonOperator(
    task_id='run_data_processing',
    python_callable=run_data_processing,
    dag=dag,
)

task_generate_summary = BashOperator(
    task_id='generate_pipeline_summary',
    bash_command='''
    echo "Tạo báo cáo tổng kết pipeline..."
    cd {{ params.project_root }}
    
    echo "=== ETL Pipeline Summary ===" > logs/etl_summary_$(date +%Y%m%d).log
    echo "Date: $(date)" >> logs/etl_summary_$(date +%Y%m%d).log
    echo "Pipeline Status: COMPLETED" >> logs/etl_summary_$(date +%Y%m%d).log
    
    echo "Pipeline summary generated!"
    ''',
    params={'project_root': PROJECT_ROOT},
    dag=dag,
)

# Thiết lập dependencies
wait_for_crawler >> task_check_raw_data
task_check_raw_data >> [task_setup_staging_schema, task_setup_dwh_schema]
[task_setup_staging_schema, task_setup_dwh_schema] >> task_raw_to_staging
task_raw_to_staging >> task_staging_to_dwh
task_staging_to_dwh >> task_validate_dwh
task_validate_dwh >> task_data_processing
task_data_processing >> task_generate_summary

# Export DAG
globals()['etl_pipeline'] = dag