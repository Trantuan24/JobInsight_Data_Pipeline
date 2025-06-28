from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys
import os

# Import các modules cần thiết
from src.processing.data_prepare import prepare_dim_job, prepare_dim_company, prepare_dim_location
from src.processing.data_processing import clean_title, extract_location_info
from src.utils.db import get_connection, get_dataframe, execute_query

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def etl_raw_to_staging(batch_size=5000, only_unprocessed=False, verbose=False):
    """
    Thực hiện ETL từ raw_jobs sang staging_jobs
    
    Args:
        batch_size (int): Số lượng bản ghi xử lý mỗi batch, mặc định là 5000
        only_unprocessed (bool): Chỉ xử lý các bản ghi chưa được xử lý, mặc định là False
        verbose (bool): Hiển thị thêm thông tin chi tiết, mặc định là False
        
    Returns:
        str: Thông tin kết quả ETL
    """
    from src.etl import run_etl
    import json
    
    print(f"Bắt đầu ETL Raw to Staging với batch_size={batch_size}, only_unprocessed={only_unprocessed}")
    
    try:
        # Sử dụng hàm run_etl từ module raw_to_staging với các tham số
        result = run_etl(batch_size=batch_size, only_unprocessed=only_unprocessed, verbose=verbose)
        
        # Kiểm tra kết quả
        success = result.get('success', False) is not False  # Nếu không có key 'success' hoặc success=True
    
        if success:
            # Lấy stats từ kết quả
            stats = result.get('stats', {})
            
            print(f"✅ ETL raw_to_staging hoàn thành thành công!")
            print(f"Thống kê ETL:")
            print(f"- Tổng số bản ghi: {stats.get('total_records', 0):,}")
            print(f"- Số bản ghi đã xử lý: {stats.get('processed_records', 0):,}")
            print(f"- Số bản ghi thành công: {stats.get('success_count', 0):,}")
            print(f"- Số bản ghi lỗi: {stats.get('failure_count', 0):,}")
            print(f"- Tỷ lệ thành công: {stats.get('success_rate', 0):.2f}%")
            print(f"- Thời gian xử lý: {stats.get('duration_seconds', 0):.2f} giây")
            print(f"- Số batch đã xử lý: {stats.get('batch_count', 0)}")
            
            return f"ETL raw_to_staging completed successfully: {json.dumps(stats)}"
        else:
            error_msg = result.get('error', 'Unknown error')
            print(f"❌ ETL raw_to_staging thất bại: {error_msg}")
            raise Exception(f"ETL raw_to_staging failed: {error_msg}")
            
    except Exception as e:
        print(f"❌ ETL raw_to_staging thất bại với lỗi không xác định: {str(e)}")
        raise

def etl_staging_to_dwh():
    """
    Thực hiện ETL từ staging sang DWH (DuckDB)
    """
    # Import các module cần thiết
    from src.etl.etl_main import run_staging_to_dwh_etl
    from src.utils.config import DUCKDB_PATH
    import os
    from datetime import datetime, timedelta
    
    # Kiểm tra và tạo thư mục cho DuckDB nếu cần
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    
    # Chạy ETL với dữ liệu 7 ngày gần nhất
    last_etl_date = datetime.now() - timedelta(days=7)
    
    print(f"Bắt đầu ETL từ staging vào DWH từ {last_etl_date}...")
    etl_result = run_staging_to_dwh_etl(last_etl_date)
    
    # Kiểm tra kết quả ETL
    if etl_result.get("success", False):
        fact_count = etl_result.get("fact_count", 0)
        source_count = etl_result.get("source_count", 0)
        duration = etl_result.get("duration_seconds", 0)
        load_months = etl_result.get("load_months", [])
        partition_success = etl_result.get("partition_success", False)
        
        print(f"ETL hoàn thành thành công! ({duration:.2f} giây)")
        print(f"- Đã xử lý {source_count} bản ghi từ staging")
        print(f"- Tạo {fact_count} fact records")
        
        # Hiển thị thông tin partition
        if partition_success:
            print(f"- Partition: ✅ Thành công")
        else:
            print(f"- Partition: ❌ Thất bại")
        
        # Đánh dấu các bản ghi đã được xử lý trong staging
        if source_count > 0:
            # Thêm cột processed_to_dwh nếu chưa có
            check_column_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'jobinsight_staging' AND table_name = 'staging_jobs' AND column_name = 'processed_to_dwh';
            """
            result = get_dataframe(check_column_query)
            
            if result.empty:
                print("Adding processed_to_dwh column to staging_jobs table")
                alter_table_query = """
                ALTER TABLE jobinsight_staging.staging_jobs 
                ADD COLUMN processed_to_dwh BOOLEAN DEFAULT FALSE;
                """
                execute_query(alter_table_query, fetch=False)
            
            # Lấy job_ids từ staging và đánh dấu đã xử lý
            query = """
            SELECT job_id FROM jobinsight_staging.staging_jobs
            WHERE processed_to_dwh IS NULL OR processed_to_dwh = FALSE
            """
            
            staging_df = get_dataframe(query)
            
            if not staging_df.empty:
                with get_connection() as conn:
                    update_query = """
                    UPDATE jobinsight_staging.staging_jobs
                    SET processed_to_dwh = TRUE
                    WHERE job_id IN %s
                    """
                    cursor = conn.cursor()
                    cursor.execute(update_query, (tuple(staging_df['job_id'].tolist()),))
                    conn.commit()
                    print(f"Đã đánh dấu {len(staging_df)} bản ghi đã xử lý")
        
        return f"Processed {source_count} records to DWH, created {fact_count} fact records"
    else:
        error_msg = etl_result.get("message", "Unknown error")
        print(f"ETL thất bại: {error_msg}")
        return f"ETL failed: {error_msg}"

def parquet_cleanup(months_to_keep=12, **kwargs):
    """
    Xóa các file Parquet cũ hơn số tháng chỉ định
    
    Args:
        months_to_keep: Số tháng dữ liệu Parquet cần giữ lại, mặc định là 12 tháng
    """
    print(f"Bắt đầu dọn dẹp dữ liệu Parquet cũ hơn {months_to_keep} tháng")
    
    try:
        # Import các module cần thiết
        from src.etl.partitioning import PartitionManager
        from src.etl.etl_utils import get_duckdb_connection
        from src.utils.config import DUCKDB_PATH
        
        # Kết nối DuckDB
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            # Khởi tạo PartitionManager
            partition_manager = PartitionManager(duck_conn)
            
            # Gọi hàm cleanup_old_partitions
            stats = partition_manager.cleanup_old_partitions(months_to_keep)
        
            # Ghi log kết quả
            print(f"Kết quả dọn dẹp Parquet:")
            print(f"- Bảng đã xử lý: {stats['tables_processed']}")
            print(f"- Partition đã xóa: {stats['dirs_removed']}")
            print(f"- File đã xóa: {stats['files_removed']}")
            print(f"- Dung lượng giải phóng: {stats['bytes_freed'] / (1024*1024):.2f} MB")
            print(f"- Lỗi: {stats['errors']}")
            
            # Trả về kết quả
            return stats
    except Exception as e:
        print(f"Lỗi khi dọn dẹp Parquet: {str(e)}")
        raise e

with DAG(
    'jobinsight_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for JobInsight data processing',
    schedule_interval='40 17 * * *',  # Run at 11:00 AM Vietnam time (Asia/Ho_Chi_Minh)
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['jobinsight', 'etl'],
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    # Sensor để đợi DAG crawl_jobs hoàn thành
    wait_for_crawl = ExternalTaskSensor(
        task_id='wait_for_crawl',
        external_dag_id='crawl_topcv_jobs',
        external_task_id='end',
        mode='reschedule',
        timeout=3600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: dt,
        poke_interval=60,  # Check mỗi phút
        dag=dag
    )

    # Task ETL từ raw_jobs sang staging
    raw_to_staging_task = PythonOperator(
        task_id='raw_to_staging',
        python_callable=etl_raw_to_staging,
        op_kwargs={
            'batch_size': 5000,            # Xử lý 5000 bản ghi mỗi batch
            'only_unprocessed': True,      # Chỉ xử lý bản ghi chưa được xử lý
            'verbose': False               # Không hiển thị thông tin chi tiết
        }
    )

    # Task ETL từ staging sang DWH
    staging_to_dwh_task = PythonOperator(
        task_id='staging_to_dwh',
        python_callable=etl_staging_to_dwh,
    )

    # Task dọn dẹp Parquet cũ
    cleanup_parquet_task = PythonOperator(
        task_id='cleanup_parquet',
        python_callable=parquet_cleanup,
        op_kwargs={'months_to_keep': 12},  # mặc định giữ 12 tháng
    )

    end = DummyOperator(
        task_id='end',
    )

    # Định nghĩa luồng thực thi có dependency với crawl_jobs
    start >> wait_for_crawl >> raw_to_staging_task >> staging_to_dwh_task >> cleanup_parquet_task >> end 