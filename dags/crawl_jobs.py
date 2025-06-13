from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

print("LOADING CRAWL_JOBS DAG!")
logging.info("LOADING CRAWL_JOBS DAG!")
import sys
import os

# Thêm đường dẫn để import các modules từ src
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "/..")

# Import các modules cần thiết
from src.crawler.crawler import crawl_jobs
from src.ingestion.ingest import ingest_dataframe

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crawl_topcv_jobs',
    default_args=default_args,
    description='Crawl job data from TopCV and ingest to database',
    schedule_interval='0 11 * * *',  # Chạy 9 giờ sáng mỗi ngày
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['jobinsight', 'crawler'],
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    # Đường dẫn tạm thời để lưu DataFrame
    temp_file_path = "/opt/airflow/data/temp_crawled_data.csv"
    
    # Task crawl dữ liệu từ TopCV và lưu vào file
    def crawl_and_save(**kwargs):
        # Crawl 5 trang từ BASE_URL, không sử dụng keywords nữa
        df = crawl_jobs(num_pages=5)
        if not df.empty:
            # Tạo thư mục nếu chưa tồn tại
            import os
            os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
            
            # Chuyển đổi các trường JSON về dạng string an toàn
            import json
            if 'skills' in df.columns:
                # Chuyển đổi skills từ cấu trúc dữ liệu thành JSON string đúng định dạng
                df['skills'] = df['skills'].apply(lambda x: json.dumps(x) if x is not None else None)
            
            # Lưu DataFrame vào file CSV
            df.to_csv(temp_file_path, index=False)
            return temp_file_path
        return None
    
    crawl_task = PythonOperator(
        task_id='crawl_topcv_data',
        python_callable=crawl_and_save,
    )
    
    # Task đọc file CSV và ingest vào database
    def load_and_ingest(**kwargs):
        import pandas as pd
        import os
        import json
        
        if os.path.exists(temp_file_path):
            df = pd.read_csv(temp_file_path)
            
            # Chuyển đổi job_id thành chuỗi
            if 'job_id' in df.columns:
                df['job_id'] = df['job_id'].astype(str)
                print(f"Đã chuyển đổi job_id sang kiểu chuỗi: {df['job_id'].head()}")
            
            # Chuyển đổi trường skills từ string JSON thành cấu trúc dữ liệu Python
            if 'skills' in df.columns:
                def parse_json_safely(value):
                    if pd.isna(value) or value is None or value == '':
                        return None
                    try:
                        return json.loads(value)
                    except:
                        return None
                
                df['skills'] = df['skills'].apply(parse_json_safely)
                print(f"Đã chuyển đổi skills sang JSON: {str(df['skills'].head())}")
            
            # Đảm bảo posted_time không bị null
            if 'posted_time' in df.columns:
                # Đếm số giá trị null
                null_count = df['posted_time'].isnull().sum()
                if null_count > 0:
                    print(f"Cảnh báo: {null_count} bản ghi có posted_time là NULL")
                    # Kiểm tra xem có cột last_update không để tính posted_time
                    if 'last_update' in df.columns:
                        from datetime import datetime
                        from src.crawler.data_extractor import parse_last_update
                        
                        # Chỉ điền posted_time cho các bản ghi NULL
                        for idx, row in df[df['posted_time'].isnull()].iterrows():
                            if pd.notna(row['last_update']):
                                try:
                                    seconds_ago = parse_last_update(row['last_update'])
                                    posted_time = datetime.now().timestamp() - seconds_ago
                                    df.at[idx, 'posted_time'] = datetime.fromtimestamp(posted_time).isoformat()
                                except:
                                    print(f"Không thể tính posted_time cho job_id: {row['job_id']}")
                
                # In thông tin về posted_time
                print(f"Kiểm tra posted_time sau xử lý: {df['posted_time'].head()}")
            
            ingest_dataframe(df)
            # Xóa file tạm sau khi ingest
            os.remove(temp_file_path)
            return f"Ingested {len(df)} records"
        return "No data to ingest"
        
    ingest_task = PythonOperator(
        task_id='ingest_job_data',
        python_callable=load_and_ingest,
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> crawl_task >> ingest_task >> end 