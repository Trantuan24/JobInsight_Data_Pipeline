from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sys
import os

# Thêm đường dẫn để import các modules từ src
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "/..")

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

def etl_raw_to_staging():
    """
    Thực hiện ETL từ raw_jobs sang staging_jobs
    """
    from src.utils.db import execute_query
    
    # First, check and add the processed_to_staging column if it doesn't exist
    check_column_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'raw_jobs' AND column_name = 'processed_to_staging';
    """
    
    result = get_dataframe(check_column_query)
    
    # If the column doesn't exist, add it
    if result.empty:
        print("Adding processed_to_staging column to raw_jobs table")
        alter_table_query = """
        ALTER TABLE raw_jobs 
        ADD COLUMN processed_to_staging BOOLEAN DEFAULT FALSE;
        """
        execute_query(alter_table_query, fetch=False)
    
    # Query lấy dữ liệu từ raw_jobs - now safely use the column
    query = """
    SELECT * FROM raw_jobs
    WHERE processed_to_staging IS NULL OR processed_to_staging = FALSE
    """
    
    df = get_dataframe(query)
    
    if df.empty:
        print("Không có dữ liệu mới để xử lý")
        return
    
    # Xử lý và chuẩn hóa dữ liệu
    df['title_clean'] = df['title'].apply(clean_title)
    df['location_pairs'] = df['location_detail'].apply(extract_location_info)
    
    # Cập nhật dữ liệu vào staging
    with get_connection() as conn:
        # Insert vào staging
        cursor = conn.cursor()
        
        # Thực hiện insert staging logic
        print(f"Inserting {len(df)} records into staging_jobs...")
        
        # Tạo schema nếu chưa tồn tại
        cursor.execute("CREATE SCHEMA IF NOT EXISTS jobinsight_staging;")
        
        # Đảm bảo bảng staging_jobs đã tồn tại
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'jobinsight_staging' AND table_name = 'staging_jobs'
        );
        """
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("Creating staging_jobs table...")
            # Execute schema file to create the table if it doesn't exist
            from src.utils.db import execute_sql_file
            import os
            sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
            schema_file = os.path.join(sql_dir, "schema_staging.sql")
            execute_sql_file(schema_file)
        
        # Insert or update staging_jobs table using the processed data
        for _, row in df.iterrows():
            # Convert location_pairs to JSON string if it's a list
            location_pairs = row['location_pairs']
            if isinstance(location_pairs, list):
                import json
                location_pairs = json.dumps(location_pairs)
            
            # Convert skills to JSON string if it's not None
            skills = row['skills']
            if skills is not None and not isinstance(skills, str):
                import json
                skills = json.dumps(skills)
            
            # Convert raw_data to JSON string if it's not None
            raw_data = row['raw_data']
            if raw_data is not None and not isinstance(raw_data, str):
                import json
                raw_data = json.dumps(raw_data)
            
            # Prepare the UPSERT query
            upsert_query = """
            INSERT INTO jobinsight_staging.staging_jobs (
                job_id, title, title_clean, job_url, company_name, company_url, 
                salary, skills, location, location_detail, location_pairs,
                deadline, verified_employer, last_update, logo_url, 
                posted_time, crawled_at, raw_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) 
            DO UPDATE SET
                title = EXCLUDED.title,
                title_clean = EXCLUDED.title_clean,
                job_url = EXCLUDED.job_url,
                company_name = EXCLUDED.company_name,
                company_url = EXCLUDED.company_url,
                salary = EXCLUDED.salary,
                skills = EXCLUDED.skills,
                location = EXCLUDED.location,
                location_detail = EXCLUDED.location_detail,
                location_pairs = EXCLUDED.location_pairs,
                deadline = EXCLUDED.deadline,
                verified_employer = EXCLUDED.verified_employer,
                last_update = EXCLUDED.last_update,
                logo_url = EXCLUDED.logo_url,
                posted_time = EXCLUDED.posted_time,
                crawled_at = EXCLUDED.crawled_at,
                raw_data = EXCLUDED.raw_data
            """
            
            cursor.execute(upsert_query, (
                row['job_id'], 
                row['title'], 
                row['title_clean'], 
                row['job_url'] if 'job_url' in row else None, 
                row['company_name'] if 'company_name' in row else None, 
                row['company_url'] if 'company_url' in row else None, 
                row['salary'] if 'salary' in row else None, 
                skills, 
                row['location'] if 'location' in row else None, 
                row['location_detail'] if 'location_detail' in row else None, 
                location_pairs, 
                row['deadline'] if 'deadline' in row else None, 
                row['verified_employer'] if 'verified_employer' in row else None, 
                row['last_update'] if 'last_update' in row else None, 
                row['logo_url'] if 'logo_url' in row else None, 
                row['posted_time'] if 'posted_time' in row else None, 
                row['crawled_at'] if 'crawled_at' in row else None, 
                raw_data
            ))
        
        # Đánh dấu dữ liệu đã xử lý
        update_query = """
        UPDATE raw_jobs
        SET processed_to_staging = TRUE
        WHERE job_id IN %s
        """
        cursor.execute(update_query, (tuple(df['job_id'].tolist()),))
        conn.commit()
    
    return f"Processed {len(df)} records to staging"

def etl_staging_to_dwh():
    """
    Thực hiện ETL từ staging sang DWH
    """
    # Import trực tiếp từ file để tránh import thông qua __init__.py
    import sys
    import os
    
    # Thêm đường dẫn project vào sys.path
    project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, project_path)
    
    # Import trực tiếp từ file
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "staging_to_dwh", 
        os.path.join(project_path, "src", "etl", "staging_to_dwh.py")
    )
    staging_to_dwh_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(staging_to_dwh_module)
    
    # Lấy các hàm cần thiết từ module
    setup_duckdb_schema = staging_to_dwh_module.setup_duckdb_schema
    get_duckdb_connection = staging_to_dwh_module.get_duckdb_connection
    run_staging_to_dwh_etl = staging_to_dwh_module.run_staging_to_dwh_etl
    
    # Import DUCKDB_PATH từ config
    from src.utils.config import DUCKDB_PATH
    
    # Trước tiên, kiểm tra và tạo schema nếu chưa tồn tại
    check_schema_query = """
    SELECT EXISTS(
        SELECT 1 FROM information_schema.schemata 
        WHERE schema_name = 'jobinsight_staging'
    );
    """
    
    schema_result = get_dataframe(check_schema_query)
    schema_exists = schema_result.iloc[0, 0] if not schema_result.empty else False
    
    # Nếu schema chưa tồn tại, tạo mới
    if not schema_exists:
        print("Creating jobinsight_staging schema...")
        create_schema_query = """
        CREATE SCHEMA jobinsight_staging;
        """
        execute_query(create_schema_query, fetch=False)
    
    # Kiểm tra bảng staging_jobs đã tồn tại chưa
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'jobinsight_staging' AND table_name = 'staging_jobs'
    );
    """
    
    table_result = get_dataframe(check_table_query)
    table_exists = table_result.iloc[0, 0] if not table_result.empty else False
    
    # Nếu bảng chưa tồn tại, tạo mới từ file schema
    if not table_exists:
        print("Creating staging_jobs table...")
        from src.utils.db import execute_sql_file
        import os
        sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
        schema_file = os.path.join(sql_dir, "schema_staging.sql")
        execute_sql_file(schema_file)
    
    # Check and add processed_to_dwh column if needed
    check_column_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'jobinsight_staging' AND table_name = 'staging_jobs' AND column_name = 'processed_to_dwh';
    """
    
    result = get_dataframe(check_column_query)
    
    # If the column doesn't exist, add it
    if result.empty:
        print("Adding processed_to_dwh column to staging_jobs table")
        alter_table_query = """
        ALTER TABLE jobinsight_staging.staging_jobs 
        ADD COLUMN processed_to_dwh BOOLEAN DEFAULT FALSE;
        """
        execute_query(alter_table_query, fetch=False)
    
    # Kiểm tra file DuckDB
    if os.path.exists(DUCKDB_PATH):
        print(f"Sử dụng DuckDB hiện có: {DUCKDB_PATH}")
    else:
        print(f"Tạo DuckDB mới: {DUCKDB_PATH}")
        # Đảm bảo thư mục cha tồn tại
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    
    # Thiết lập schema và bảng cho DuckDB
    setup_result = setup_duckdb_schema()
    if not setup_result:
        print("Không thể thiết lập schema DuckDB!")
        return "Failed to setup DuckDB schema"
    
    # Chạy ETL từ staging vào DWH (DuckDB)
    from datetime import datetime, timedelta
    last_etl_date = datetime.now() - timedelta(days=7)  # Lấy dữ liệu 7 ngày gần nhất
    
    print(f"Bắt đầu ETL từ staging vào DWH từ {last_etl_date}...")
    etl_result = run_staging_to_dwh_etl(last_etl_date)
    
    # Kiểm tra kết quả ETL
    if etl_result.get("success", False):
        fact_count = etl_result.get("fact_count", 0)
        source_count = etl_result.get("source_count", 0)
        duration = etl_result.get("duration_seconds", 0)
        print(f"ETL hoàn thành thành công! Đã chuyển {source_count} bản ghi từ staging vào {fact_count} fact records trong {duration:.2f} giây")
        
        # Đánh dấu các bản ghi đã được xử lý trong staging
        if source_count > 0:
            # Query lấy dữ liệu từ staging (chỉ để lấy job_ids)
            query = """
            SELECT job_id FROM jobinsight_staging.staging_jobs
            WHERE processed_to_dwh IS NULL OR processed_to_dwh = FALSE
            """
            
            staging_df = get_dataframe(query)
            
            if not staging_df.empty:
                # Đánh dấu dữ liệu đã xử lý
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


with DAG(
    'jobinsight_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for JobInsight data processing',
    schedule_interval='30 11 * * *',  # Chạy 9:30 sáng mỗi ngày
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['jobinsight', 'etl'],
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    # Task ETL từ raw_jobs sang staging
    raw_to_staging_task = PythonOperator(
        task_id='raw_to_staging',
        python_callable=etl_raw_to_staging,
    )

    # Task ETL từ staging sang DWH
    staging_to_dwh_task = PythonOperator(
        task_id='staging_to_dwh',
        python_callable=etl_staging_to_dwh,
    )


    end = DummyOperator(
        task_id='end',
    )

    # Định nghĩa luồng thực thi
    start >> raw_to_staging_task >> staging_to_dwh_task >> end 