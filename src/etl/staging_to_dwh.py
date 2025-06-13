#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL module cho việc chuyển dữ liệu từ Staging sang Data Warehouse (DuckDB)
Fixed version với logic parsing location mới
"""
import pandas as pd
import logging
import json
import os
import sys
import duckdb
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# Thiết lập đường dẫn và logging
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Đảm bảo thư mục logs tồn tại
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, "etl.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from src.utils.logger import get_logger
from src.utils.db import get_connection, get_dataframe, execute_query
from src.utils.config import DUCKDB_PATH, DWH_STAGING_SCHEMA, STAGING_JOBS_TABLE

try:
    from src.processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range, parse_single_location_item, parse_job_location,
        check_dimension_changes, apply_scd_type2_updates, 
        generate_daily_fact_records, calculate_load_month
    )
except ImportError:
    from processing.data_prepare import (
        prepare_dim_job, prepare_dim_company, prepare_dim_location, 
        generate_date_range, parse_single_location_item, parse_job_location,
        check_dimension_changes, apply_scd_type2_updates, 
        generate_daily_fact_records, calculate_load_month
    )

# Đường dẫn đến thư mục SQL
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
if not os.path.exists(SQL_DIR):
    SQL_DIR = os.path.join(os.getcwd(), "sql")
    os.makedirs(SQL_DIR, exist_ok=True)

def get_duckdb_connection(duckdb_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """ Kết nối đến DuckDB """
    # Đảm bảo đường dẫn là tuyệt đối
    if not os.path.isabs(duckdb_path):
        duckdb_path = os.path.join(PROJECT_ROOT, duckdb_path)
    
    # Đảm bảo thư mục cha tồn tại
    parent_dir = os.path.dirname(duckdb_path)
    os.makedirs(parent_dir, exist_ok=True)
    
    logger.info(f"Kết nối DuckDB tại: {duckdb_path}")
    
    return duckdb.connect(duckdb_path)

def execute_sql_file_duckdb(sql_file_path, conn=None):
    """Thực thi file SQL trên DuckDB với connection truyền vào (hoặc tự tạo)"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        # Nếu không truyền conn thì tự tạo và tự đóng, còn truyền vào thì không đóng
        close_conn = False
        if conn is None:
            conn = get_duckdb_connection()
            close_conn = True
        conn.execute(sql_script)
        if close_conn:
            conn.close()
        logger.info(f"Đã thực thi file SQL: {sql_file_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thực thi file SQL {sql_file_path}: {str(e)}")
        return False

def setup_duckdb_schema():
    """Thiết lập schema và bảng cho DuckDB"""
    try:
        with get_duckdb_connection() as conn:
            # Tạo schema nếu chưa có (DuckDB sẽ không báo lỗi nếu đã tồn tại)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {DWH_STAGING_SCHEMA};")
            logger.info(f"Đã đảm bảo tồn tại schema: {DWH_STAGING_SCHEMA}")

            # Thực thi file schema_dwh.sql để tạo bảng và chỉ mục
            schema_dwh = os.path.join(SQL_DIR, "schema_dwh.sql")
            if os.path.exists(schema_dwh):
                if not execute_sql_file_duckdb(schema_dwh, conn=conn):
                    logger.error("Không thể thiết lập schema và bảng!")
                    return False
            else:
                logger.error(f"Không tìm thấy file schema: {schema_dwh}")
                return False

            # Kiểm tra xem các bảng đã được tạo thành công chưa
            tables_in_db = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
            for table in ['DimJob', 'DimCompany', 'DimLocation', 'DimDate', 'FactJobPostingDaily', 'FactJobLocationBridge']:
                if table in tables_in_db:
                    logger.info(f"✓ Bảng {table} đã được tạo thành công")
                else:
                    logger.warning(f"✗ Bảng {table} KHÔNG được tạo thành công")

        logger.info("Đã thiết lập schema và bảng database thành công cho DuckDB!")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thiết lập schema database với DuckDB: {str(e)}")
        return False
    

def get_staging_batch(last_etl_date: datetime) -> pd.DataFrame:
    """
    Lấy batch dữ liệu từ staging jobs kể từ lần ETL gần nhất
    
    Args:
        last_etl_date: Timestamp của lần ETL gần nhất
        
    Returns:
        DataFrame chứa bản ghi cần xử lý
    """
    try:
        logger.info(f"Truy vấn dữ liệu từ bảng {STAGING_JOBS_TABLE}")
        
        query = f"""
            SELECT *
            FROM {STAGING_JOBS_TABLE}
            WHERE crawled_at >= %s
            OR
            (crawled_at IS NOT NULL AND %s IS NULL)
        """
        
        # Kiểm tra xem bảng có tồn tại không
        table_exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{DWH_STAGING_SCHEMA}' 
                AND table_name = 'staging_jobs'
            );
        """
        
        # Thực hiện kiểm tra
        exists = execute_query(table_exists_query, fetch=True)
        
        if not exists or not exists[0].get('exists', False):
            logger.error(f"Bảng {STAGING_JOBS_TABLE} không tồn tại!")
            # Trả về DataFrame rỗng nếu bảng không tồn tại
            return pd.DataFrame()
        
        df = get_dataframe(query, params=(last_etl_date, last_etl_date))
        logger.info(f"Đã lấy {len(df)} bản ghi từ staging (từ {last_etl_date})")
        
        # Log các cột để debug
        if not df.empty:
            logger.info(f"Các cột có trong dữ liệu: {list(df.columns)}")
        
        return df
    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu từ staging: {e}", exc_info=True)
        # Trả về DataFrame rỗng trong trường hợp lỗi
        return pd.DataFrame()
    
def lookup_location_key(
    duck_conn: duckdb.DuckDBPyConnection,
    province: str = None,
    city: str = None,
    district: str = None
) -> Optional[int]:
    """
    Tìm location_sk từ bảng DimLocation dựa trên province, city, district
    
    Args:
        duck_conn: Kết nối DuckDB
        province: Tên tỉnh
        city: Tên thành phố
        district: Tên quận/huyện
    
    Returns:
        location_sk hoặc None nếu không tìm thấy
    """
    try:
        # Xây dựng query động dựa trên các tham số có giá trị
        conditions = ["is_current = TRUE"]
        params = []
        
        if province is not None:
            conditions.append("province = ?")
            params.append(province)
        else:
            conditions.append("province IS NULL")
            
        if city is not None:
            conditions.append("city = ?")
            params.append(city)
        else:
            conditions.append("city IS NULL")
            
        if district is not None:
            conditions.append("district = ?")
            params.append(district)
        else:
            conditions.append("district IS NULL")
        
        query = f"""
            SELECT location_sk
            FROM DimLocation
            WHERE {' AND '.join(conditions)}
            LIMIT 1
        """
        
        result = duck_conn.execute(query, params).fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Lỗi khi tìm location_sk: {e}")
        return None

def lookup_dimension_key(
    duck_conn: duckdb.DuckDBPyConnection,
    dim_table: str,
    key_column: str,
    key_value: Any,
    surrogate_key_col: str
) -> Optional[int]:
    """
    Tìm surrogate key từ bảng dimension
    
    Args:
        duck_conn: Kết nối DuckDB
        dim_table: Tên bảng dimension
        key_column: Tên cột dùng để tìm kiếm
        key_value: Giá trị cần tìm
        surrogate_key_col: Tên cột surrogate key
    
    Returns:
        Surrogate key hoặc None nếu không tìm thấy
    """
    try:
        query = f"""
            SELECT {surrogate_key_col}
            FROM {dim_table}
            WHERE {key_column} = ?
            AND is_current = TRUE
            LIMIT 1
        """
        
        result = duck_conn.execute(query, [key_value]).fetchone()
        if result:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Lỗi khi tìm khóa trong {dim_table}: {e}")
        return None

def batch_insert_records(duck_conn: duckdb.DuckDBPyConnection, table_name: str, records: List[Dict], batch_size: int = 1000, upsert_on_conflict: str = None):
    """
    Batch insert records để tối ưu performance
    
    Args:
        duck_conn: Kết nối DuckDB
        table_name: Tên bảng
        records: List các records cần insert
        batch_size: Kích thước batch
        upsert_on_conflict: Column(s) để UPSERT (e.g., "(fact_id, location_sk)")
    """
    if not records:
        return 0
        
    inserted_count = 0
    
    # Chia records thành các batch
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        try:
            # Xử lý đặc biệt cho FactJobLocationBridge với duplicate check
            if table_name == 'FactJobLocationBridge':
                for record in batch:
                    fact_id = record['fact_id']
                    location_sk = record['location_sk']
                    
                    # Kiểm tra xem bridge record đã tồn tại chưa
                    check_query = """
                        SELECT 1 FROM FactJobLocationBridge 
                        WHERE fact_id = ? AND location_sk = ?
                    """
                    exists = duck_conn.execute(check_query, [fact_id, location_sk]).fetchone()
                    
                    if not exists:
                        insert_query = """
                            INSERT INTO FactJobLocationBridge (fact_id, location_sk)
                            VALUES (?, ?)
                        """
                        duck_conn.execute(insert_query, [fact_id, location_sk])
                        inserted_count += 1
                        
            else:
                # Xử lý thông thường cho các bảng khác
                # Chuẩn bị dữ liệu batch
                df_batch = pd.DataFrame(batch)
                
                # Handle JSON columns
                for col in df_batch.columns:
                    df_batch[col] = df_batch[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )
                
                # Insert batch
                duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_batch")
                inserted_count += len(batch)
            
        except Exception as e:
            logger.warning(f"Lỗi khi batch insert vào {table_name}: {e}")
            # Fallback: insert từng record
            for record in batch:
                try:
                    columns = list(record.keys())
                    placeholders = ', '.join(['?'] * len(columns))
                    values = [record[col] for col in columns]
                    
                    # Handle JSON values
                    for j, val in enumerate(values):
                        if isinstance(val, (dict, list)):
                            values[j] = json.dumps(val)
                    
                    # Kiểm tra nếu có upsert_on_conflict
                    if upsert_on_conflict and table_name == 'FactJobLocationBridge':
                        # Skip nếu đã tồn tại
                        check_query = f"SELECT 1 FROM {table_name} WHERE fact_id = ? AND location_sk = ?"
                        exists = duck_conn.execute(check_query, [values[0], values[1]]).fetchone()
                        if exists:
                            continue
                    
                    query = f"""
                        INSERT INTO {table_name} ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    duck_conn.execute(query, values)
                    inserted_count += 1
                except Exception as e2:
                    logger.error(f"Lỗi khi insert single record vào {table_name}: {e2}")
    
    logger.info(f"Đã batch insert {inserted_count} records vào {table_name}")
    return inserted_count

def process_dimension_with_scd2(
    duck_conn: duckdb.DuckDBPyConnection,
    staging_records: pd.DataFrame,
    dim_table: str,
    prepare_function,
    natural_key: str,
    surrogate_key: str,
    compare_columns: List[str]
) -> Dict[str, int]:
    """
    Xử lý dimension table với SCD Type 2
    
    Args:
        duck_conn: Kết nối DuckDB
        staging_records: Dữ liệu staging
        dim_table: Tên bảng dimension
        prepare_function: Function chuẩn bị dữ liệu
        natural_key: Natural key column
        surrogate_key: Surrogate key column
        compare_columns: Columns để so sánh thay đổi
    
    Returns:
        Dict thống kê insert/update
    """
    logger.info(f"Xử lý {dim_table} với SCD Type 2")
    
    # Chuẩn bị dữ liệu
    prepared_data = prepare_function(staging_records)
    
    if prepared_data.empty:
        logger.warning(f"Không có dữ liệu để xử lý cho {dim_table}")
        return {'inserted': 0, 'updated': 0, 'unchanged': 0}
    
    # Kiểm tra thay đổi
    to_insert, to_update, unchanged = check_dimension_changes(
        duck_conn, prepared_data, dim_table, natural_key, compare_columns
    )
    
    stats = {
        'inserted': 0,
        'updated': 0,
        'unchanged': len(unchanged)
    }
    
    # Áp dụng updates (SCD Type 2)
    if to_update:
        apply_scd_type2_updates(duck_conn, dim_table, surrogate_key, to_update)
        stats['updated'] = len(to_update)
    
    # Insert records mới
    if not to_insert.empty:
        insert_records = []
        for _, record in to_insert.iterrows():
            record_dict = record.to_dict()
            # Loại bỏ surrogate key
            if surrogate_key in record_dict:
                del record_dict[surrogate_key]
            insert_records.append(record_dict)
        
        stats['inserted'] = batch_insert_records(duck_conn, dim_table, insert_records)
    
    logger.info(f"{dim_table} - Inserted: {stats['inserted']}, Updated: {stats['updated']}, Unchanged: {stats['unchanged']}")
    return stats

def generate_fact_records(
    duck_conn: duckdb.DuckDBPyConnection,
    staging_records: pd.DataFrame
) -> Tuple[List[Dict], List[Dict]]:
    """
    Tạo bản ghi fact và bridge từ dữ liệu staging
    
    Args:
        duck_conn: Kết nối DuckDB
        staging_records: Dữ liệu từ staging
    
    Returns:
        Tuple chứa (fact_records, bridge_records)
    """
    fact_records = []
    bridge_records = []
    
    for _, job in staging_records.iterrows():
        # Lookup dimension keys
        job_sk = lookup_dimension_key(
            duck_conn, 'DimJob', 'job_id', job.job_id, 'job_sk'
        )
        
        company_name = job.company_name_standardized if pd.notna(job.company_name_standardized) else job.company_name
        company_sk = lookup_dimension_key(
            duck_conn, 'DimCompany', 'company_name_standardized', company_name, 'company_sk'
        )
        
        # Check if required keys exist
        if not job_sk or not company_sk:
            logger.warning(f"Bỏ qua job_id={job.job_id}: Không tìm thấy dimension key (job_sk={job_sk}, company_sk={company_sk})")
            continue
        
        # Xử lý ngày
        due_date = pd.to_datetime(job.due_date) if pd.notna(job.due_date) else None
        posted_time = pd.to_datetime(job.posted_time) if pd.notna(job.posted_time) else None
        crawled_at = pd.to_datetime(job.crawled_at) if pd.notna(job.crawled_at) else datetime.now()
        
        # Tạo danh sách các ngày cần tạo fact records
        daily_dates = generate_daily_fact_records(posted_time, due_date)
        
        # Tính load_month
        load_month = calculate_load_month(crawled_at)
        
        # Tạo fact records cho từng ngày
        for date_id in daily_dates:
            try:
                # Kiểm tra xem fact record đã tồn tại chưa
                check_query = """
                    SELECT fact_id FROM FactJobPostingDaily 
                    WHERE job_sk = ? AND date_id = ?
                """
                existing_fact = duck_conn.execute(check_query, [job_sk, date_id]).fetchone()
                
                if existing_fact:
                    # Đã tồn tại, chỉ cập nhật một số field quan trọng
                    fact_id = existing_fact[0]
                    
                    update_query = """
                        UPDATE FactJobPostingDaily 
                        SET 
                            time_remaining = ?,
                            crawled_at = ?,
                            load_month = ?
                        WHERE fact_id = ?
                    """
                    
                    update_values = [
                        job.time_remaining if pd.notna(job.time_remaining) else None,
                        crawled_at,
                        load_month,
                        fact_id
                    ]
                    
                    try:
                        duck_conn.execute(update_query, update_values)
                        logger.debug(f"Updated existing fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                    except Exception as e:
                        logger.error(f"Lỗi khi update fact record cho job_id={job.job_id}, date={date_id}: {e}")
                        continue
                        
                else:
                    # Chưa tồn tại, tạo mới
                    fact_record = {
                        'job_sk': job_sk,
                        'company_sk': company_sk,
                        'date_id': date_id,
                        'salary_min': job.salary_min if pd.notna(job.salary_min) else None,
                        'salary_max': job.salary_max if pd.notna(job.salary_max) else None,
                        'salary_type': job.salary_type if pd.notna(job.salary_type) else None,
                        'due_date': due_date,
                        'time_remaining': job.time_remaining if pd.notna(job.time_remaining) else None,
                        'verified_employer': job.verified_employer if pd.notna(job.verified_employer) else False,
                        'posted_time': posted_time,
                        'crawled_at': crawled_at,
                        'load_month': load_month
                    }
                
                    # Insert fact record và lấy fact_id
                    columns = ', '.join([k for k, v in fact_record.items() if v is not None])
                    placeholders = ', '.join(['?'] * len([v for v in fact_record.values() if v is not None]))
                    values = [v for v in fact_record.values() if v is not None]
                    
                    insert_query = f"""
                        INSERT INTO FactJobPostingDaily ({columns})
                        VALUES ({placeholders})
                        RETURNING fact_id
                    """
                    
                    try:
                        result = duck_conn.execute(insert_query, values).fetchone()
                        if not result:
                            logger.warning(f"Không thể insert fact record cho job_id={job.job_id}, date={date_id}")
                            continue
                            
                        fact_id = result[0]
                        fact_records.append(fact_record)
                        logger.debug(f"Inserted new fact record: job_sk={job_sk}, date_id={date_id}, fact_id={fact_id}")
                    
                    except Exception as e:
                        logger.error(f"Lỗi khi insert fact record cho job_id={job.job_id}, date={date_id}: {e}")
                        continue
                    
                # Xử lý locations với cấu trúc mới (province, city, district)
                location_str = None
                
                # Ưu tiên sử dụng location_pairs nếu có
                if hasattr(job, 'location_pairs'):
                    try:
                        location_pairs_value = getattr(job, 'location_pairs')
                        if location_pairs_value is not None and str(location_pairs_value).lower() not in ['nan', 'none', '']:
                            location_str = str(location_pairs_value)
                    except:
                        pass
                        
                # Fallback về location nếu không có location_pairs
                if not location_str and hasattr(job, 'location'):
                    try:
                        location_value = getattr(job, 'location')
                        if location_value is not None and str(location_value).lower() not in ['nan', 'none', '']:
                            location_str = str(location_value)
                    except:
                        pass
                
                # Xóa bridge records cũ cho fact_id này (nếu update)
                if existing_fact:
                    duck_conn.execute("DELETE FROM FactJobLocationBridge WHERE fact_id = ?", [fact_id])
                
                if location_str:
                    # Parse location string thành các tuple (province, city, district)
                    logger.debug(f"Parsing location_str: {location_str}")
                    parsed_locations = parse_job_location(location_str)
                    logger.debug(f"Parsed locations: {parsed_locations}")
                    
                    location_sks_added = set()  # Tránh duplicate locations cho cùng 1 fact_id
                    
                    for province, city, district in parsed_locations:
                        location_sk = lookup_location_key(duck_conn, province, city, district)
                        
                        if location_sk and location_sk not in location_sks_added:
                            bridge_records.append({'fact_id': fact_id, 'location_sk': location_sk})
                            location_sks_added.add(location_sk)
                        elif not location_sk:
                            # Nếu không tìm thấy exact match, thử tìm Unknown
                            unknown_location_sk = lookup_location_key(duck_conn, None, 'Unknown', None)
                            if unknown_location_sk and unknown_location_sk not in location_sks_added:
                                bridge_records.append({'fact_id': fact_id, 'location_sk': unknown_location_sk})
                                location_sks_added.add(unknown_location_sk)
                            else:
                                logger.warning(f"Không tìm thấy location_sk cho job_id={job.job_id}, location=({province}, {city}, {district})")
                else:
                    # Không có location, sử dụng Unknown
                    unknown_location_sk = lookup_location_key(duck_conn, None, 'Unknown', None)
                    if unknown_location_sk:
                        bridge_records.append({'fact_id': fact_id, 'location_sk': unknown_location_sk})
                        
            except Exception as e:
                logger.error(f"Lỗi khi insert fact record cho job_id={job.job_id}, date={date_id}: {e}")
                continue
    
    return fact_records, bridge_records


def cleanup_duplicate_fact_records(duck_conn: duckdb.DuckDBPyConnection):
    """
    Dọn dẹp các duplicate records trong FactJobPostingDaily và FactJobLocationBridge
    """
    logger.info("Bắt đầu dọn dẹp duplicate fact records...")
    
    try:
        # 1. Backup bridge records trước khi xóa
        logger.info("Backup bridge records...")
        backup_bridge_query = """
            CREATE OR REPLACE TEMP TABLE bridge_backup AS
            SELECT DISTINCT fact_id, location_sk 
            FROM FactJobLocationBridge
        """
        duck_conn.execute(backup_bridge_query)
        
        # 2. Tìm và xóa duplicate fact records, giữ lại record có fact_id nhỏ nhất
        logger.info("Tìm duplicate fact records...")
        find_duplicates_query = """
            SELECT job_sk, date_id, COUNT(*) as count, MIN(fact_id) as keep_fact_id
            FROM FactJobPostingDaily
            GROUP BY job_sk, date_id
            HAVING COUNT(*) > 1
        """
        
        duplicates = duck_conn.execute(find_duplicates_query).fetchdf()
        
        if not duplicates.empty:
            logger.info(f"Tìm thấy {len(duplicates)} nhóm duplicate fact records")
            
            total_deleted = 0
            for _, dup in duplicates.iterrows():
                job_sk, date_id, count, keep_fact_id = dup['job_sk'], dup['date_id'], dup['count'], dup['keep_fact_id']
                
                # Lấy danh sách fact_id cần xóa (tất cả trừ keep_fact_id)
                get_delete_ids_query = """
                    SELECT fact_id FROM FactJobPostingDaily
                    WHERE job_sk = ? AND date_id = ? AND fact_id != ?
                """
                delete_ids = duck_conn.execute(get_delete_ids_query, [job_sk, date_id, keep_fact_id]).fetchall()
                
                if delete_ids:
                    fact_ids_to_delete = [row[0] for row in delete_ids]
                    placeholders = ','.join(['?'] * len(fact_ids_to_delete))
                    
                    # Xóa bridge records trước
                    delete_bridge_query = f"""
                        DELETE FROM FactJobLocationBridge 
                        WHERE fact_id IN ({placeholders})
                    """
                    duck_conn.execute(delete_bridge_query, fact_ids_to_delete)
                    
                    # Xóa fact records
                    delete_fact_query = f"""
                        DELETE FROM FactJobPostingDaily 
                        WHERE fact_id IN ({placeholders})
                    """
                    duck_conn.execute(delete_fact_query, fact_ids_to_delete)
                    
                    total_deleted += len(fact_ids_to_delete)
                    logger.debug(f"Đã xóa {len(fact_ids_to_delete)} duplicate records cho job_sk={job_sk}, date_id={date_id}")
            
            logger.info(f"Đã xóa tổng cộng {total_deleted} duplicate fact records")
            
            # 3. Restore bridge records cho các fact_id còn lại
            logger.info("Restore bridge records...")
            restore_bridge_query = """
                INSERT INTO FactJobLocationBridge (fact_id, location_sk)
                SELECT DISTINCT b.fact_id, b.location_sk
                FROM bridge_backup b
                JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM FactJobLocationBridge fb 
                    WHERE fb.fact_id = b.fact_id AND fb.location_sk = b.location_sk
                )
            """
            duck_conn.execute(restore_bridge_query)
            
        else:
            logger.info("Không tìm thấy duplicate fact records")
            
        # 4. Thống kê sau khi dọn dẹp
        stats_query = """
            SELECT 
                COUNT(*) as total_facts,
                (SELECT COUNT(*) FROM (SELECT DISTINCT job_sk, date_id FROM FactJobPostingDaily)) as unique_combinations
            FROM FactJobPostingDaily
        """
        stats = duck_conn.execute(stats_query).fetchone()
        logger.info(f"Sau dọn dẹp: {stats[0]} fact records, {stats[1]} unique combinations")
        
        if stats[0] != stats[1]:
            logger.warning(f"Vẫn còn {stats[0] - stats[1]} duplicate records!")
        else:
            logger.info("✅ Đã dọn dẹp thành công tất cả duplicate records")
            
    except Exception as e:
        logger.error(f"Lỗi khi dọn dẹp duplicate records: {e}")
        raise

def verify_etl_integrity(staging_count: int, fact_count: int, threshold: float = 0.9) -> bool:
    """
    Kiểm tra tính toàn vẹn của quá trình ETL Staging to DWH
    
    Args:
        staging_count: Số bản ghi staging đầu vào
        fact_count: Số bản ghi fact đã tạo
        threshold: Ngưỡng chấp nhận (% dữ liệu được xử lý thành công)
        
    Returns:
        bool: True nếu tỷ lệ dữ liệu chuyển đổi đạt threshold
    """
    if staging_count == 0:
        logger.warning("Không có dữ liệu nguồn để xử lý")
        return True
    
    # Mỗi staging record có thể tạo ra nhiều fact record (mỗi ngày một record)
    # Nên kiểm tra xem có fact records được tạo không, không so sánh số lượng
    if fact_count == 0:
        logger.error("Không có fact record nào được tạo từ staging data!")
        return False
    
    logger.info(f"Đã tạo {fact_count} fact records từ {staging_count} staging records")
    return True

def run_staging_to_dwh_etl(last_etl_date: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Thực hiện quy trình ETL chuyển dữ liệu từ Staging sang Data Warehouse
    
    Args:
        last_etl_date: Timestamp của lần ETL gần nhất, mặc định là 7 ngày trước
        
    Returns:
        Dict[str, Any]: Kết quả thống kê ETL
    """
    start_time = datetime.now()
    
    try:
        # Thiết lập ngày ETL gần nhất nếu không có
        if last_etl_date is None:
            last_etl_date = datetime.now() - timedelta(days=7)
        
        logger.info("="*60)
        logger.info(f"🚀 BẮT ĐẦU ETL STAGING TO DWH - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"🕒 Lấy dữ liệu từ: {last_etl_date}")
        logger.info("="*60)
        
        # 1. Lấy dữ liệu từ staging
        staging_batch = get_staging_batch(last_etl_date)
        if staging_batch.empty:
            logger.info("Không có bản ghi nào để xử lý từ staging")
            return {
                "success": True,
                "message": "Không có dữ liệu để xử lý",
                "source_count": 0,
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
            
        logger.info(f"Đã lấy {len(staging_batch)} bản ghi từ staging")
        
        # 2. Kiểm tra file DuckDB
        if os.path.exists(DUCKDB_PATH):
            logger.info(f"📁 Sử dụng DuckDB hiện có: {DUCKDB_PATH}")
        else:
            logger.info(f"🆕 Tạo DuckDB mới: {DUCKDB_PATH}")
        
        # 3. Thiết lập schema và bảng (giữ nguyên dữ liệu cũ)
        if not setup_duckdb_schema():
            return {
                "success": False,
                "message": "Không thể thiết lập schema DuckDB",
                "source_count": len(staging_batch),
                "fact_count": 0,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
        
        # 4. Kết nối DuckDB và thực hiện ETL với SCD Type 2
        with get_duckdb_connection(DUCKDB_PATH) as duck_conn:
            # Dọn dẹp duplicate records hiện có (chạy 1 lần)
            logger.info("🧹 Dọn dẹp duplicate records hiện có...")
            cleanup_duplicate_fact_records(duck_conn)
            
            # 5. Xử lý và insert dữ liệu với SCD Type 2
            dim_stats = {}
            
            # 5.1 DimJob với SCD Type 2
            dim_stats['DimJob'] = process_dimension_with_scd2(
                duck_conn, staging_batch, 'DimJob', prepare_dim_job,
                'job_id', 'job_sk', ['title_clean', 'skills', 'job_url']
            )
        
            # 5.2. DimCompany với SCD Type 2
            dim_stats['DimCompany'] = process_dimension_with_scd2(
                duck_conn, staging_batch, 'DimCompany', prepare_dim_company,
                'company_name_standardized', 'company_sk', ['company_url', 'verified_employer']
            )
        
            # 5.3. DimLocation - xử lý đặc biệt vì composite key
            logger.info("Xử lý DimLocation với composite key")
            dim_location_df = prepare_dim_location(staging_batch)
            if not dim_location_df.empty:
                location_records = []
                for _, location in dim_location_df.iterrows():
                    location_dict = location.to_dict()
                    if 'location_sk' in location_dict:
                        del location_dict['location_sk']
                    location_records.append(location_dict)
                
                dim_stats['DimLocation'] = {
                    'inserted': batch_insert_records(duck_conn, 'DimLocation', location_records),
                    'updated': 0,
                    'unchanged': 0
                }
            else:
                dim_stats['DimLocation'] = {'inserted': 0, 'updated': 0, 'unchanged': 0}
        
            # 5.4. Đảm bảo bảng DimDate có đầy đủ các ngày cần thiết
            logger.info("Xử lý DimDate")
            start_date = (datetime.now() - timedelta(days=60)).date()
            end_date = (datetime.now() + timedelta(days=240)).date()
            
            date_df = generate_date_range(start_date, end_date)
            
            # Filter out existing dates
            new_date_records = []
            for _, date_record in date_df.iterrows():
                date_dict = date_record.to_dict()
                exists = duck_conn.execute(f"SELECT 1 FROM DimDate WHERE date_id = ?", [date_dict['date_id']]).fetchone()
                if not exists:
                    new_date_records.append(date_dict)
            
            dim_stats['DimDate'] = {
                'inserted': batch_insert_records(duck_conn, 'DimDate', new_date_records),
                'updated': 0,
                'unchanged': len(date_df) - len(new_date_records)
            }
        
            # 5.5. Insert dữ liệu vào FactJobPostingDaily và FactJobLocationBridge
            logger.info("Xử lý FactJobPostingDaily và FactJobLocationBridge")
            fact_records, bridge_records = generate_fact_records(duck_conn, staging_batch)
            
            # Kiểm tra tính toàn vẹn của dữ liệu
            if not verify_etl_integrity(len(staging_batch), len(fact_records)):
                logger.warning("⚠️ Phát hiện vấn đề về tính toàn vẹn dữ liệu trong quá trình ETL!")
                # Vẫn tiếp tục nhưng đã cảnh báo
            
            logger.info(f"Đã insert {len(fact_records)} bản ghi vào FactJobPostingDaily")
            logger.info(f"Chuẩn bị insert {len(bridge_records)} bản ghi vào FactJobLocationBridge")
        
            # Batch insert bridge records vào FactJobLocationBridge
            bridge_inserted = batch_insert_records(duck_conn, 'FactJobLocationBridge', bridge_records)
            logger.info(f"Đã batch insert {bridge_inserted} bản ghi vào FactJobLocationBridge")
        
            # 6. Tổng kết ETL
            logger.info("="*60)
            logger.info("📊 TỔNG KẾT ETL STAGING TO DWH")
            logger.info("="*60)
            
            total_inserted = sum(stats.get('inserted', 0) for stats in dim_stats.values())
            total_updated = sum(stats.get('updated', 0) for stats in dim_stats.values())
            total_unchanged = sum(stats.get('unchanged', 0) for stats in dim_stats.values())
            
            for table, stats in dim_stats.items():
                logger.info(f"{table:15} - Insert: {stats['inserted']:5}, Update: {stats['updated']:5}, Unchanged: {stats['unchanged']:5}")
            
            logger.info(f"{'FACTS':15} - FactJobPostingDaily: {len(fact_records)} records")
            logger.info(f"{'BRIDGE':15} - FactJobLocationBridge: {bridge_inserted} records")
            logger.info("-"*60)
            logger.info(f"TỔNG DIM        - Insert: {total_inserted:5}, Update: {total_updated:5}, Unchanged: {total_unchanged:5}")
            logger.info(f"TỔNG FACT/BRIDGE- Records: {len(fact_records) + bridge_inserted}")
            
            # Log load_month stats
            load_months = set()
            if fact_records:
                load_months = set(record.get('load_month') for record in fact_records)
                logger.info(f"Partition load_month: {', '.join(sorted(load_months))}")
            
            # 7. Validation và Data Quality Check
            logger.info("🔍 Bắt đầu validation ETL...")
            validation_success = True
            validation_message = ""
            
            try:
                from src.utils.etl_validator import generate_etl_report, log_validation_results
                validation_results = generate_etl_report(duck_conn)
                log_validation_results(validation_results)
                
                # Kiểm tra các vấn đề nghiêm trọng
                if validation_results.get('issues', {}).get('critical', 0) > 0:
                    validation_success = False
                    validation_message = f"Phát hiện {validation_results['issues']['critical']} vấn đề nghiêm trọng trong validation"
                    logger.error(validation_message)
            except ImportError:
                logger.warning("Không thể import etl_validator - bỏ qua validation")
            except Exception as e:
                validation_success = False
                validation_message = f"Lỗi khi thực hiện validation: {str(e)}"
                logger.error(validation_message)
        
        # Tính thời gian chạy
        duration = (datetime.now() - start_time).total_seconds()
        
        # Tổng kết
        logger.info("="*60)
        logger.info(f"✅ ETL HOÀN THÀNH TRONG {duration:.2f} GIÂY!")
        logger.info("="*60)
        
        # Thống kê kết quả ETL
        etl_stats = {
            "success": True,
            "source_count": len(staging_batch),
            "fact_count": len(fact_records),
            "bridge_count": bridge_inserted,
            "dim_stats": dim_stats,
            "total_dim_inserted": total_inserted,
            "total_dim_updated": total_updated,
            "load_months": list(load_months),
            "duration_seconds": duration,
            "validation_success": validation_success,
            "validation_message": validation_message
        }
        
        return etl_stats
    
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = f"Lỗi trong quá trình ETL: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "success": False,
            "message": error_msg,
            "duration_seconds": duration
        }

if __name__ == "__main__":
    # Chạy ETL với dữ liệu từ 7 ngày trước
    etl_result = run_staging_to_dwh_etl()
    
    # Kiểm tra kết quả
    if etl_result.get("success", False):
        logger.info("✅ ETL HOÀN THÀNH THÀNH CÔNG!")
        sys.exit(0)
    else:
        logger.error(f"❌ ETL THẤT BẠI: {etl_result.get('message', 'Unknown error')}")
        sys.exit(1)
