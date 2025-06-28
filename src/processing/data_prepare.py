import pandas as pd
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import os
import sys

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

def prepare_dim_job(staging_records: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn bị dữ liệu cho bảng DimJob
    
    Args:
        staging_records: Dữ liệu từ staging
        
    Returns:
        DataFrame cho DimJob
    """
    logger.info("Chuẩn bị dữ liệu cho DimJob")
    # Đảm bảo chỉ lấy các cột cần thiết và chuyển đổi định dạng nếu cần
    dim_job_df = staging_records.loc[:, ['job_id', 'title_clean', 'job_url', 'skills', 'last_update', 'logo_url']]
    
    # Xử lý các giá trị null
    dim_job_df['title_clean'] = dim_job_df['title_clean'].fillna('Unknown Title')
    
    # Xử lý trường skills - đảm bảo là JSON
    dim_job_df['skills'] = dim_job_df['skills'].apply(
        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else 
                  (x if isinstance(x, str) else json.dumps([]))
    )
    
    # Thêm các cột cần thiết cho SCD Type 2
    today = datetime.now().date()
    dim_job_df['effective_date'] = today
    dim_job_df['expiry_date'] = None
    dim_job_df['is_current'] = True
    
    # Thêm cột job_sk trống để đủ 10 cột theo schema
    # Schema: job_sk, job_id, title_clean, job_url, skills, last_update, logo_url, effective_date, expiry_date, is_current
    dim_job_df['job_sk'] = None
    
    # Sắp xếp lại các cột theo đúng thứ tự trong schema
    dim_job_df = dim_job_df[['job_sk', 'job_id', 'title_clean', 'job_url', 'skills', 'last_update', 'logo_url', 'effective_date', 'expiry_date', 'is_current']]
    
    logger.info(f"Đã chuẩn bị {len(dim_job_df)} bản ghi DimJob")
    return dim_job_df

def prepare_dim_company(staging_records: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn bị dữ liệu cho bảng DimCompany
    
    Args:
        staging_records: Dữ liệu từ staging
        
    Returns:
        DataFrame cho DimCompany
    """
    logger.info("Chuẩn bị dữ liệu cho DimCompany")
    # Lấy các cột cần thiết
    company_df = staging_records.loc[:, ['company_name', 'company_name_standardized', 'company_url', 'verified_employer']]
    
    # Sử dụng company_name nếu không có company_name_standardized
    company_df.loc[pd.isna(company_df['company_name_standardized']), 'company_name_standardized'] = (
        company_df.loc[pd.isna(company_df['company_name_standardized']), 'company_name']
    )
    
    # Thêm các cột cần thiết cho SCD Type 2
    today = datetime.now().date()
    company_df['effective_date'] = today
    company_df['expiry_date'] = None
    company_df['is_current'] = True
    
    # Loại bỏ duplicate
    company_df = company_df.drop(columns=['company_name']).drop_duplicates(subset=['company_name_standardized'])
    
    # Thêm cột company_sk trống để đủ 7 cột theo schema
    # Schema: company_sk, company_name_standardized, company_url, verified_employer, effective_date, expiry_date, is_current
    company_df['company_sk'] = None
    
    # Sắp xếp lại các cột theo đúng thứ tự trong schema
    company_df = company_df[['company_sk', 'company_name_standardized', 'company_url', 'verified_employer', 'effective_date', 'expiry_date', 'is_current']]
    
    logger.info(f"Đã chuẩn bị {len(company_df)} bản ghi DimCompany")
    return company_df

def check_dimension_changes(
    duck_conn,
    new_records: pd.DataFrame,
    dim_table: str,
    natural_key: str,
    compare_columns: List[str]
) -> Tuple[pd.DataFrame, List[Dict], List[Dict]]:
    """
    Kiểm tra thay đổi trong dimension table (SCD Type 2)
    
    Args:
        duck_conn: Kết nối DuckDB
        new_records: Dữ liệu mới cần kiểm tra
        dim_table: Tên bảng dimension
        natural_key: Cột natural key để so sánh
        compare_columns: Các cột cần so sánh để detect changes
    
    Returns:
        Tuple chứa (records_to_insert, records_to_update, unchanged_records)
    """
    to_insert = []
    to_update = []
    unchanged = []
    
    try:
        # Xử lý từng bản ghi mới
        for _, new_record in new_records.iterrows():
            key_value = new_record[natural_key]
            
            # Tìm bản ghi hiện tại trong dimension
            query = f"""
                SELECT * FROM {dim_table}
                WHERE {natural_key} = ? AND is_current = TRUE
            """
            
            existing = duck_conn.execute(query, [key_value]).fetchdf()
            
            if existing.empty:
                # Bản ghi mới hoàn toàn
                to_insert.append(new_record.to_dict())
            else:
                # So sánh các trường để xem có thay đổi không
                existing_record = existing.iloc[0]
                has_changes = False
                
                for col in compare_columns:
                    if col in new_record and col in existing_record:
                        if isinstance(new_record[col], (list, dict)):
                            new_val_str = json.dumps(new_record[col])
                        else:
                            new_val_str = str(new_record[col]) if pd.notna(new_record[col]) else None
                            
                        if isinstance(existing_record[col], (list, dict)):
                            old_val_str = json.dumps(existing_record[col])
                        else:
                            old_val_str = str(existing_record[col]) if pd.notna(existing_record[col]) else None
                        
                        if new_val_str != old_val_str:
                            has_changes = True
                            break
                
                if has_changes:
                    # Cần update: đóng record cũ và tạo record mới
                    to_update.append({
                        'old': existing_record.to_dict(),
                        'new': new_record.to_dict()
                    })
                else:
                    # Không có thay đổi
                    unchanged.append(new_record.to_dict())
                    
        # Chuyển về DataFrames
        to_insert_df = pd.DataFrame(to_insert) if to_insert else pd.DataFrame()
        
        logger.info(f"{dim_table}: {len(to_insert)} to insert, {len(to_update)} to update, {len(unchanged)} unchanged")
        
        return to_insert_df, to_update, unchanged
        
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra thay đổi dimension {dim_table}: {e}")
        # Fallback: coi tất cả là insert mới
        to_insert_df = new_records.copy()
        return to_insert_df, [], []

def apply_scd_type2_updates(
    duck_conn,
    dim_table: str, 
    surrogate_key: str,
    updates: List[Dict]
) -> int:
    """
    Áp dụng SCD Type 2 updates cho dimension table:
    1. Đóng bản ghi cũ (cập nhật expiry_date, is_current)
    2. Tạo bản ghi mới dựa trên dữ liệu mới
    
    Args:
        duck_conn: Kết nối DuckDB
        dim_table: Tên bảng dimension
        surrogate_key: Tên cột surrogate key
        updates: List các dict {'old': old_dict, 'new': new_dict}
    
    Returns:
        Số lượng cập nhật thành công
    """
    today = datetime.now().date()
    success_count = 0
    
    for update_pair in updates:
        try:
            old_record = update_pair['old']
            new_record = update_pair['new']
            
            # 1. Đóng bản ghi cũ
            update_query = f"""
                UPDATE {dim_table}
                SET expiry_date = ?, is_current = FALSE
                WHERE {surrogate_key} = ?
            """
            duck_conn.execute(update_query, [today, old_record[surrogate_key]])
            
            # 2. Chuẩn bị record mới
            # - Loại bỏ surrogate key để DBMS tự tạo
            if surrogate_key in new_record:
                del new_record[surrogate_key]
                
            # - Thêm thông tin SCD2
            new_record['effective_date'] = today
            new_record['expiry_date'] = None
            new_record['is_current'] = True
            
            # Xử lý JSON nếu cần
            for key, value in new_record.items():
                if isinstance(value, (list, dict)):
                    new_record[key] = json.dumps(value)
            
            # 3. Insert bản ghi mới
            columns = list(new_record.keys())
            placeholders = ', '.join(['?'] * len(columns))
            values = [new_record[col] for col in columns]
            
            insert_query = f"""
                INSERT INTO {dim_table} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            duck_conn.execute(insert_query, values)
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"Lỗi khi thực hiện SCD2 update: {str(e)}")
    
    logger.info(f"Đã thực hiện {success_count}/{len(updates)} SCD2 updates cho {dim_table}")
    return success_count

def generate_daily_fact_records(
    posted_date: Optional[datetime], 
    due_date: Optional[datetime],
    current_date: datetime = None
) -> List[datetime]:
    """
    Tạo danh sách các ngày cho fact records từ posted_date đến due_date
    
    Args:
        posted_date: Ngày đăng job
        due_date: Ngày hết hạn job
        current_date: Ngày hiện tại (default: hôm nay)
    
    Returns:
        List các ngày cần tạo fact records
    """
    if current_date is None:
        current_date = datetime.now()
    
    dates = []
    
    # Xác định start_date và end_date
    if posted_date is not None:
        start_date = posted_date.date() if hasattr(posted_date, 'date') else posted_date
    else:
        start_date = current_date.date()
    
    if due_date is not None:
        end_date = due_date.date() if hasattr(due_date, 'date') else due_date
    else:
        # Nếu không có due_date, giả sử job có hiệu lực 30 ngày
        end_date = start_date + timedelta(days=30)
    
    # Đảm bảo không tạo fact cho quá khứ xa hoặc tương lai xa
    min_date = (current_date - timedelta(days=90)).date()
    max_date = (current_date + timedelta(days=180)).date()
    
    start_date = max(start_date, min_date)
    end_date = min(end_date, max_date)
    
    # Tạo list các ngày
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    
    return dates

def calculate_load_month(date_value: Any) -> str:
    """
    Tính toán load_month từ date value để dùng làm partition key
    
    Args:
        date_value: Giá trị date (có thể là datetime, date, hoặc string)
    
    Returns:
        String dạng YYYY-MM
    """
    try:
        if date_value is None:
            date_value = datetime.now()
        elif isinstance(date_value, str):
            date_value = pd.to_datetime(date_value)
        
        return date_value.strftime('%Y-%m')
    except Exception as e:
        logger.warning(f"Lỗi khi tính load_month: {e}, sử dụng tháng hiện tại")
        return datetime.now().strftime('%Y-%m')

def generate_date_range(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Tạo range các ngày cho bảng DimDate
    
    Args:
        start_date: Ngày bắt đầu
        end_date: Ngày kết thúc
        
    Returns:
        DataFrame cho DimDate
    """
    date_range = pd.date_range(start=start_date, end=end_date)
    
    date_records = []
    for date in date_range:
        date_records.append({
            'date_id': date.date(),
            'day': date.day,
            'month': date.month,
            'quarter': (date.month - 1) // 3 + 1,
            'year': date.year,
            'weekday': date.day_name()
        })
    
    return pd.DataFrame(date_records)

def prepare_dim_location(staging_records: pd.DataFrame) -> pd.DataFrame:
    """
    Chuẩn bị dữ liệu cho bảng DimLocation với xử lý province, city, district
    
    Logic xử lý:
    - Dựa trên location_pairs và location
    - Mỗi record là một (province, city, district) duy nhất
    - Bridge table sau đó sẽ map nhiều location cho 1 job
    
    Args:
        staging_records: Dữ liệu từ staging
        
    Returns:
        DataFrame cho DimLocation với unique (province, city, district) combinations
    """
    logger.info("Chuẩn bị dữ liệu cho DimLocation")
    locations = []
    
    # Xử lý từng bản ghi staging
    for _, record in staging_records.iterrows():
        location_pairs_list = extract_location_pairs_list(record)
        
        if not location_pairs_list:
            # Không có dữ liệu location nào
            continue
        
        # Xử lý từng location pair
        for pair_str in location_pairs_list:
            province, city, districts = parse_location_pair(pair_str)
            
            if not city:
                # Không có city, bỏ qua
                continue
            
            if districts:
                # Có districts, tạo một record cho mỗi district
                for district in districts:
                    locations.append({
                        'province': province,
                        'city': city,
                        'district': district
                    })
            else:
                # Không có district, tạo record với district = None
                locations.append({
                    'province': province,
                    'city': city,
                    'district': None
                })
    
    # Tạo DataFrame
    if not locations:
        # Nếu không có dữ liệu nào, tạo record Unknown
        locations.append({
            'province': None,
            'city': 'Unknown',
            'district': None
        })
    
    location_df = pd.DataFrame(locations)
    
    # Thêm các cột cần thiết cho SCD Type 2
    today = datetime.now().date()
    location_df['effective_date'] = today
    location_df['expiry_date'] = None
    location_df['is_current'] = True
    
    # Loại bỏ duplicate dựa trên (province, city, district)
    location_df = location_df.drop_duplicates(subset=['province', 'city', 'district'], keep='first')
    
    # Thêm cột location_sk trống để đủ 7 cột theo schema
    # Schema: location_sk, province, city, district, effective_date, expiry_date, is_current
    location_df['location_sk'] = None
    
    # Sắp xếp lại các cột theo đúng thứ tự trong schema
    location_df = location_df[['location_sk', 'province', 'city', 'district', 'effective_date', 'expiry_date', 'is_current']]
    
    logger.info(f"Đã chuẩn bị {len(location_df)} bản ghi DimLocation unique")
    
    # Log thống kê để debug
    logger.info("Thống kê DimLocation:")
    logger.info(f"  - Có province: {location_df['province'].notna().sum()}")
    logger.info(f"  - Có city: {location_df['city'].notna().sum()}")
    logger.info(f"  - Có district: {location_df['district'].notna().sum()}")
    
    return location_df

def parse_location_pair(pair_str):
    """
    Parse location pair string và trả về (province, city, districts_list)
    
    Args:
        pair_str: String dạng "value1:value2" hoặc "value"
        
    Returns:
        tuple: (province, city, districts_list)
    """
    province = None
    city = None
    districts = []
    
    if not isinstance(pair_str, str) or not pair_str.strip():
        return province, city, districts
        
    pair_str = pair_str.strip()
    
    if ":" in pair_str:
        parts = pair_str.split(":", 1)
        if len(parts) == 2:
            part1, part2 = parts[0].strip(), parts[1].strip()
            
            # Kiểm tra xem có phải "province:city" không (có "TP" trong part2)
            if "TP" in part2.upper():
                # Dạng "Tỉnh ABC:TP XYZ"
                province = part1
                city = part2
                districts = []  # Không có district
            else:
                # Dạng "City:District1, District2, ..."
                city = part1
                # Tách districts bằng dấu phẩy
                if "," in part2:
                    districts = [d.strip() for d in part2.split(",") if d.strip()]
                else:
                    districts = [part2] if part2 else []
    else:
        # Chỉ có city riêng lẻ
        city = pair_str
        districts = []
    
    return province, city, districts

def extract_location_pairs_list(record):
    """
    Trích xuất danh sách location pairs từ record
    
    Args:
        record: Dict hoặc Series chứa dữ liệu 
    
    Returns:
        List các location pairs
    """
    location_pairs_list = []
    
    # Xử lý trường location_pairs nếu có
    if 'location_pairs' in record:
        location_pairs_value = record['location_pairs']
        
        # Kiểm tra null an toàn
        if pd.notna(location_pairs_value):
            if isinstance(location_pairs_value, list):
                # Đã là list
                location_pairs_list = location_pairs_value
            elif isinstance(location_pairs_value, str):
                try:
                    # Thử parse JSON
                    parsed = json.loads(location_pairs_value)
                    if isinstance(parsed, list):
                        location_pairs_list = parsed
                    else:
                        location_pairs_list = [str(parsed)]
                except json.JSONDecodeError:
                    # Không phải JSON, coi như string thường
                    location_pairs_list = [location_pairs_value]
            else:
                # Các kiểu khác, convert thành string
                location_pairs_list = [str(location_pairs_value)]
    
    # Nếu không có location_pairs hoặc rỗng, dùng trường location
    if not location_pairs_list and 'location' in record:
        location_value = record['location']
        
        # Kiểm tra null
        if pd.notna(location_value):
            location_value = str(location_value).strip()
            
            # Kiểm tra định dạng "City │ District1, District2"
            if "│" in location_value:
                location_parts = location_value.split("│", 1)
                if len(location_parts) == 2:
                    city = location_parts[0].strip()
                    districts_str = location_parts[1].strip()
                    # Tạo các cặp "city:district"
                    if "," in districts_str:
                        for district in districts_str.split(","):
                            if district.strip():
                                location_pairs_list.append(f"{city}:{district.strip()}")
                    else:
                        location_pairs_list.append(f"{city}:{districts_str}")
                else:
                    location_pairs_list = [location_value]
            else:
                # Chỉ có city
                location_pairs_list = [location_value]
    
    return location_pairs_list

def parse_job_location(location_str: str) -> List[Tuple[str, str, str]]:
    """
    Parse location string từ job thành list các tuple (province, city, district)
    
    Args:
        location_str: String location từ job data
        
    Returns:
        List các tuple (province, city, district)
    """
    locations = []
    
    if not isinstance(location_str, str) or not location_str.strip():
        return [(None, 'Unknown', None)]
    
    # Parse JSON list nếu có
    location_items = []
    try:
        # Thử parse JSON
        parsed_list = json.loads(location_str)
        if isinstance(parsed_list, list):
            location_items = [str(item).strip() for item in parsed_list if str(item).strip()]
        else:
            location_items = [location_str.strip()]
    except (json.JSONDecodeError, TypeError):
        # Nếu không parse được JSON, thử parse format ['item1', 'item2']
        try:
            if location_str.strip().startswith('[') and location_str.strip().endswith(']'):
                import ast
                parsed_list = ast.literal_eval(location_str)
                if isinstance(parsed_list, list):
                    location_items = [str(item).strip() for item in parsed_list if str(item).strip()]
                else:
                    location_items = [location_str.strip()]
            else:
                location_items = [location_str.strip()]
        except (ValueError, SyntaxError):
            location_items = [location_str.strip()]
    
    # Xử lý từng location item RIÊNG BIỆT
    for item in location_items:
        if not item or not item.strip():
            continue
        item = item.strip()
        
        # Parse 1 item
        parsed_locations = parse_single_location_item(item)
        locations.extend(parsed_locations)
    
    return locations if locations else [(None, 'Unknown', None)]

def parse_single_location_item(item: str) -> List[Tuple[str, str, str]]:
    """
    Parse một location item riêng lẻ thành các tuple (province, city, district)
    
    Xử lý các định dạng:
    - "Hà Nội: Thanh Xuân, Đống Đa" -> [(None, 'Hà Nội', 'Thanh Xuân'), (None, 'Hà Nội', 'Đống Đa')]
    - "Hà Nội: Cầu Giấy" -> [(None, 'Hà Nội', 'Cầu Giấy')]
    - "Hồ Chí Minh" -> [(None, 'Hồ Chí Minh', None)]
    - "Bình Định: TP Quy Nhơn" -> [('Bình Định', 'TP Quy Nhơn', None)]
    """
    locations = []
    
    # Xử lý định dạng "City │ District" hoặc "Province │ City │ District"
    if "│" in item:
        segments = [seg.strip() for seg in item.split("│") if seg.strip()]
        if len(segments) == 2:
            # Dạng "City │ District"
            locations.append((None, segments[0], segments[1]))
        elif len(segments) == 3:
            # Dạng "Province │ City │ District"
            locations.append((segments[0], segments[1], segments[2]))
        elif len(segments) == 1:
            # Chỉ có 1 phần
            locations.append((None, segments[0], None))
        else:
            # Nhiều hơn 3 phần, lấy 3 phần đầu
            locations.append((segments[0], segments[1], segments[2]))
    
    # Xử lý định dạng "Province:City" hoặc "City:District" 
    elif ":" in item:
        segments = item.split(":", 1)
        if len(segments) == 2:
            part1, part2 = segments[0].strip(), segments[1].strip()
            
            # Nếu part2 chứa dấu phẩy -> multiple districts trong cùng city/province
            if "," in part2:
                districts = [d.strip() for d in part2.split(",") if d.strip()]
                for district in districts:
                    # Kiểm tra xem có phải "province:city" không (có "TP" trong district)
                    if "TP" in district.upper():
                        locations.append((part1, district, None))
                    else:
                        # Dạng "City:District1, District2"
                        locations.append((None, part1, district))
            else:
                # Single location sau dấu ":"
                # Kiểm tra xem có phải "province:city" không (có "TP" trong part2)
                if "TP" in part2.upper():
                    locations.append((part1, part2, None))
                else:
                    # Dạng "City:District"
                    locations.append((None, part1, part2))
        else:
            locations.append((None, item, None))
    
    else:
        # Item đơn giản không có ":" hay "│"
        locations.append((None, item, None))
    
    return locations
