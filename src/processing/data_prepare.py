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
    
    logger.info(f"Đã chuẩn bị {len(company_df)} bản ghi DimCompany")
    return company_df

def check_dimension_changes(
    duck_conn,
    new_records: pd.DataFrame,
    dim_table: str,
    natural_key: str,
    compare_columns: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
    records_to_insert = []
    records_to_update = []
    unchanged_records = []
    
    try:
        for _, new_record in new_records.iterrows():
            natural_key_value = new_record[natural_key]
            
            # Tìm bản ghi hiện tại trong dimension
            query = f"""
                SELECT * FROM {dim_table}
                WHERE {natural_key} = ? AND is_current = TRUE
            """
            
            existing = duck_conn.execute(query, [natural_key_value]).fetchdf()
            
            if existing.empty:
                # Bản ghi mới hoàn toàn
                records_to_insert.append(new_record)
            else:
                # So sánh các trường để xem có thay đổi không
                existing_record = existing.iloc[0]
                has_changes = False
                
                for col in compare_columns:
                    if col in new_record.index and col in existing_record.index:
                        old_val = existing_record[col]
                        new_val = new_record[col]
                        
                        # Handle null values
                        old_is_null = pd.isna(old_val) if old_val is not None else True
                        new_is_null = pd.isna(new_val) if new_val is not None else True
                        
                        if old_is_null and new_is_null:
                            continue
                        elif old_is_null != new_is_null:
                            has_changes = True
                            break
                        elif str(old_val) != str(new_val):
                            has_changes = True
                            break
                
                if has_changes:
                    # Cần update: đóng record cũ và tạo record mới
                    records_to_update.append({
                        'old_record': existing_record,
                        'new_record': new_record
                    })
                else:
                    # Không có thay đổi
                    unchanged_records.append(new_record)
    
    except Exception as e:
        logger.error(f"Lỗi khi check dimension changes: {e}")
        # Fallback: coi tất cả là insert mới
        records_to_insert = new_records.to_dict('records')
    
    return (
        pd.DataFrame(records_to_insert) if records_to_insert else pd.DataFrame(),
        records_to_update,
        pd.DataFrame(unchanged_records) if unchanged_records else pd.DataFrame()
    )

def apply_scd_type2_updates(duck_conn, dim_table: str, surrogate_key: str, updates: List[Dict]):
    """
    Áp dụng SCD Type 2 updates: đóng bản ghi cũ và tạo bản ghi mới
    
    Args:
        duck_conn: Kết nối DuckDB
        dim_table: Tên bảng dimension
        surrogate_key: Tên cột surrogate key
        updates: List các bản ghi cần update
    """
    today = datetime.now().date()
    
    for update_info in updates:
        old_record = update_info['old_record']
        new_record = update_info['new_record']
        try:
            logger.info(f"Updating old record: {old_record.to_dict() if hasattr(old_record, 'to_dict') else old_record}")
            logger.info(f"Surrogate key value: {old_record[surrogate_key]}")
            # 1. Đóng bản ghi cũ
            update_query = f"""
                UPDATE {dim_table}
                SET expiry_date = ?, is_current = FALSE
                WHERE {surrogate_key} = ?
            """
            # Sửa thành:
            surrogate_key_value = old_record[surrogate_key]
            if hasattr(surrogate_key_value, 'item'):
                surrogate_key_value = surrogate_key_value.item()  # Chuyển numpy.int32 -> int
            duck_conn.execute(update_query, [today, surrogate_key_value])
            
            # 2. Insert bản ghi mới
            new_record_dict = new_record.to_dict() if hasattr(new_record, 'to_dict') else dict(new_record)

            # Loại bỏ mọi trường liên quan đến surrogate key (phòng trường hợp tên cột viết hoa/thường khác nhau)
            for key in list(new_record_dict.keys()):
                if key.lower() == surrogate_key.lower():
                    del new_record_dict[key]

            # Đặt effective_date và is_current
            new_record_dict['effective_date'] = today
            new_record_dict['is_current'] = True
            new_record_dict['expiry_date'] = None

            # Insert
            columns = list(new_record_dict.keys())
            placeholders = ', '.join(['?'] * len(columns))
            values = [new_record_dict[col] for col in columns]

            # Handle JSON values
            for i, val in enumerate(values):
                if isinstance(val, (dict, list)):
                    values[i] = json.dumps(val)

            logger.info(f"Inserting new SCD2 record into {dim_table}: {new_record_dict}")

            insert_query = f"""
                INSERT INTO {dim_table} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            duck_conn.execute(insert_query, values)
            
            logger.info(f"Updated {dim_table}: closed old record {old_record[surrogate_key]} and created new record")
            
        except Exception as e:
            logger.error(f"Lỗi khi apply SCD2 update cho {dim_table}: {e}")

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
    max_date = (current_date + timedelta(days=365)).date()
    
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
    Tính toán load_month từ date value
    
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
        elif hasattr(date_value, 'date'):
            date_value = date_value
        
        return date_value.strftime('%Y-%m')
    except:
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
    1. Cặp "value1:value2" có 2 dạng:
       - "province:city" (nhận biết bởi có "TP" trong value2)
       - "city:district" (không có "TP" trong value2)
    2. Dạng chỉ có "city" riêng lẻ
    3. List nhiều cặp sẽ được tách từng cặp riêng biệt
    4. Xét trùng lặp dựa trên 3 cột: (province, city, district)
    
    Args:
        staging_records: Dữ liệu từ staging
        
    Returns:
        DataFrame cho DimLocation với unique (province, city, district) combinations
    """
    logger.info("Chuẩn bị dữ liệu cho DimLocation")
    locations = []
    
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
        """Trích xuất danh sách location pairs từ record"""
        location_pairs_list = []
        
        # Xử lý trường location_pairs nếu có
        if 'location_pairs' in record:
            location_pairs_value = record['location_pairs']
            
            # Kiểm tra null an toàn
            try:
                is_null = pd.isna(location_pairs_value)
                # Nếu là array, kiểm tra all null
                if hasattr(is_null, '__len__') and len(is_null) > 1:
                    is_null = is_null.all()
            except (ValueError, TypeError):
                # Fallback: check directly
                is_null = location_pairs_value is None or str(location_pairs_value).lower() in ['nan', 'none', '']
            
            if not is_null:
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
            
            # Kiểm tra null an toàn cho location
            try:
                is_null = pd.isna(location_value)
                if hasattr(is_null, '__len__') and len(is_null) > 1:
                    is_null = is_null.all()
            except (ValueError, TypeError):
                is_null = location_value is None or str(location_value).lower() in ['nan', 'none', '']
            
            if not is_null:
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
    
    logger.info(f"Đã chuẩn bị {len(location_df)} bản ghi DimLocation unique")
    
    # Log thống kê để debug
    logger.info("Thống kê DimLocation:")
    logger.info(f"  - Có province: {location_df['province'].notna().sum()}")
    logger.info(f"  - Có city: {location_df['city'].notna().sum()}")
    logger.info(f"  - Có district: {location_df['district'].notna().sum()}")
    
    # Log một vài examples
    logger.info("Một vài ví dụ DimLocation:")
    for i, row in location_df.head(10).iterrows():
        logger.info(f"  {row['province']} | {row['city']} | {row['district']}")
    
    return location_df

def parse_single_location_item(item: str) -> List[Tuple[str, str, str]]:
    """
    Parse một location item riêng lẻ thành các tuple (province, city, district)
    
    Xử lý patterns:
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

def parse_job_location(location_str: str) -> List[Tuple[str, str, str]]:
    """
    Parse location string từ job thành list các tuple (province, city, district)
    
    Logic parsing MỚI:
    1. Nếu array element chứa dấu phẩy -> tách districts trong cùng 1 city: 
       "Hà Nội: Thanh Xuân, Đống Đa" -> [(None, 'Hà Nội', 'Thanh Xuân'), (None, 'Hà Nội', 'Đống Đa')]
    2. Nếu array elements riêng biệt -> mỗi element là location độc lập:
       ["Hà Nội: Đống Đa", "Hồ Chí Minh", "Bình Dương"] -> 3 locations riêng biệt
    
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
        import json
        # Thử parse JSON trước
        parsed_list = json.loads(location_str)
        if isinstance(parsed_list, list):
            location_items = [str(item).strip() for item in parsed_list if str(item).strip()]
        else:
            location_items = [location_str.strip()]
    except (json.JSONDecodeError, TypeError):
        # Nếu không parse được JSON, thử parse format ['item1', 'item2'] bằng eval
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
    
    # Xử lý từng location item RIÊNG BIỆT - không dùng context cross-item
    for item in location_items:
        if not item or not item.strip():
            continue
        item = item.strip()
        
        # Parse 1 item - có thể chứa comma-separated districts
        item_locations = parse_single_location_item(item)
        locations.extend(item_locations)
    
    return locations if locations else [(None, 'Unknown', None)]
