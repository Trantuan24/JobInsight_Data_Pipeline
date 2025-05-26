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
    company_df['is_current'] = True
    
    # Loại bỏ duplicate
    company_df = company_df.drop(columns=['company_name']).drop_duplicates(subset=['company_name_standardized'])
    
    logger.info(f"Đã chuẩn bị {len(company_df)} bản ghi DimCompany")
    return company_df

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
