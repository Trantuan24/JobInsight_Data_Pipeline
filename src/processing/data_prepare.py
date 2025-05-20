import pandas as pd
import logging
import json
from datetime import datetime, timedelta
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
    Xử lý mỗi cặp location_pair tách biệt và xét trùng lặp dựa trên 3 cột: province, city, district
    Tách các quận/huyện khi một thành phố có nhiều quận/huyện được liệt kê
    
    Args:
        staging_records: Dữ liệu từ staging
        
    Returns:
        DataFrame cho DimLocation với unique province-city-district combinations
    """
    logger.info("Chuẩn bị dữ liệu cho DimLocation")
    locations = []
    
    def parse_location_pair(pair):
        """Helper function to parse location pair and extract province, city, district"""
        province = None
        city = None
        districts = []
        
        if isinstance(pair, str) and ":" in pair:
            parts = pair.split(":", 1)
            if len(parts) == 2:
                part1, part2 = parts[0].strip(), parts[1].strip()
                
                # Trường hợp "Tỉnh:Thành phố" khi có chữ "TP" trong phần sau
                if "TP" in part2:
                    province = part1
                    city = part2
                # Trường hợp "Thành phố:Quận/huyện" khi không có chữ "TP"
                else:
                    city = part1
                    # Kiểm tra và tách các quận/huyện nếu có nhiều, phân cách bằng dấu phẩy
                    if "," in part2:
                        districts = [d.strip() for d in part2.split(",") if d.strip()]
                    else:
                        districts = [part2]
        elif isinstance(pair, str):
            # Trường hợp chỉ có tên thành phố
            city = pair
        
        return province, city, districts
    
    # Xử lý từng bản ghi staging
    for _, record in staging_records.iterrows():
        if pd.isna(record['location']):
            continue
            
        # Xử lý location_pairs từ nhiều định dạng có thể có
        location_pairs_list = []
        
        if isinstance(record['location_pairs'], list):
            location_pairs_list = record['location_pairs']
        elif isinstance(record['location_pairs'], dict):
            location_pairs_list = [json.dumps(record['location_pairs'])]
        elif isinstance(record['location_pairs'], str):
            try:
                parsed = json.loads(record['location_pairs'])
                if isinstance(parsed, list):
                    location_pairs_list = parsed
                elif isinstance(parsed, dict):
                    location_pairs_list = [json.dumps(parsed)]
                else:
                    location_pairs_list = [str(parsed)]
            except json.JSONDecodeError:
                if record['location_pairs']:
                    location_pairs_list = [record['location_pairs']]
        
        # Không có location_pairs thì lấy location
        if not location_pairs_list and isinstance(record['location'], str):
            city_districts_map = {}
            
            # Tách location string và kiểm tra cấu trúc "City │ District1, District2"
            location_parts = record['location'].split("│")
            if len(location_parts) == 2:
                city = location_parts[0].strip()
                districts_str = location_parts[1].strip()
                districts = [d.strip() for d in districts_str.split(",") if d.strip()]
                
                # Tạo một entry cho mỗi district
                for district in districts:
                    locations.append({
                        'location': f"{city} - {district}",
                        'location_detail': record.get('location_detail'),
                        'location_pairs': None,
                        'province': None,
                        'city': city,
                        'district': district
                    })
            else:
                # Không có định dạng đặc biệt, xem như là thành phố
                locations.append({
                    'location': record['location'],
                    'location_detail': record.get('location_detail'),
                    'location_pairs': None,
                    'province': None,
                    'city': record['location'],
                    'district': None
                })
        
        # Xử lý mỗi location_pair riêng biệt
        for pair in location_pairs_list:
            province, city, districts = parse_location_pair(pair)
            
            # Với mỗi district, tạo một bản ghi riêng
            if city:
                if districts:
                    for district in districts:
                        locations.append({
                            'location': f"{city} - {district}",
                            'location_detail': record.get('location_detail'),
                            'location_pairs': json.dumps([pair]) if pair else None,
                            'province': province,
                            'city': city,
                            'district': district
                        })
                else:
                    # Khfrom datetime import datetime, timedelta ông có district
                    locations.append({
                        'location': city,
                        'location_detail': record.get('location_detail'),
                        'location_pairs': json.dumps([pair]) if pair else None,
                        'province': province,
                        'city': city,
                        'district': None
                    })
    
    # Tạo DataFrame
    if not locations:
        locations.append({
            'location': 'Unknown', 
            'location_detail': None, 
            'location_pairs': None,
            'province': None,
            'city': None,
            'district': None
        })
    
    location_df = pd.DataFrame(locations)
    
    # Thêm các cột cần thiết cho SCD Type 2
    today = datetime.now().date()
    location_df['effective_date'] = today
    location_df['is_current'] = True
    
    # Loại bỏ duplicate dựa trên province, city, district
    location_df = location_df.drop_duplicates(subset=['province', 'city', 'district'])
    
    logger.info(f"Đã chuẩn bị {len(location_df)} bản ghi DimLocation")
    return location_df

