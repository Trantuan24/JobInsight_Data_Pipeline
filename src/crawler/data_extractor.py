import sys
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

# Thêm đường dẫn gốc dự án vào sys.path để import được các module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Thử import logger từ cả hai cách
try:
    from src.utils.logger import get_logger
except ImportError:
    try:
        # Khi chạy trực tiếp file này
        from utils.logger import get_logger
    except ImportError:
        import logging
        logging.basicConfig(level=logging.INFO)
        def get_logger(name):
            return logging.getLogger(name)

logger = get_logger("crawler.extractor")

# Cố gắng import với cả hai cách
try:
    # Khi import từ bên ngoài src/crawler
    from src.crawler.crawler_config import IMPORTANT_FIELDS
    from src.crawler.crawler_utils import job_data_lock, job_id_crawled, parse_last_update
except ImportError:
    # Khi chạy trực tiếp từ thư mục src/crawler
    from crawler_config import IMPORTANT_FIELDS
    from crawler_utils import job_data_lock, job_id_crawled, parse_last_update

async def extract_job_data(job_element):
    """Trích xuất thông tin chi tiết từ phần tử job (async version)"""
    try:
        job_id = await job_element.get_attribute('data-job-id')
        title_element = await job_element.query_selector('div.box-header div.body div.title-block h3.title a span[data-original-title]')
        title = await title_element.get_attribute('data-original-title') if title_element else "Unknown Title"
        job_url_element = await job_element.query_selector('div.box-header div.body div.title-block h3.title a')
        job_url = await job_url_element.get_attribute('href') if job_url_element else None
        company_element = await job_element.query_selector('div.box-header div.body a.company')
        company_name = await company_element.text_content() if company_element else "Unknown Company"
        company_name = company_name.strip() if company_name else "Unknown Company"
        company_url = await company_element.get_attribute('href') if company_element else None
        salary_element = await job_element.query_selector('div.box-header div.body div.title-block label.title-salary')
        salary = await salary_element.text_content() if salary_element else "Not specified"
        salary = salary.strip() if salary else "Not specified"
        
        skills = []
        skill_elements = await job_element.query_selector_all('div.box-header div.body div.skills label.item')
        for skill in skill_elements:
            skill_text = await skill.text_content()
            skill_text = skill_text.strip() if skill_text else ""
            if skill_text.endswith('+'):
                additional_skills = await skill.get_attribute('data-original-title')
                if additional_skills:
                    skills.append(additional_skills)
            else:
                skills.append(skill_text)
        
        location_element = await job_element.query_selector('div.box-header div.body div.box-info div.label-content label.address')
        location_tooltip = await location_element.get_attribute('data-original-title') if location_element else None
        location = await location_element.text_content() if location_element else "Unknown Location"
        location = location.strip() if location else "Unknown Location"
        
        deadline_element = await job_element.query_selector('div.box-header div.body div.box-info div.label-content label.time strong')
        deadline = await deadline_element.text_content() if deadline_element else "Unknown"
        deadline = deadline.strip() if deadline else "Unknown"
        
        verified = await job_element.query_selector('span.icon-verified-employer') is not None
        
        update_element = await job_element.query_selector('div.box-header div.body label.deadline')
        last_update = await update_element.text_content() if update_element else "Unknown"
        last_update = last_update.strip() if last_update else "Unknown"
        
        logo_element = await job_element.query_selector('div.box-header div.avatar a img')
        logo_url = await logo_element.get_attribute('src') if logo_element else None
        
        crawled_at = datetime.now().isoformat()
        
        # Tạo job data
        job_data = {
            'job_id': job_id,
            'title': title,
            'job_url': job_url,
            'company_name': company_name,
            'company_url': company_url,
            'salary': salary,
            'skills': skills,
            'location': location,
            'location_detail': location_tooltip,
            'deadline': deadline,
            'verified_employer': verified,
            'last_update': last_update,
            'logo_url': logo_url,
            'crawled_at': crawled_at
        }
        
        # Nếu là job mới, thêm posted_time
        with job_data_lock:
            if job_id not in job_id_crawled:
                seconds_ago = parse_last_update(last_update)
                posted_time = datetime.now().timestamp() - seconds_ago
                job_data['posted_time'] = datetime.fromtimestamp(posted_time).isoformat()
        
        return job_data
    except Exception as e:
        logger.error(f"Error extracting job data: {str(e)}")
        return None

def has_job_updated(existing_data, new_data):
    """Kiểm tra xem dữ liệu job có cập nhật không"""
    for field in IMPORTANT_FIELDS:
        if field in existing_data and field in new_data:
            if existing_data[field] != new_data[field]:
                return True
    
    # So sánh skills nếu có
    if 'skills' in existing_data and 'skills' in new_data:
        if len(existing_data['skills']) != len(new_data['skills']):
            return True
        
        for skill1, skill2 in zip(existing_data['skills'], new_data['skills']):
            if skill1 != skill2:
                return True
    
    return False

def extract_tracking_data(job_data):
    """Trích xuất thông tin cần theo dõi từ job data"""
    tracking_data = {}
    
    # Chỉ lưu các trường quan trọng cần theo dõi
    for field in IMPORTANT_FIELDS:
        if field in job_data:
            tracking_data[field] = job_data[field]
    
    # Thêm các trường bổ sung cần thiết
    if 'skills' in job_data:
        tracking_data['skills'] = job_data['skills']
    if 'posted_time' in job_data:
        tracking_data['posted_time'] = job_data['posted_time']
    
    tracking_data['last_crawled'] = datetime.now().isoformat()
    
    return tracking_data 