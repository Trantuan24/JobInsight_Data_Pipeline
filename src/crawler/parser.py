from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import threading
import os
import sys

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
    from src.crawler.crawler_utils import log_action, parse_last_update
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from src.crawler.crawler_utils import log_action, parse_last_update

from src.crawler.crawler_config import BACKUP_DIR

logger = get_logger("crawler.parser")

# Thread lock for job_id_crawled
job_data_lock = threading.Lock()
job_id_crawled = set()

def parse_html_files():
    """Parse các file HTML đã backup để extract job data"""
    logger.info("Starting HTML parsing")
    log_action("parse_start", status="info")
    
    all_jobs = []
    html_files = sorted(BACKUP_DIR.glob("it_p*.html"))
    
    if not html_files:
        logger.warning("No HTML files found to parse")
        return pd.DataFrame()
    
    for html_file in html_files:
        try:
            logger.info(f"Parsing {html_file}")
            
            # Đọc HTML content
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Parse HTML với BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Tìm các job items
            job_items = soup.find_all('div', class_='job-item-2')
            logger.info(f"Found {len(job_items)} jobs in {html_file.name}")
            
            for job_item in job_items:
                try:
                    # Extract job data từ HTML element
                    job_data = extract_job_data(job_item)
                    
                    if job_data['job_id'] and job_data['title']:
                        all_jobs.append(job_data)
                        
                except Exception as e:
                    logger.error(f"Error parsing job item: {str(e)}")
                    continue
            
            log_action("parse_ok", status="success", error=f"Parsed {len(job_items)} jobs from {html_file.name}")
            
        except Exception as e:
            logger.error(f"Error parsing file {html_file}: {str(e)}")
            log_action("parse_error", status="error", error=str(e))
            continue
    
    # Chuyển sang DataFrame
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        df = df.drop_duplicates(subset=['job_id'])
        
        # Đảm bảo thứ tự các cột đúng với schema SQL và không có raw_data
        column_order = [
            'job_id', 'title', 'job_url', 'company_name', 'company_url',
            'salary', 'skills', 'location', 'location_detail', 'deadline',
            'verified_employer', 'last_update', 'logo_url', 'posted_time', 'crawled_at'
        ]
        
        # Loại bỏ cột raw_data nếu có
        if 'raw_data' in df.columns:
            df = df.drop(columns=['raw_data'])
            
        # Chỉ lấy các cột đã có trong df và đảm bảo thứ tự đúng
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        logger.info(f"Parsed total {len(df)} unique jobs")
        return df
    else:
        logger.warning("No jobs parsed from HTML files")
        return pd.DataFrame()

def extract_job_data(job_item):
    """Extract job data từ một job item HTML"""
    job_data = {
        'job_id': None,
        'title': None,
        'job_url': None,
        'company_name': None,
        'company_url': None,
        'salary': None,
        'skills': [],
        'location': None,
        'location_detail': None,
        'deadline': None,
        'verified_employer': False,
        'last_update': None,
        'logo_url': None,
        'crawled_at': datetime.now().isoformat()
    }
    
    # Extract job_id - lấy trực tiếp từ data-job-id nếu có
    job_id_attr = job_item.get('data-job-id')
    if job_id_attr:
        job_data['job_id'] = job_id_attr
    else:
        # Fallback: Extract từ href như trước
        link_elem = job_item.find('a', href=True)
        if link_elem:
            href = link_elem['href']
            clean_href = href.split('?')[0]
            job_id_match = clean_href.split('-')[-1].replace('.html', '')
            if job_id_match:
                job_data['job_id'] = job_id_match
                job_data['job_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
    
    # Title - lấy từ title span data-original-title nếu có
    title_span = job_item.select_one('h3.title a span[data-original-title]')
    if title_span and title_span.has_attr('data-original-title'):
        job_data['title'] = title_span['data-original-title'].strip()
    else:
        # Fallback: Lấy text từ title element
        title_elem = job_item.find('h3', class_='title')
        if title_elem:
            job_data['title'] = title_elem.get_text(strip=True)
    
    # Job URL
    job_url_elem = job_item.select_one('h3.title a')
    if job_url_elem and job_url_elem.has_attr('href'):
        href = job_url_elem['href']
        job_data['job_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
    
    # Company
    company_elem = job_item.find('a', class_='company')
    if company_elem:
        job_data['company_name'] = company_elem.get_text(strip=True)
        company_href = company_elem.get('href')
        if company_href:
            job_data['company_url'] = f"https://www.topcv.vn{company_href}" if company_href.startswith('/') else company_href
    
    # Location - check for tooltip data first
    location_elem = job_item.select_one('label.address')
    if location_elem:
        # Lấy text thuần túy, không lấy HTML tag
        job_data['location'] = location_elem.get_text(strip=True)
        if location_elem.has_attr('data-original-title'):
            # Lấy content của tooltip là mô tả chi tiết location
            tooltip = location_elem['data-original-title']
            # Xử lý HTML trong tooltip để lấy text
            if tooltip:
                # Tạo một soup mới để parse tooltip HTML
                tooltip_soup = BeautifulSoup(tooltip, 'html.parser')
                job_data['location_detail'] = tooltip_soup.get_text(strip=True)
        
        # Nếu không có tooltip hoặc không parse được, sử dụng location
        if 'location_detail' not in job_data or not job_data['location_detail']:
            job_data['location_detail'] = job_data['location']
    else:
        # Fallback: old structure
        location_elem = job_item.find('div', class_='info')
        if location_elem:
            location_text = location_elem.find('span')
            if location_text:
                job_data['location'] = location_text.get_text(strip=True)
                job_data['location_detail'] = job_data['location']
    
    # Salary - check label.title-salary first
    salary_elem = job_item.select_one('label.title-salary')
    if salary_elem:
        job_data['salary'] = salary_elem.get_text(strip=True)
    else:
        # Fallback: old structure
        salary_elem = job_item.find('div', class_='salary')
        if salary_elem:
            job_data['salary'] = salary_elem.get_text(strip=True)
    
    # Skills - check both new and old structures
    skills = []
    # New structure: labels with possible tooltip
    skill_elems = job_item.select('div.skills label.item')
    if skill_elems:
        for skill in skill_elems:
            skill_text = skill.get_text(strip=True)
            # If skill ends with '+' check for tooltip additional skills
            if skill_text.endswith('+') and skill.has_attr('data-original-title'):
                additional_skills = skill['data-original-title']
                if additional_skills and not additional_skills.startswith('<'):
                    skills.append(additional_skills)
            else:
                skills.append(skill_text)
    else:
        # Fallback: old structure with tag-item
        old_skill_elems = job_item.find_all('span', class_='tag-item')
        if old_skill_elems:
            skills = [skill.get_text(strip=True) for skill in old_skill_elems]
    
    job_data['skills'] = skills
    
    # Deadline - new format
    deadline_elem = job_item.select_one('label.time strong')
    if deadline_elem:
        job_data['deadline'] = deadline_elem.get_text(strip=True)
    else:
        # Fallback: Check old format
        deadline_found = False
        label_elems = job_item.find_all('div', class_='label-content')
        for label in label_elems:
            text = label.get_text(strip=True)
            if 'Còn' in text and 'ngày' in text:
                parts = text.strip().split()
                for i, part in enumerate(parts):
                    if part == "Còn" and i+1 < len(parts):
                        job_data['deadline'] = parts[i+1]
                        deadline_found = True
                        break
            if deadline_found:
                break
    
    # Verified employer - đảm bảo là boolean
    verified_elem = job_item.select_one('span.icon-verified-employer')
    job_data['verified_employer'] = verified_elem is not None
    
    # Last update - check for label.deadline
    update_elem = job_item.select_one('label.deadline')
    if update_elem:
        job_data['last_update'] = update_elem.get_text(strip=True)
    else:
        # Fallback: Check old format
        update_found = False
        label_elems = job_item.find_all('div', class_='label-content')
        for label in label_elems:
            text = label.get_text(strip=True)
            if 'Cập nhật' in text:
                job_data['last_update'] = text.strip()
                update_found = True
                break
    
    # Xác thực dữ liệu đã trích xuất
    # Đảm bảo location_detail không chứa HTML tag
    if job_data['location_detail'] and ('<' in job_data['location_detail'] or '>' in job_data['location_detail']):
        job_data['location_detail'] = job_data['location']
        
    # Đảm bảo deadline không chứa nội dung HTML
    if job_data['deadline'] and ('<' in job_data['deadline'] or '>' in job_data['deadline'] or 'margin' in job_data['deadline']):
        job_data['deadline'] = None
        
    # Đảm bảo verified_employer là boolean
    if not isinstance(job_data['verified_employer'], bool):
        job_data['verified_employer'] = False
        
    # Đảm bảo last_update là text hợp lệ
    if job_data['last_update'] and len(job_data['last_update']) < 3:
        job_data['last_update'] = "Unknown"
    
    # Logo
    logo_elem = job_item.select_one('a img')
    if logo_elem and logo_elem.has_attr('src'):
        job_data['logo_url'] = logo_elem['src']
    else:
        # Fallback: old structure
        logo_elem = job_item.find('img', class_='img-responsive')
        if logo_elem:
            job_data['logo_url'] = logo_elem.get('src', '')
    
    # Posted time - only set for new jobs
    if job_data['job_id']:
        with job_data_lock:
            if job_data['job_id'] not in job_id_crawled and job_data['last_update']:
                seconds_ago = parse_last_update(job_data['last_update'])
                posted_time = datetime.now().timestamp() - seconds_ago
                job_data['posted_time'] = datetime.fromtimestamp(posted_time).isoformat()
                job_id_crawled.add(job_data['job_id'])
    
    return job_data 