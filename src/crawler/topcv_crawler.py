from playwright.async_api import async_playwright
import time
import random
import json
import logging
import os
import pandas as pd
from datetime import datetime
import re
import shutil
import asyncio
import concurrent.futures
from typing import List, Dict, Any, Optional
import threading

# Khóa để đồng bộ hóa truy cập vào dữ liệu chung
job_data_lock = threading.Lock()

# Tạo thư mục data nếu chưa có
os.makedirs('data', exist_ok=True)
os.makedirs('data/job_tracking', exist_ok=True)
os.makedirs('data/job_tracking/backups', exist_ok=True)

# Thiết lập logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename=f'logs/topcv_crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Danh sách keywords cụ thể
KEYWORDS = [
    'Backend', 'Frontend', 'Fullstack', 'DevOps', 'QA', 
    'Python', 'Java', 'JavaScript', 'React', 'NodeJS',
    'Data', 'AI', 'Machine Learning', 'Senior', 'Junior'
]

# Danh sách user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
]

# Trường quan trọng để kiểm tra thay đổi
IMPORTANT_FIELDS = ['title', 'salary', 'deadline', 'last_update', 'location', 'company_name']

# Số instance browser tối đa cho đa luồng
MAX_CONCURRENT_BROWSERS = 3

# Hàm tạo tên file tracking theo tháng
def get_tracking_filename():
    """Trả về file tracking JSON theo tháng hiện tại"""
    current_month = datetime.now().strftime('%Y%m')
    return f'data/job_tracking/jobs_tracking_{current_month}.json'

# Tạo backup cho file tracking
def backup_tracking_file():
    """Tạo bản backup cho file tracking hiện tại"""
    current_file = get_tracking_filename()
    if os.path.exists(current_file):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f'data/job_tracking/backups/jobs_tracking_backup_{timestamp}.json'
        try:
            shutil.copy2(current_file, backup_file)
            logging.info(f"Created backup: {backup_file}")
            return True
        except Exception as e:
            logging.error(f"Error creating backup: {str(e)}")
    return False

# Lưu trữ job_id và thông tin cần kiểm tra
job_data_crawled = {}
CRAWLED_JOBS_PATH = get_tracking_filename()

try:
    with open(CRAWLED_JOBS_PATH, 'r', encoding='utf-8') as f:
        job_data_crawled = json.load(f)
    logging.info(f"Loaded {len(job_data_crawled)} tracked jobs from {CRAWLED_JOBS_PATH}")
except FileNotFoundError:
    logging.info(f"No tracking file found. Creating new tracking at {CRAWLED_JOBS_PATH}")

# Set các job ID đã crawl
job_id_crawled = set(job_data_crawled.keys())

async def random_delay(min_seconds=1, max_seconds=3):
    """Tạo độ trễ ngẫu nhiên để mô phỏng hành vi người dùng thực (async version)"""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)
    return delay

def save_crawled_data():
    """Lưu dữ liệu tracking jobs"""
    # Tạo backup trước khi lưu
    backup_tracking_file()
    
    with open(CRAWLED_JOBS_PATH, 'w', encoding='utf-8') as f:
        json.dump(job_data_crawled, f, ensure_ascii=False, indent=4)
    logging.info(f"Saved {len(job_data_crawled)} tracked jobs to {CRAWLED_JOBS_PATH}")

def parse_last_update(update_text):
    """Phân tích chuỗi ngày cập nhật và chuyển đổi thành số giây"""
    try:
        # Xử lý các định dạng chuỗi thời gian như "3 ngày trước", "1 giờ trước", vv.
        if "ngày" in update_text:
            days = int(re.search(r'(\d+)', update_text).group(1))
            return days * 24 * 60 * 60  # Chuyển thành số giây
        elif "giờ" in update_text:
            hours = int(re.search(r'(\d+)', update_text).group(1))
            return hours * 60 * 60  # Chuyển thành số giây
        elif "phút" in update_text:
            minutes = int(re.search(r'(\d+)', update_text).group(1))
            return minutes * 60  # Chuyển thành số giây
        elif "giây" in update_text:
            seconds = int(re.search(r'(\d+)', update_text).group(1))
            return seconds
        else:
            return 0
    except Exception:
        return 0

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
        logging.error(f"Error extracting job data: {str(e)}")
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

async def crawl_job_listings(page_url, keyword=None, num_pages=1):
    """Crawler chính cho một URL cụ thể với từ khóa tùy chọn, trả về danh sách jobs (async version)"""
    # Xây dựng URL hoàn chỉnh nếu có keyword
    if keyword:
        encoded_keyword = keyword.replace(' ', '+')
        final_url = f"{page_url}?keyword={encoded_keyword}" if '?' not in page_url else f"{page_url}&keyword={encoded_keyword}"
    else:
        final_url = page_url
    
    # Đảm bảo URL bắt đầu bằng https://
    if not final_url.startswith('http'):
        final_url = 'https://' + final_url.lstrip('/')
    
    logging.info(f"Starting crawl for URL: {final_url}")
    
    async with async_playwright() as p:
        user_agent = random.choice(USER_AGENTS)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1366, 'height': 768},
        )
        
        page = await context.new_page()
        
        # Ẩn WebDriver đơn giản
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
        """)
        
        all_jobs = []
        try:
            for page_num in range(1, num_pages + 1):
                current_url = f"{final_url}&page={page_num}" if '?' in final_url else f"{final_url}?page={page_num}"
                if page_num == 1:
                    current_url = final_url  # Sử dụng URL gốc cho page 1
                
                logging.info(f"Crawling page {page_num}: {current_url}")
                
                try:
                    # Điều hướng đến trang
                    for retry_attempt in range(3):  # Thử tối đa 3 lần
                        try:
                            # Tăng timeout lên 60s thay vì 30s
                            await page.goto(current_url, wait_until='domcontentloaded', timeout=60000)
                            await page.wait_for_selector('div.job-item-2', timeout=30000)
                            break  # Thoát khỏi vòng lặp retry nếu thành công
                        except Exception as e:
                            if retry_attempt == 2:  # Nếu là lần thử cuối cùng
                                logging.error(f"Error loading page after 3 attempts: {str(e)}")
                                raise  # Ném lỗi để khớp với xử lý hiện tại
                            else:
                                logging.warning(f"Error loading page (attempt {retry_attempt+1}/3), retrying: {str(e)}")
                                await asyncio.sleep(5)  # Đợi 5 giây trước khi thử lại
                except Exception as e:
                    logging.error(f"Error loading page: {str(e)}")
                    continue
                
                try:
                    # Đơn giản hóa scroll
                    await page.evaluate("""
                        () => {
                            window.scrollTo(0, document.body.scrollHeight / 2);
                            setTimeout(() => {
                                window.scrollTo(0, document.body.scrollHeight);
                            }, 1000);
                        }
                    """)
                    
                    await asyncio.sleep(2)  # Thay thế cho page.wait_for_timeout
                    job_elements = await page.query_selector_all('div.job-item-2')
                    logging.info(f"Found {len(job_elements)} job listings on page {page_num}")
                    
                    # Bất đồng bộ xử lý tất cả job elements cùng lúc
                    job_tasks = [extract_job_data(job_element) for job_element in job_elements]
                    job_results = await asyncio.gather(*job_tasks)
                    
                    for job_data in job_results:
                        if not job_data:
                            continue
                        
                        job_id = job_data['job_id']
                        # Cập nhật thông tin tracking
                        tracking_data = extract_tracking_data(job_data)
                        
                        with job_data_lock:
                            if job_id in job_id_crawled:
                                # Kiểm tra xem job có cập nhật không
                                existing_job = job_data_crawled.get(job_id, {})
                                if has_job_updated(existing_job, tracking_data):
                                    logging.info(f"Updating changed job ID: {job_id}")
                                    # Sử dụng posted_time cũ nếu có
                                    if 'posted_time' in existing_job and 'posted_time' not in job_data:
                                        job_data['posted_time'] = existing_job['posted_time']
                                    # Cập nhật tracking data
                                    job_data_crawled[job_id] = tracking_data
                                    all_jobs.append(job_data)
                                else:
                                    logging.info(f"Skipping unchanged job ID: {job_id}")
                            else:
                                logging.info(f"Found new job ID: {job_id}")
                                job_id_crawled.add(job_id)
                                job_data_crawled[job_id] = tracking_data
                                all_jobs.append(job_data)
                
                except Exception as e:
                    logging.error(f"Error processing page {page_num}: {str(e)}")
                    # Tiếp tục với page tiếp theo thay vì dừng hoàn toàn
                    continue
                
                # Độ trễ giữa các trang
                await asyncio.sleep(random.uniform(2, 4))
                
            # Lưu dữ liệu theo dõi job_id (do đang ở bên ngoài lock nên không cần lock lại)
            save_crawled_data()
            
            logging.info(f"Successfully crawled {len(all_jobs)} jobs from '{keyword or 'no keyword'}'")
            return all_jobs
                
        except Exception as e:
            logging.error(f"Error during crawling: {str(e)}")
            with job_data_lock:
                save_crawled_data()
            return []
        finally:
            await browser.close()

async def crawl_keyword(base_url, keyword, num_pages):
    """Hàm helper để chạy crawler cho một keyword"""
    try:
        logging.info(f"Crawling for keyword: {keyword}")
        results = await crawl_job_listings(base_url, keyword, num_pages)
        logging.info(f"Completed crawling for keyword '{keyword}', found {len(results)} jobs")
        return results
    except Exception as e:
        logging.error(f"Error in crawl_keyword for '{keyword}': {str(e)}")
        return []

async def async_crawl_multiple_keywords(base_url="https://www.topcv.vn/viec-lam-it", keywords=None, num_pages=1):
    """Chạy crawler cho nhiều keywords đồng thời (với giới hạn đồng thời)"""
    if not keywords:
        keywords = KEYWORDS
    
    logging.info(f"Starting async multi-keyword crawl with {len(keywords)} keywords")
    
    all_jobs = []
    
    # Crawl trang gốc trước (không keyword)
    logging.info(f"Crawling base URL without keywords: {base_url}")
    base_results = await crawl_job_listings(base_url, keyword=None, num_pages=num_pages)
    all_jobs.extend(base_results)
    logging.info(f"Found {len(base_results)} jobs from base URL (no keyword)")
    
    # Đợi một chút trước khi tiếp tục với các keyword
    await asyncio.sleep(3)
    
    # Chia keywords thành các nhóm nhỏ hơn để tránh quá tải
    # Giảm MAX_CONCURRENT_BROWSERS xuống còn 2 nếu có nhiều keyword
    max_concurrent = 2 if len(keywords) > 5 else MAX_CONCURRENT_BROWSERS
    keyword_groups = [keywords[i:i+max_concurrent] for i in range(0, len(keywords), max_concurrent)]
    
    # Xử lý từng nhóm keywords
    failed_keywords = []
    for group_idx, group in enumerate(keyword_groups):
        logging.info(f"Processing keyword group {group_idx+1}/{len(keyword_groups)}: {', '.join(group)}")
        
        # Tạo task cho mỗi keyword trong nhóm
        tasks = [crawl_keyword(base_url, keyword, num_pages) for keyword in group]
        
        try:
            # Đặt timeout cho cả nhóm - nếu một keyword nào đó mất quá nhiều thời gian
            group_timeout = 120  # 2 phút cho mỗi nhóm keyword
            group_results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 
                                                  timeout=group_timeout)
            
            # Xử lý kết quả của mỗi keyword
            for idx, results in enumerate(group_results):
                keyword = group[idx]
                if isinstance(results, Exception):
                    logging.error(f"Failed to crawl keyword '{keyword}': {str(results)}")
                    failed_keywords.append(keyword)
                    continue
                
                # Nếu thành công thì add vào kết quả
                all_jobs.extend(results)
                logging.info(f"Added {len(results)} jobs from keyword '{keyword}'")
        
        except asyncio.TimeoutError:
            logging.error(f"Timeout processing keyword group {group_idx+1}")
            # Đánh dấu tất cả keyword trong nhóm này là thất bại
            failed_keywords.extend(group)
        
        # Đợi lâu hơn giữa các nhóm để tránh quá tải
        if len(keyword_groups) > 1 and group_idx < len(keyword_groups) - 1:
            wait_time = random.uniform(8, 12)  # 8-12 giây giữa các nhóm
            logging.info(f"Waiting {wait_time:.1f}s before processing next keyword group")
            await asyncio.sleep(wait_time)
    
    # Log các keyword thất bại nếu có
    if failed_keywords:
        logging.warning(f"Failed to crawl {len(failed_keywords)} keywords: {', '.join(failed_keywords)}")
    
    # Lọc trùng lặp trước khi trả về
    unique_job_ids = set()
    filtered_jobs = []
    
    for job in all_jobs:
        if job['job_id'] not in unique_job_ids:
            unique_job_ids.add(job['job_id'])
            filtered_jobs.append(job)
    
    logging.info(f"Completed async crawl with {len(filtered_jobs)} unique jobs from {len(all_jobs)} total")
    return filtered_jobs

def crawl_multiple_keywords(base_url="https://www.topcv.vn/viec-lam-it", keywords=None, num_pages=1):
    """Wrapper function để chạy đa luồng và trả về DataFrame"""
    # Chạy async function với asyncio
    all_jobs = asyncio.run(async_crawl_multiple_keywords(base_url, keywords, num_pages))
    
    # Chuyển sang DataFrame
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        # Loại bỏ trùng lặp theo job_id
        df = df.drop_duplicates(subset=['job_id'])
        logging.info(f"Created DataFrame with {len(df)} unique jobs")
        return df
    else:
        logging.warning("No data collected from any keyword!")
        return pd.DataFrame()

if __name__ == "__main__":
    # Chạy crawl và trả về DataFrame
    start_time = time.time()
    
    df_results = crawl_multiple_keywords(num_pages=1)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    if not df_results.empty:
        print(f"Crawled {len(df_results)} job listings in {elapsed_time:.2f} seconds")
        print(f"Average time per job: {elapsed_time/len(df_results):.2f} seconds")
        # DataFrame có thể được sử dụng để import vào database ở bước tiếp theo
        print(df_results.head())