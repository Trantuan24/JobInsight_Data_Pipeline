from playwright.async_api import async_playwright
import time
import random
import pandas as pd
import asyncio
import sys
import os
from typing import List, Dict, Any, Optional

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

logger = get_logger("crawler.main")

# Cố gắng import với cả hai cách
try:
    # Khi import từ bên ngoài src/crawler (ví dụ: từ tests)
    from src.crawler.crawler_config import (
        USER_AGENTS, PAGE_LOAD_TIMEOUT, SELECTOR_TIMEOUT,
        MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES,
        BASE_URL
    )
    from src.crawler.crawler_utils import (
        job_data_lock, job_id_crawled, job_data_crawled,
        save_crawled_data, random_delay
    )
    from src.crawler.data_extractor import (
        extract_job_data, extract_tracking_data, has_job_updated
    )
except ImportError:
    # Khi chạy trực tiếp từ thư mục src/crawler
    from crawler_config import (
        USER_AGENTS, PAGE_LOAD_TIMEOUT, SELECTOR_TIMEOUT,
        MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES,
        BASE_URL
    )
    from crawler_utils import (
        job_data_lock, job_id_crawled, job_data_crawled,
        save_crawled_data, random_delay
    )
    from data_extractor import (
        extract_job_data, extract_tracking_data, has_job_updated
    )

async def crawl_job_listings(page_url, num_pages=5):
    """
    Crawler chính cho một URL cụ thể, trả về danh sách jobs (async version).
    
    Args:
        page_url (str): URL gốc của trang tuyển dụng (ví dụ: https://www.topcv.vn/viec-lam-it)
        num_pages (int, optional): Số trang cần crawl. Mặc định là 5.
        
    Returns:
        List[Dict[str, Any]]: Danh sách các job đã crawl, mỗi job là một dict chứa thông tin chi tiết.
        
    Raises:
        Exception: Khi có lỗi trong quá trình crawl như timeout, kết nối bị từ chối, v.v.
    
    Notes:
        - Sử dụng Playwright để crawl dữ liệu.
        - Thực hiện xử lý bất đồng bộ (async) với asyncio.
        - Tự động xử lý phân trang.
        - Xử lý retry khi gặp lỗi mạng hoặc timeout (tối đa 3 lần).
        - Sử dụng user-agent ngẫu nhiên và delay giữa các request để tránh bị chặn.
    """
    # Đảm bảo URL bắt đầu bằng https://
    if not page_url.startswith('http'):
        page_url = 'https://' + page_url.lstrip('/')
    
    logger.info(f"Starting crawl for URL: {page_url}")
    
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
                current_url = f"{page_url}?page={page_num}" if '?' not in page_url else f"{page_url}&page={page_num}"
                if page_num == 1:
                    current_url = page_url  # Sử dụng URL gốc cho page 1
                
                logger.info(f"Crawling page {page_num}: {current_url}")
                
                try:
                    # Điều hướng đến trang
                    for retry_attempt in range(3):  # Thử tối đa 3 lần
                        try:
                            # Tăng timeout lên 60s thay vì 30s
                            await page.goto(current_url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
                            await page.wait_for_selector('div.job-item-2', timeout=SELECTOR_TIMEOUT)
                            break  # Thoát khỏi vòng lặp retry nếu thành công
                        except Exception as e:
                            if retry_attempt == 2:  # Nếu là lần thử cuối cùng
                                logger.error(f"Error loading page after 3 attempts: {str(e)}")
                                raise  # Ném lỗi để khớp với xử lý hiện tại
                            else:
                                logger.warning(f"Error loading page (attempt {retry_attempt+1}/3), retrying: {str(e)}")
                                await asyncio.sleep(5)  # Đợi 5 giây trước khi thử lại
                except Exception as e:
                    logger.error(f"Error loading page: {str(e)}")
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
                    logger.info(f"Found {len(job_elements)} job listings on page {page_num}")
                    
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
                                    logger.info(f"Updating changed job ID: {job_id}")
                                    # Sử dụng posted_time cũ nếu có
                                    if 'posted_time' in existing_job and 'posted_time' not in job_data:
                                        job_data['posted_time'] = existing_job['posted_time']
                                    # Cập nhật tracking data
                                    job_data_crawled[job_id] = tracking_data
                                    all_jobs.append(job_data)
                                else:
                                    logger.info(f"Skipping unchanged job ID: {job_id}")
                            else:
                                logger.info(f"Found new job ID: {job_id}")
                                job_id_crawled.add(job_id)
                                job_data_crawled[job_id] = tracking_data
                                all_jobs.append(job_data)
                
                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {str(e)}")
                    # Tiếp tục với page tiếp theo thay vì dừng hoàn toàn
                    continue
                
                # Độ trễ giữa các trang
                await asyncio.sleep(random.uniform(MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES))
                
            # Lưu dữ liệu theo dõi job_id (do đang ở bên ngoài lock nên không cần lock lại)
            save_crawled_data()
            
            logger.info(f"Successfully crawled {len(all_jobs)} jobs")
            return all_jobs
                
        except Exception as e:
            logger.error(f"Error during crawling: {str(e)}")
            with job_data_lock:
                save_crawled_data()
            return []
        finally:
            await browser.close()

def crawl_jobs(num_pages=5):
    """
    Hàm công khai để thực hiện crawl dữ liệu việc làm từ TopCV.
    
    Args:
        num_pages (int): Số trang cần crawl từ base URL. Mặc định là 5.
                                       
    Returns:
        pandas.DataFrame: DataFrame chứa thông tin các công việc đã crawl
        
    Notes:
        - Hàm này gọi phương thức bất đồng bộ crawl_job_listings bên trong và trả về kết quả
        - Kết quả là một DataFrame với các cột: title, company, location, salary, v.v.
    """
    logger.info(f"Starting job crawling process with {num_pages} pages")
    
    # Gọi hàm async crawl_job_listings
    all_jobs = asyncio.run(crawl_job_listings(BASE_URL, num_pages))
    
    # Chuyển sang DataFrame
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        # Loại bỏ trùng lặp theo job_id
        df = df.drop_duplicates(subset=['job_id'])
        logger.info(f"Created DataFrame with {len(df)} unique jobs")
        return df
    else:
        logger.warning("No jobs were crawled")
        return pd.DataFrame()

def get_job_listing(num_pages=5):
    """
    Lấy danh sách công việc từ base URL.
    
    Args:
        num_pages (int): Số trang cần lấy. Mặc định là 5.
        
    Returns:
        pandas.DataFrame: DataFrame chứa danh sách công việc
    """
    logger.info(f"Getting job listings for {num_pages} pages")
    return crawl_jobs(num_pages)

async def crawl_job_details(job_url):
    """
    Crawl thông tin chi tiết của một công việc cụ thể.
    
    Args:
        job_url (str): URL chi tiết của công việc
        
    Returns:
        Dict[str, Any]: Thông tin chi tiết của công việc
    """
    logger.info(f"Crawling job details for: {job_url}")
    
    # Kiểm tra URL
    if not job_url.startswith("https://"):
        job_url = f"https://www.topcv.vn{job_url}" if not job_url.startswith("/") else f"https://www.topcv.vn{job_url}"
    
    async with async_playwright() as p:
        user_agent = random.choice(USER_AGENTS)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()
        
        try:
            await page.goto(job_url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            
            # Đợi cho các phần tử chính hiện ra
            await page.wait_for_selector('.job-detail', timeout=SELECTOR_TIMEOUT)
            
            # Trích xuất thông tin chi tiết
            job_detail = {}
            
            # Tiêu đề công việc
            title_el = await page.query_selector('.job-detail h1.job-title')
            if title_el:
                job_detail['title'] = await title_el.inner_text()
            
            # Thông tin công ty
            company_el = await page.query_selector('.job-detail .company-name')
            if company_el:
                job_detail['company'] = await company_el.inner_text()
            
            # Mô tả công việc
            job_desc_el = await page.query_selector('.job-detail .job-description')
            if job_desc_el:
                job_detail['description'] = await job_desc_el.inner_text()
            
            # Yêu cầu
            job_req_el = await page.query_selector('.job-detail .job-requirements')
            if job_req_el:
                job_detail['requirements'] = await job_req_el.inner_text()
            
            # Các thông tin khác như lương, địa điểm, v.v.
            
            logger.info(f"Successfully crawled details for job: {job_detail.get('title', 'Unknown')}")
            return job_detail
            
        except Exception as e:
            logger.error(f"Error crawling job details: {str(e)}")
            return None
        finally:
            await browser.close()

if __name__ == "__main__":
    # Chạy crawl và trả về DataFrame
    start_time = time.time()
    
    # Crawl 5 trang từ BASE_URL
    df_results = crawl_jobs(num_pages=5)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    if not df_results.empty:
        print(f"Crawled {len(df_results)} job listings in {elapsed_time:.2f} seconds")
        print(f"Average time per job: {elapsed_time/len(df_results):.2f} seconds")
        print(f"Total pages crawled: 5")
        # DataFrame có thể được sử dụng để import vào database ở bước tiếp theo 