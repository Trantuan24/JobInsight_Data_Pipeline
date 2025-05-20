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
        KEYWORDS, USER_AGENTS, PAGE_LOAD_TIMEOUT, SELECTOR_TIMEOUT,
        MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES,
        MIN_DELAY_BETWEEN_GROUPS, MAX_DELAY_BETWEEN_GROUPS,
        MAX_CONCURRENT_BROWSERS, GROUP_TIMEOUT, BASE_URL
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
        KEYWORDS, USER_AGENTS, PAGE_LOAD_TIMEOUT, SELECTOR_TIMEOUT,
        MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES,
        MIN_DELAY_BETWEEN_GROUPS, MAX_DELAY_BETWEEN_GROUPS,
        MAX_CONCURRENT_BROWSERS, GROUP_TIMEOUT, BASE_URL
    )
    from crawler_utils import (
        job_data_lock, job_id_crawled, job_data_crawled,
        save_crawled_data, random_delay
    )
    from data_extractor import (
        extract_job_data, extract_tracking_data, has_job_updated
    )

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
    
    logger.info(f"Starting crawl for URL: {final_url}")
    
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
            
            logger.info(f"Successfully crawled {len(all_jobs)} jobs from '{keyword or 'no keyword'}'")
            return all_jobs
                
        except Exception as e:
            logger.error(f"Error during crawling: {str(e)}")
            with job_data_lock:
                save_crawled_data()
            return []
        finally:
            await browser.close()

async def crawl_keyword(base_url, keyword, num_pages):
    """Hàm helper để chạy crawler cho một keyword"""
    try:
        logger.info(f"Crawling for keyword: {keyword}")
        results = await crawl_job_listings(base_url, keyword, num_pages)
        logger.info(f"Completed crawling for keyword '{keyword}', found {len(results)} jobs")
        return results
    except Exception as e:
        logger.error(f"Error in crawl_keyword for '{keyword}': {str(e)}")
        return []

async def async_crawl_multiple_keywords(base_url="https://www.topcv.vn/viec-lam-it", keywords=None, num_pages=1):
    """Chạy crawler cho nhiều keywords đồng thời (với giới hạn đồng thời)"""
    if not keywords:
        keywords = KEYWORDS
    
    logger.info(f"Starting async multi-keyword crawl with {len(keywords)} keywords")
    
    all_jobs = []
    
    # Crawl trang gốc trước (không keyword)
    logger.info(f"Crawling base URL without keywords: {base_url}")
    base_results = await crawl_job_listings(base_url, keyword=None, num_pages=num_pages)
    all_jobs.extend(base_results)
    logger.info(f"Found {len(base_results)} jobs from base URL (no keyword)")
    
    # Đợi một chút trước khi tiếp tục với các keyword
    await asyncio.sleep(5)  # Tăng thời gian chờ từ 3s lên 5s
    
    # Chia keywords thành các nhóm nhỏ hơn để tránh quá tải
    # Giảm số lượng concurrent xuống còn 2 nếu có nhiều keyword
    max_concurrent = 2 if len(keywords) > 5 else MAX_CONCURRENT_BROWSERS
    keyword_groups = [keywords[i:i+max_concurrent] for i in range(0, len(keywords), max_concurrent)]
    
    # Xử lý từng nhóm keywords
    failed_keywords = []
    for group_idx, group in enumerate(keyword_groups):
        logger.info(f"Processing keyword group {group_idx+1}/{len(keyword_groups)}: {', '.join(group)}")
        
        # Tạo task cho mỗi keyword trong nhóm
        tasks = []
        for keyword in group:
            try:
                task = crawl_keyword(base_url, keyword, num_pages)
                tasks.append(task)
            except Exception as e:
                logger.error(f"Error creating task for keyword '{keyword}': {str(e)}")
                failed_keywords.append(keyword)
        
        if not tasks:
            logger.warning(f"No valid tasks in group {group_idx+1}, skipping")
            continue
            
        try:
            # Đặt timeout cho cả nhóm - nếu một keyword nào đó mất quá nhiều thời gian
            group_timeout = 180  # 3 phút cho mỗi nhóm keyword
            group_results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 
                                                 timeout=GROUP_TIMEOUT)
            
            # Xử lý kết quả của mỗi keyword
            for idx, results in enumerate(group_results):
                if idx >= len(group):
                    continue  # Phòng ngừa lỗi index
                    
                keyword = group[idx]
                if isinstance(results, Exception):
                    logger.error(f"Failed to crawl keyword '{keyword}': {str(results)}")
                    failed_keywords.append(keyword)
                    continue
                
                # Nếu thành công thì add vào kết quả
                all_jobs.extend(results)
                logger.info(f"Added {len(results)} jobs from keyword '{keyword}'")
        
        except asyncio.TimeoutError:
            logger.error(f"Timeout processing keyword group {group_idx+1}")
            # Đánh dấu tất cả keyword trong nhóm này là thất bại
            failed_keywords.extend(group)
            
        except Exception as e:
            logger.error(f"Error processing keyword group {group_idx+1}: {str(e)}")
            failed_keywords.extend(group)
        
        # Đợi lâu hơn giữa các nhóm để tránh quá tải
        if len(keyword_groups) > 1 and group_idx < len(keyword_groups) - 1:
            wait_time = random.uniform(10, 15)  # 10-15 giây giữa các nhóm
            logger.info(f"Waiting {wait_time:.1f}s before processing next keyword group")
            await asyncio.sleep(wait_time)
    
    # Log các keyword thất bại nếu có
    if failed_keywords:
        logger.warning(f"Failed to crawl {len(failed_keywords)} keywords: {', '.join(failed_keywords)}")
    
    # Lọc trùng lặp trước khi trả về
    unique_job_ids = set()
    filtered_jobs = []
    
    for job in all_jobs:
        if job['job_id'] not in unique_job_ids:
            unique_job_ids.add(job['job_id'])
            filtered_jobs.append(job)
    
    logger.info(f"Completed async crawl with {len(filtered_jobs)} unique jobs from {len(all_jobs)} total")
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
        logger.info(f"Created DataFrame with {len(df)} unique jobs")
        return df
    else:
        logger.warning("No data collected from any keyword!")
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