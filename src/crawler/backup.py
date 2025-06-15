from playwright.async_api import async_playwright
import asyncio
import random
from datetime import datetime
import os
import sys

# Thêm đường dẫn gốc dự án vào sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import modules
try:
    from src.utils.logger import get_logger
    from src.crawler.crawler_utils import log_action
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    from src.crawler.crawler_utils import log_action

from src.crawler.crawler_config import (
    USER_AGENTS, PAGE_LOAD_TIMEOUT, SELECTOR_TIMEOUT,
    MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES,
    BASE_URL, BACKUP_DIR, MAX_RETRY, RETRY_DELAYS, VIEWPORTS
)

logger = get_logger("crawler.backup")

async def backup_single_page(page_num):
    """Backup HTML của một trang với session mới"""
    user_agent = random.choice(USER_AGENTS)
    is_mobile = "Mobile" in user_agent or "Android" in user_agent or "iPhone" in user_agent
    viewport = VIEWPORTS["mobile"] if is_mobile else VIEWPORTS["desktop"]
    
    for attempt in range(1, MAX_RETRY + 1):
        try:
            async with async_playwright() as p:
                # Mở browser mới cho mỗi page (session mới)
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=user_agent,
                    viewport=viewport,
                )
                
                page = await context.new_page()
                
                # Anti-detection
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });
                """)
                
                # Tạo URL cho page
                url = f"{BASE_URL}?page={page_num}" if page_num > 1 else BASE_URL
                
                logger.info(f"Backing up page {page_num} (attempt {attempt}/{MAX_RETRY})")
                log_action("backup_start", page_num, "info", attempt=attempt, user_agent=user_agent)
                
                # Navigate và đợi content
                await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
                await page.wait_for_selector('div.job-item-2', timeout=SELECTOR_TIMEOUT)
                
                # Scroll để load hết content
                await page.evaluate("""
                    () => {
                        window.scrollTo(0, document.body.scrollHeight / 2);
                        setTimeout(() => {
                            window.scrollTo(0, document.body.scrollHeight);
                        }, 1000);
                    }
                """)
                
                await asyncio.sleep(2)
                
                # Lấy HTML content
                html_content = await page.content()
                
                # Tạo filename với timestamp
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = BACKUP_DIR / f"it_p{page_num}_{timestamp}.html"
                
                # Lưu file
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                await browser.close()
                
                log_action("backup_ok", page_num, "success", attempt=attempt, user_agent=user_agent)
                logger.info(f"Backed up page {page_num} to {filename}")
                
                return {"success": True, "filename": str(filename), "page": page_num}
                
        except Exception as e:
            error_msg = str(e)
            log_action("backup_error", page_num, "error", error=error_msg, attempt=attempt, user_agent=user_agent)
            
            if attempt < MAX_RETRY:
                delay = RETRY_DELAYS[attempt - 1]
                logger.warning(f"Error on page {page_num} (attempt {attempt}), retrying in {delay}s: {error_msg}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to backup page {page_num} after {MAX_RETRY} attempts: {error_msg}")
                return {"success": False, "error": error_msg, "page": page_num}
            
        finally:
            if 'browser' in locals():
                await browser.close()

async def backup_html_pages(num_pages=5):
    """Backup HTML của các trang theo thứ tự"""
    logger.info(f"Starting HTML backup for {num_pages} pages")
    results = []
    
    for page_num in range(1, num_pages + 1):
        result = await backup_single_page(page_num)
        results.append(result)
        
        # Delay giữa các page
        if page_num < num_pages:
            delay = random.uniform(MIN_DELAY_BETWEEN_PAGES, MAX_DELAY_BETWEEN_PAGES)
            logger.info(f"Waiting {delay:.1f}s before next page...")
            await asyncio.sleep(delay)
    
    successful = sum(1 for r in results if r["success"])
    logger.info(f"Backup completed: {successful}/{num_pages} pages successful")
    
    return results 