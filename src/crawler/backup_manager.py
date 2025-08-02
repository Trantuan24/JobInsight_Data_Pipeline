#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module quản lý việc backup HTML từ TopCV với đa luồng
"""

import os
import sys
import asyncio
import concurrent.futures
import random
from datetime import datetime
from pathlib import Path
import time
from typing import Dict, List, Any, Optional, Union
import traceback


# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config
    from src.utils.path_helpers import ensure_path, ensure_dir, get_timestamp_filename
    from src.utils.retry import async_retry
    from src.utils.user_agent_manager import UserAgentManager
    from src.crawler.captcha_handler import CaptchaHandler

    from playwright.async_api import async_playwright
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    
    # Fallback Config & path_helpers
    class Config:
        class Crawler:
            BASE_URL = "https://www.topcv.vn/viec-lam-it"
            PAGE_LOAD_TIMEOUT = 60000
            SELECTOR_TIMEOUT = 20000
            MIN_DELAY = 3  # Giảm từ 6s xuống 3s để tăng tốc
            MAX_DELAY = 6  # Giảm từ 10s xuống 6s để tăng tốc
            MAX_RETRY = 3
            RETRY_DELAYS = [2, 4, 8]
        class Dirs:
            BACKUP_DIR = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "raw_backup"))
        class Threading:
            MAX_WORKERS = min(32, (os.cpu_count() or 1) * 5)
    
    def ensure_path(path):
        from pathlib import Path
        if isinstance(path, str):
            return Path(path)
        return path
    
    def ensure_dir(path):
        dir_path = ensure_path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path
    
    def get_timestamp_filename(base_dir, prefix, suffix, timestamp_format="%Y%m%d%H%M%S"):
        from pathlib import Path
        timestamp = datetime.now().strftime(timestamp_format)
        filename = f"{prefix}_{timestamp}{suffix}"
        return ensure_path(base_dir) / filename
    
    def async_retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    from src.utils.user_agent_manager import UserAgentManager
    from src.crawler.captcha_handler import CaptchaHandler
    from playwright.async_api import async_playwright

logger = get_logger("crawler.backup_manager")

class HTMLBackupManager:
    """
    Class quản lý việc backup HTML từ website
    - Hỗ trợ đa luồng để backup nhiều trang cùng lúc
    - Tích hợp UserAgentManager để quản lý user-agent
    - Tích hợp CaptchaHandler để xử lý block/captcha
    - Sử dụng semaphore để kiểm soát số lượng request đồng thời
    """
    
    def __init__(self, config=None):
        """
        Khởi tạo HTMLBackupManager
        
        Args:
            config: Config dict cho backup manager
        """
        self.config = config or {}
        
        # Khởi tạo các thành phần con
        self.ua_manager = UserAgentManager.from_config()
        self.captcha_handler = CaptchaHandler()
        
        # Lấy config từ input hoặc dùng default
        self.base_url = self.config.get('base_url', Config.Crawler.BASE_URL)
        self.page_load_timeout = self.config.get('page_load_timeout', Config.Crawler.PAGE_LOAD_TIMEOUT)
        self.selector_timeout = self.config.get('selector_timeout', Config.Crawler.SELECTOR_TIMEOUT)
        self.min_delay = self.config.get('min_delay', Config.Crawler.MIN_DELAY)
        self.max_delay = self.config.get('max_delay', Config.Crawler.MAX_DELAY)
        self.max_retry = self.config.get('max_retry', Config.Crawler.MAX_RETRY)
        self.retry_delays = self.config.get('retry_delays', Config.Crawler.RETRY_DELAYS)
        
        # Config đa luồng
        self.max_workers = self.config.get('max_workers', Config.Threading.MAX_WORKERS)
        self.concurrent_backups = self.config.get('concurrent_backups',
                                                min(5, max(3, (os.cpu_count() or 1))))
        
        # Backup directory - sử dụng pathlib.Path
        self.backup_dir = ensure_dir(self.config.get('backup_dir', Config.Dirs.BACKUP_DIR))
        
        # Semaphore để kiểm soát số lượng concurrent requests
        self._semaphore = None  # Sẽ được khởi tạo khi backup_html_pages được gọi
        
        logger.info(f"Khởi tạo HTMLBackupManager với {self.max_workers} workers và {self.concurrent_backups} concurrent backups")
        logger.info(f"Backup directory: {self.backup_dir}")
    
    @async_retry(max_tries=3, backoff_factor=2.0)
    async def _backup_page_impl(self, page_num: int, user_agent: str, viewport: Dict[str, int]) -> Dict[str, Any]:
        """
        Implementation của backup trang, được wrap bởi retry decorator
        
        Args:
            page_num: Số trang cần backup
            user_agent: User-agent cần sử dụng
            viewport: Viewport cần sử dụng
            
        Returns:
            Dict: Kết quả backup
        """
        # Tạo URL cho page
        url = f"{self.base_url}?page={page_num}" if page_num > 1 else self.base_url
        
        async with async_playwright() as p:
            browser = None
            try:
                # Mở browser mới cho mỗi page (session mới)
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=user_agent,
                    viewport=viewport,
                )
                
                page = await context.new_page()
                
                # Anti-detection
                await self.captcha_handler.apply_anti_detection(page)
                
                logger.info(f"Backing up page {page_num}")
                
                # Navigate và đợi content
                await page.goto(url, wait_until='domcontentloaded', timeout=self.page_load_timeout)
                
                try:
                    await page.wait_for_selector('div.job-item-2', timeout=self.selector_timeout)
                except Exception as e:
                    logger.warning(f"Selector not found on page {page_num}: {str(e)}")
                    
                    # Lấy HTML hiện tại để kiểm tra captcha/block
                    html_content = await page.content()
                    
                    if self.captcha_handler.detect_captcha(html_content):
                        # Xử lý captcha/block
                        success, info = await self.captcha_handler.handle_captcha(page)
                        if not success:
                            raise Exception(f"Phát hiện captcha/block: {info}")
                
                # Scroll để load hết content
                await page.evaluate("""
                    () => {
                        return new Promise((resolve) => {
                            // Scroll đến giữa trang
                            window.scrollTo(0, document.body.scrollHeight / 2);
                            
                            // Sau 1s, tiếp tục scroll đến cuối trang
                            setTimeout(() => {
                                window.scrollTo(0, document.body.scrollHeight);
                                
                                // Đợi thêm 1s cho các lazy content load
                                setTimeout(resolve, 1000);
                            }, 1000);
                        });
                    }
                """)
                
                # Lấy HTML content
                html_content = await page.content()
                
                # Tạo filename với timestamp sử dụng path_helpers
                filename = get_timestamp_filename(
                    self.backup_dir,
                    f"it_p{page_num}",
                    ".html"
                )
                
                # Lưu file
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                logger.info(f"Backed up page {page_num} to {filename}")
                
                return {
                    "success": True, 
                    "page": page_num,
                    "filename": str(filename),
                    "timestamp": datetime.now().isoformat(),
                    "size_bytes": len(html_content)
                }
                
            finally:
                if browser:
                    await browser.close()
    
    async def backup_single_page(self, page_num: int) -> Dict[str, Any]:
        """
        Backup HTML của một trang với session mới và retry logic

        Args:
            page_num: Số trang cần backup

        Returns:
            Dict: Kết quả backup {'success': bool, 'filename': str, 'page': int, ...}
        """
        # Random delay trước mỗi page để tránh pattern detection
        if page_num > 1:  # Không delay cho page đầu tiên
            pre_delay = random.uniform(1, 3)  # 1-3 giây random delay
            await asyncio.sleep(pre_delay)

        # Lấy random user-agent và viewport
        user_agent = self.ua_manager.get_random_agent()
        viewport = self.ua_manager.get_viewport(user_agent)

        try:
            # Sử dụng hàm đã được wrap với retry decorator
            return await self._backup_page_impl(page_num, user_agent, viewport)
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Failed to backup page {page_num} after retries: {error_msg}")
            logger.debug(traceback.format_exc())
            return {
                "success": False,
                "error": error_msg,
                "error_type": type(e).__name__,
                "page": page_num
            }
    
    async def backup_html_pages_sequential(self, num_pages=5) -> List[Dict[str, Any]]:
        """
        Backup HTML của các trang tuần tự
        
        Args:
            num_pages: Số trang cần backup
            
        Returns:
            List[Dict]: Danh sách kết quả backup
        """
        logger.info(f"Starting sequential HTML backup for {num_pages} pages")
        results = []
        
        for page_num in range(1, num_pages + 1):
            result = await self.backup_single_page(page_num)
            results.append(result)
            
            # Delay giữa các page
            if page_num < num_pages:
                delay = random.uniform(self.min_delay, self.max_delay)
                logger.info(f"Waiting {delay:.1f}s before next page...")
                await asyncio.sleep(delay)
        
        successful = sum(1 for r in results if r.get("success", False))
        logger.info(f"Sequential backup completed: {successful}/{num_pages} pages successful")
        
        return results
    
    async def backup_html_pages_parallel(self, num_pages=5) -> List[Dict[str, Any]]:
        """
        Backup HTML của các trang song song (đa luồng) với semaphore để kiểm soát số lượng concurrent requests
        
        Args:
            num_pages: Số trang cần backup
            
        Returns:
            List[Dict]: Danh sách kết quả backup
        """
        logger.info(f"Starting parallel HTML backup for {num_pages} pages with {self.concurrent_backups} concurrent tasks")
        
        # Khởi tạo semaphore để giới hạn số lượng concurrent tasks
        self._semaphore = asyncio.Semaphore(self.concurrent_backups)
        
        async def backup_with_semaphore(page_num):
            """Wrapper với semaphore"""
            async with self._semaphore:
                return await self.backup_single_page(page_num)
        
        # Tạo tasks cho tất cả các trang
        tasks = [backup_with_semaphore(page_num) for page_num in range(1, num_pages + 1)]
        
        # Chạy tất cả tasks và đợi kết quả
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Xử lý exceptions nếu có
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_msg = f"{type(result).__name__}: {str(result)}"
                logger.error(f"Error backing up page {i+1}: {error_msg}")
                logger.debug(traceback.format_exc())
                processed_results.append({
                    "success": False,
                    "error": error_msg,
                    "error_type": type(result).__name__,
                    "page": i+1
                })
            else:
                processed_results.append(result)
        
        successful = sum(1 for r in processed_results if r.get("success", False))
        failed = num_pages - successful

        # Circuit Breaker: Nếu quá nhiều pages bị block, pause để tránh bị ban
        if failed >= 3 and num_pages >= 4:  # Nếu >= 3 pages fail trong batch >= 4 pages
            logger.warning(f"Circuit Breaker triggered: {failed}/{num_pages} pages failed. Pausing 5 minutes to avoid IP ban...")
            await asyncio.sleep(300)  # Pause 5 minutes
            logger.info("Circuit Breaker: Resuming after pause")

        logger.info(f"Parallel backup completed: {successful}/{num_pages} pages successful")

        return processed_results
    
    async def backup_html_pages(self, num_pages=5, parallel=True) -> List[Dict[str, Any]]:
        """
        Backup HTML của các trang (wrapper function)
        
        Args:
            num_pages: Số trang cần backup
            parallel: True để chạy song song, False để chạy tuần tự
            
        Returns:
            List[Dict]: Danh sách kết quả backup
        """
        if parallel:
            return await self.backup_html_pages_parallel(num_pages)
        else:
            return await self.backup_html_pages_sequential(num_pages)


# Production module - test code removed