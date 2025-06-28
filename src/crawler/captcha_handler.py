#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module xử lý captcha và anti-block cho crawler
"""

import os
import sys
import re
import time
import random
import asyncio
from typing import Tuple, Dict, Any, Optional


# Import modules
try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

logger = get_logger("crawler.captcha_handler")

class CaptchaHandler:
    """
    Class xử lý captcha và các biện pháp chống bot
    - Phát hiện captcha/block trong nội dung HTML
    - Chiến lược xử lý khi bị block
    - Xoay vòng IP/user-agent
    """
    
    def __init__(self):
        """Khởi tạo CaptchaHandler"""
        self.captcha_patterns = [
            r'captcha',
            r'robot', 
            r'automated\s+access', 
            r'unusual\s+traffic',
            r'suspicious\s+activity',
            r'access\s+denied',
            r'security\s+check',
            r'please\s+verify',
            r'recaptcha'
        ]
        self.captcha_regex = re.compile('|'.join(self.captcha_patterns), re.IGNORECASE)
        
        self.retry_count = 0
        self.max_retries = 3
        self.delay_between_retries = [2, 5, 10]  # Thời gian delay tăng dần
        
        logger.info("Khởi tạo CaptchaHandler")
    
    def detect_captcha(self, html_content: str) -> bool:
        """
        Phát hiện captcha hoặc block trong nội dung HTML
        
        Args:
            html_content: Nội dung HTML cần kiểm tra
            
        Returns:
            bool: True nếu phát hiện captcha/block
        """
        if not html_content:
            logger.warning("HTML content rỗng, không thể kiểm tra captcha")
            return False
        
        # Kiểm tra bằng regex
        if self.captcha_regex.search(html_content):
            logger.warning("Phát hiện captcha/block pattern trong nội dung HTML")
            return True
        
        # Kiểm tra nội dung cực ngắn (thường là bị block)
        if len(html_content) < 1000:
            logger.warning(f"Nội dung HTML quá ngắn ({len(html_content)} bytes), có thể bị block")
            return True
            
        # Kiểm tra các yếu tố quan trọng trong page (tùy website)
        if "<div class='job-item-2'" not in html_content and "job-item-2" in html_content:
            logger.warning("Không tìm thấy job-item-2 div trong content, có thể bị block")
            return True
            
        return False
    
    async def handle_captcha(self, page) -> Tuple[bool, Dict[str, Any]]:
        """
        Xử lý captcha/block
        
        Args:
            page: Playwright page object
            
        Returns:
            Tuple[bool, Dict]: (Thành công hay không, thông tin thêm)
        """
        # Tăng retry count
        self.retry_count += 1
        
        # Nếu đã retry quá nhiều lần, từ bỏ
        if self.retry_count > self.max_retries:
            logger.error(f"Đã retry {self.retry_count}/{self.max_retries} lần, từ bỏ")
            return False, {"error": "max_retries_exceeded", "retries": self.retry_count}
        
        # Quyết định thời gian delay
        delay = self.delay_between_retries[min(self.retry_count - 1, len(self.delay_between_retries) - 1)]
        actual_delay = delay + random.uniform(0, 2)  # Thêm random để tránh pattern
        
        logger.info(f"Xử lý captcha/block, retry {self.retry_count}/{self.max_retries}, delay {actual_delay:.2f}s")
        
        try:
            # Thực hiện các biện pháp anti-block
            
            # 1. Thay đổi fingerprint
            await page.evaluate("""
                () => {
                    // Thay đổi userAgent
                    Object.defineProperty(navigator, 'userAgent', {
                        get: () => Math.random().toString(36).substring(2, 15)
                    });
                    
                    // Thay đổi platform
                    Object.defineProperty(navigator, 'platform', {
                        get: () => ['Win32', 'MacIntel', 'Linux x86_64'][Math.floor(Math.random() * 3)]
                    });
                    
                    // Thay đổi language
                    Object.defineProperty(navigator, 'language', {
                        get: () => ['en-US', 'vi-VN', 'en-GB'][Math.floor(Math.random() * 3)]
                    });
                }
            """)
            
            # 2. Thêm delay
            logger.info(f"Delay {actual_delay:.2f}s để tránh block")
            await asyncio.sleep(actual_delay)
            
            # 3. Thêm scroll behavior như người dùng thật
            await page.evaluate("""
                () => {
                    const scrollAmount = window.innerHeight / 2;
                    const scrollSteps = 5;
                    const scrollDelay = 300;
                    
                    return new Promise((resolve) => {
                        let currentStep = 0;
                        
                        const scrollStep = () => {
                            if (currentStep >= scrollSteps) {
                                resolve();
                                return;
                            }
                            
                            window.scrollBy(0, scrollAmount / scrollSteps);
                            currentStep += 1;
                            
                            setTimeout(scrollStep, scrollDelay);
                        };
                        
                        scrollStep();
                    });
                }
            """)
            
            # 4. Cố gắng thay đổi cookie
            await page.evaluate("""
                () => {
                    // Xoá cookie hiện có
                    document.cookie.split(";").forEach(function(c) {
                        document.cookie = c.replace(/^ +/, "").replace(/=.*/, "=;expires=" + new Date().toUTCString() + ";path=/");
                    });
                }
            """)
            
            return True, {"retry": self.retry_count, "delay": actual_delay}
            
        except Exception as e:
            logger.error(f"Lỗi xử lý captcha: {str(e)}")
            return False, {"error": str(e), "retry": self.retry_count}
    
    def reset(self):
        """Reset retry count và trạng thái"""
        self.retry_count = 0
        
    async def apply_anti_detection(self, page):
        """
        Áp dụng các kỹ thuật chống phát hiện bot
        
        Args:
            page: Playwright page object
        """
        try:
            # Thêm các script chống phát hiện
            await page.add_init_script("""
                () => {
                    // Ẩn webdriver
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false
                    });
                    
                    // Giả lập các thuộc tính như chrome
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {},
                    };
                    
                    // Giả lập permissions
                    if (!window.Notification) {
                        window.Notification = {
                            permission: 'denied'
                        };
                    }
                    
                    // Override plugins
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => {
                            const plugins = [];
                            for (let i = 0; i < 5; i++) {
                                plugins.push({
                                    name: `Plugin ${i}`,
                                    description: `Plugin description ${i}`,
                                    filename: `plugin_${i}.dll`
                                });
                            }
                            return plugins;
                        }
                    });
                    
                    // Giả lập ngôn ngữ
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['vi-VN', 'vi', 'en-US', 'en']
                    });
                }
            """)
        
        except Exception as e:
            logger.error(f"Lỗi khi áp dụng anti-detection: {str(e)}")


# Test the class if run directly
if __name__ == "__main__":
    import asyncio
    
    # Tạo event loop để test các async functions
    loop = asyncio.get_event_loop()
    
    handler = CaptchaHandler()
    
    # Test captcha detection
    html_with_captcha = "<html><body>Please complete this security check to access the website</body></html>"
    html_normal = "<html><body><div class='job-item-2'>Job listing</div></body></html>"
    
    print(f"Detect captcha in test html: {handler.detect_captcha(html_with_captcha)}")
    print(f"Detect captcha in normal html: {handler.detect_captcha(html_normal)}")
    
    # Các async function cần event loop để chạy, không thực sự test được mà không có page object 