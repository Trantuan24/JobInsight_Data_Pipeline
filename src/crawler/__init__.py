"""
Job Crawler module for JobInsight
"""

# Thư mục crawler chứa các module liên quan đến việc crawl dữ liệu

from src.crawler.crawler_utils import parse_last_update
from src.crawler.backup_manager import HTMLBackupManager
from src.crawler.parser import TopCVParser
from src.crawler.captcha_handler import CaptchaHandler
from src.crawler.crawler import TopCVCrawler

__all__ = [
    'parse_last_update',
    'HTMLBackupManager',
    'TopCVParser',
    'CaptchaHandler',
    'TopCVCrawler'
]
