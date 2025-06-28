"""
Job Crawler module for JobInsight
"""

# Thư mục crawler chứa các module liên quan đến việc crawl dữ liệu

from src.crawler.crawler_utils import parse_last_update, log_action
from src.crawler.backup_manager import HTMLBackupManager
from src.crawler.parser import TopCVParser, parse_html_files
from src.crawler.captcha_handler import CaptchaHandler
from src.crawler.crawler import TopCVCrawler, backup_html_pages

__all__ = [
    'parse_last_update',
    'log_action',
    'HTMLBackupManager',
    'TopCVParser',
    'CaptchaHandler',
    'TopCVCrawler',
    'backup_html_pages',
    'parse_html_files'
]
