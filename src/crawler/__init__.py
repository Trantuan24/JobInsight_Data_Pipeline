"""
Job Crawler module for JobInsight
"""

# Thư mục crawler chứa các module liên quan đến việc crawl dữ liệu

from src.crawler.crawler_utils import parse_last_update, log_action
from src.crawler.backup import backup_html_pages, backup_single_page
from src.crawler.parser import parse_html_files, extract_job_data
from src.crawler.crawler import crawl_jobs, get_job_listing

__all__ = [
    'parse_last_update',
    'log_action',
    'backup_html_pages',
    'backup_single_page',
    'parse_html_files',
    'extract_job_data',
    'crawl_jobs',
    'get_job_listing'
]
