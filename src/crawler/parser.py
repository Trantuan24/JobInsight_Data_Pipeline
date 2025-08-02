#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module xử lý parse HTML thành dữ liệu có cấu trúc
"""

from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import datetime
import threading
import os
import sys
from pathlib import Path
import glob
from typing import Dict, List, Any, Optional, Union, Set
import concurrent.futures
import traceback
import re


# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.config import Config
    from src.utils.path_helpers import ensure_path, glob_files
    from src.utils.retry import retry
    from src.crawler.crawler_utils import parse_last_update
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    
    # Fallback config
    class Config:
        class Dirs:
            BACKUP_DIR = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "raw_backup"))
        class Threading:
            MAX_WORKERS = min(32, (os.cpu_count() or 1) * 5)
    
    # Fallback path_helpers
    def ensure_path(path):
        if isinstance(path, str):
            return Path(path)
        return path
    
    def glob_files(directory, pattern):
        directory = ensure_path(directory)
        return sorted(directory.glob(pattern))
    
    # Fallback retry
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    # Fallback parse_last_update
    def parse_last_update(update_text):
        return 0

logger = get_logger("crawler.parser")

class TopCVParser:
    """
    Class parse HTML từ TopCV thành dữ liệu có cấu trúc
    - Parse từng file HTML thành dict data
    - Parse nhiều file và trả về DataFrame
    - Áp dụng OOP thay vì global functions
    - Hỗ trợ đa luồng với ThreadPoolExecutor
    """
    
    def __init__(self, backup_dir=None, max_workers=None):
        """
        Khởi tạo TopCVParser
        
        Args:
            backup_dir: Đường dẫn đến thư mục chứa file HTML backup
            max_workers: Số lượng worker thread tối đa cho processing đa luồng
        """
        # Sử dụng config mới nếu có, fallback về BACKUP_DIR nếu không
        try:
            self.backup_dir = backup_dir if backup_dir is not None else Config.Dirs.BACKUP_DIR
        except (NameError, AttributeError):
            # Fallback nếu không import được Config
            self.backup_dir = backup_dir if backup_dir is not None else os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                "data", "raw_backup"
            )
            
        # Đảm bảo backup_dir là Path object
        self.backup_dir = ensure_path(self.backup_dir)
        
        # Xác định số lượng worker thread
        try:
            self.max_workers = max_workers if max_workers is not None else Config.Threading.MAX_WORKERS
        except (NameError, AttributeError):
            self.max_workers = max_workers if max_workers is not None else min(
                20, (os.cpu_count() or 1) * 4
            )
        
        # Thread-safe để tracking processed job IDs với memory management
        self._job_id_processed: Set[str] = set()
        self._job_data_lock = threading.Lock()
        self._max_processed_ids = 10000  # Limit để tránh memory leak
        
        logger.info(f"Khởi tạo TopCVParser với backup_dir: {self.backup_dir}, max_workers: {self.max_workers}")

    def _cleanup_processed_ids(self):
        """Clean up processed IDs set để tránh memory leak"""
        current_size = len(self._job_id_processed)
        if current_size > self._max_processed_ids:
            logger.info(f"Memory cleanup triggered: processed IDs set size {current_size} > {self._max_processed_ids}")
            self._job_id_processed.clear()
            logger.info("Processed IDs set cleared to prevent memory leak")

    def find_html_files(self) -> List[Path]:
        """
        Tìm các file HTML trong thư mục backup
        
        Returns:
            List[Path]: Danh sách đường dẫn đến các file HTML
        """
        # Sử dụng hàm glob_files từ path_helpers
        html_files = glob_files(self.backup_dir, "it_p*.html")
        
        logger.info(f"Tìm thấy {len(html_files)} file HTML để parse")
        return html_files
    
    @retry(max_tries=2)
    def parse_html_file(self, html_file: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        Parse một file HTML
        
        Args:
            html_file: Đường dẫn file HTML cần parse
            
        Returns:
            List[Dict]: Danh sách các job data đã parse
        """
        html_file = ensure_path(html_file)
        logger.info(f"Parsing {html_file}")
        parsed_jobs = []
        
        try:
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
                    job_data = self.extract_job_data(job_item)

                    # Debug logging để hiểu parse failures
                    if not job_data['job_id']:
                        logger.debug(f"Job item missing job_id in {html_file.name}")
                    elif not job_data['title']:
                        logger.debug(f"Job item missing title for job_id {job_data['job_id']} in {html_file.name}")

                    if job_data['job_id'] and job_data['title']:
                        # Thêm vào danh sách job đã parse
                        with self._job_data_lock:
                            if job_data['job_id'] not in self._job_id_processed:
                                self._job_id_processed.add(job_data['job_id'])
                                parsed_jobs.append(job_data)

                                # Cleanup nếu set quá lớn
                                if len(self._job_id_processed) > self._max_processed_ids:
                                    self._cleanup_processed_ids()
                    else:
                        logger.debug(f"Skipped job item: job_id={job_data.get('job_id')}, title={job_data.get('title')[:50] if job_data.get('title') else None}")
                        
                except Exception as e:
                    logger.error(f"Error parsing job item in {html_file.name}: {type(e).__name__}: {str(e)}")
                    logger.debug(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error parsing file {html_file}: {type(e).__name__}: {str(e)}")
            logger.debug(traceback.format_exc())
            
        return parsed_jobs
    
    def extract_job_data(self, job_item) -> Dict[str, Any]:
        """
        Extract job data từ một job item HTML
        
        Args:
            job_item: BeautifulSoup object của một job item
            
        Returns:
            Dict: Job data đã extract
        """
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
            'posted_time': None,
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
                    location_detail = tooltip_soup.get_text(strip=True)
                    # Clean up potential CSV-breaking characters
                    location_detail = location_detail.replace('\n', ' ').replace('\r', ' ')
                    location_detail = ' '.join(location_detail.split())  # Normalize whitespace
                    job_data['location_detail'] = location_detail
            
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
                        # Split multiple skills by comma và clean up
                        if ',' in additional_skills:
                            split_skills = [s.strip() for s in additional_skills.split(',')]
                            skills.extend(split_skills)
                        else:
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
            deadline_elem = job_item.find('div', class_='deadline')
            if deadline_elem:
                job_data['deadline'] = deadline_elem.get_text(strip=True)
        
        # Verified employer
        badge_elem = job_item.select_one('span.vip-badge')
        if badge_elem:
            job_data['verified_employer'] = True
        
        # Last update - kết hợp nhiều selector
        # 1. Check label.deadline (từ version cũ)
        update_elem = job_item.select_one('label.deadline')
        if update_elem:
            job_data['last_update'] = update_elem.get_text(strip=True)
        else:
            # 2. Check span.time (current implementation)
            update_elem = job_item.select_one('span.time')
            if update_elem:
                job_data['last_update'] = update_elem.get_text(strip=True)
            else:
                # 3. Fallback: Check div.label-content (từ version cũ)
                label_elems = job_item.find_all('div', class_='label-content')
                for label in label_elems:
                    text = label.get_text(strip=True)
                    if 'Cập nhật' in text:
                        job_data['last_update'] = text.strip()
                        break
        
        # Logo - sử dụng selector theo yêu cầu mới
        logo_elem = job_item.select_one('a img')
        if logo_elem and logo_elem.has_attr('src'):
            job_data['logo_url'] = logo_elem['src']
        else:
            # Fallback: old structure
            logo_elem = job_item.find('img', class_='img-responsive')
            if logo_elem:
                job_data['logo_url'] = logo_elem.get('src', '')
        
        # Posted time - tính toán từ last_update
        if job_data['job_id'] and job_data['last_update']:
            seconds_ago = parse_last_update(job_data['last_update'])
            posted_time = datetime.now().timestamp() - seconds_ago
            job_data['posted_time'] = datetime.fromtimestamp(posted_time).isoformat()
        
        # Validate và clean data trước khi return
        return self._validate_job_data(job_data)

    def _validate_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate và clean job data để tránh CSV issues"""
        # Clean string fields để remove potential CSV-breaking characters
        string_fields = ['title', 'company_name', 'salary', 'location', 'location_detail', 'last_update']
        for field in string_fields:
            if job_data.get(field):
                # Remove newlines và normalize whitespace
                cleaned = str(job_data[field]).replace('\n', ' ').replace('\r', ' ')
                # Replace semicolons with commas để tránh CSV parsing issues
                cleaned = cleaned.replace(';', ',')
                job_data[field] = ' '.join(cleaned.split())

        # Ensure skills is properly formatted list
        if job_data.get('skills') and isinstance(job_data['skills'], list):
            # Clean individual skills
            cleaned_skills = []
            for skill in job_data['skills']:
                if skill and isinstance(skill, str):
                    cleaned_skill = skill.strip().replace('\n', ' ').replace('\r', ' ')
                    if cleaned_skill:  # Only add non-empty skills
                        cleaned_skills.append(cleaned_skill)
            job_data['skills'] = cleaned_skills

        return job_data

    def parse_multiple_files(self, html_files: List[Union[str, Path]] = None) -> pd.DataFrame:
        """
        Parse nhiều file HTML và trả về DataFrame.
        Hỗ trợ xử lý đa luồng với ThreadPoolExecutor.
        
        Args:
            html_files: Danh sách đường dẫn file HTML, nếu None sẽ dùng find_html_files()
            
        Returns:
            pd.DataFrame: DataFrame chứa dữ liệu parsed
        """
        if html_files is None:
            html_files = self.find_html_files()
        
        if not html_files:
            logger.warning("No HTML files found to parse")
            return pd.DataFrame()
        
        logger.info(f"Starting parse of {len(html_files)} HTML files with max_workers={self.max_workers}")
        
        all_jobs = []
        
        # Sử dụng ThreadPoolExecutor để xử lý đa luồng
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tất cả các file để parse
            future_to_file = {executor.submit(self.parse_html_file, file): file for file in html_files}
            
            # Collect kết quả
            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    jobs_from_file = future.result()
                    if jobs_from_file:
                        all_jobs.extend(jobs_from_file)
                        logger.info(f"Parsed {len(jobs_from_file)} jobs from {os.path.basename(str(file))}")
                    else:
                        logger.warning(f"No jobs parsed from {os.path.basename(str(file))}")
                except Exception as e:
                    logger.error(f"Error processing {file}: {type(e).__name__}: {str(e)}")
                    logger.debug(traceback.format_exc())
        
        # Chuyển sang DataFrame
        if all_jobs:
            # Chuyển đổi các list thành JSON string trước khi tạo DataFrame
            for job in all_jobs:
                # Chuyển skills list thành JSON string để tránh lỗi khi lưu vào database
                if 'skills' in job and isinstance(job['skills'], list):
                    job['skills'] = json.dumps(job['skills'])
            
            df = pd.DataFrame(all_jobs)
            # Drop duplicates dựa trên job_id
            df = df.drop_duplicates(subset=['job_id'])
            
            # Đảm bảo thứ tự các cột đúng với schema SQL và không có raw_data
            column_order = [
                'job_id', 'title', 'job_url', 'company_name', 'company_url',
                'salary', 'skills', 'location', 'location_detail', 'deadline',
                'verified_employer', 'last_update', 'logo_url', 'posted_time', 'crawled_at'
            ]
            
            # Lọc và sắp xếp cột
            existing_columns = [col for col in column_order if col in df.columns]
            df = df[existing_columns]
            
            logger.info(f"Created DataFrame with {len(df)} unique jobs (from {len(all_jobs)} total parsed)")
            return df
        
        logger.warning("No valid job data parsed from any HTML files")
        return pd.DataFrame()
