#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module chính để quản lý toàn bộ quá trình crawl từ TopCV
"""

import os
import sys
import asyncio
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional, Union


# Import modules
try:
    from src.utils.logger import get_logger
    from src.utils.config import CRAWLER_CONFIG
    from src.utils.user_agent_manager import UserAgentManager
    from src.crawler.backup_manager import HTMLBackupManager
    from src.crawler.parser import TopCVParser
    from src.db.bulk_operations import DBBulkOperations
    from src.crawler.captcha_handler import CaptchaHandler
    from src.ingestion.cdc import save_cdc_record
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)
    CRAWLER_CONFIG = {}
    from src.utils.user_agent_manager import UserAgentManager
    from src.crawler.backup_manager import HTMLBackupManager
    from src.crawler.parser import TopCVParser
    from src.db.bulk_operations import DBBulkOperations
    from src.crawler.captcha_handler import CaptchaHandler
    from src.ingestion.cdc import save_cdc_record

logger = get_logger("crawler.main")

class TopCVCrawler:
    """
    Class chính quản lý toàn bộ quá trình crawl từ TopCV
    - Tích hợp tất cả các component trong một class
    - Giúp quản lý luồng dữ liệu xuyên suốt pipeline
    - Cung cấp interface đơn giản cho người dùng
    """
    
    def __init__(self, config=None):
        """
        Khởi tạo TopCVCrawler
        
        Args:
            config: Config dict cho crawler
        """
        self.config = config or CRAWLER_CONFIG
        
        # Khởi tạo các thành phần con
        self.ua_manager = UserAgentManager.from_config()
        self.captcha_handler = CaptchaHandler()
        self.backup_manager = HTMLBackupManager(self.config)
        self.parser = TopCVParser()
        self.db_ops = DBBulkOperations()
        
        # Config crawl với validation
        self.num_pages = self._validate_num_pages(self.config.get('num_pages', 5))
        self.use_parallel = self.config.get('use_parallel', True)
        self.db_table = self._validate_db_table(self.config.get('db_table', 'raw_jobs'))
        self.db_schema = self.config.get('db_schema', None)
        self.enable_cdc = self.config.get('enable_cdc', True)  # Mặc định bật CDC
        
        logger.info(f"Khởi tạo TopCVCrawler với {self.num_pages} trang, " +
                    f"{'đa luồng' if self.use_parallel else 'tuần tự'}, " +
                    f"{'có CDC' if self.enable_cdc else 'không CDC'}")

    def _validate_num_pages(self, num_pages):
        """Validate số trang crawl"""
        if not isinstance(num_pages, int) or num_pages < 1 or num_pages > 50:
            raise ValueError(f"num_pages phải là integer từ 1-50, nhận được: {num_pages}")
        return num_pages

    def _validate_db_table(self, db_table):
        """Validate tên bảng database"""
        if not isinstance(db_table, str) or not db_table.strip():
            raise ValueError(f"db_table phải là string không rỗng, nhận được: {db_table}")
        return db_table.strip()

    async def crawl(self, num_pages=None) -> Dict[str, Any]:
        """
        Thực hiện toàn bộ quy trình crawl
        
        Args:
            num_pages: Số trang cần crawl, nếu None sẽ dùng từ config
            
        Returns:
            Dict: Kết quả quá trình crawl
        """
        start_time = datetime.now()
        num_pages = num_pages or self.num_pages
        
        logger.info(f"Bắt đầu crawl {num_pages} trang từ TopCV")
        
        # Phần 1: Backup HTML
        logger.info("--- Phần 1: Backup HTML ---")
        backup_results = await self.backup_manager.backup_html_pages(num_pages, self.use_parallel)
        
        # Tính số trang backup thành công
        successful_backups = sum(1 for r in backup_results if r.get("success", False))
        logger.info(f"Backup kết thúc: {successful_backups}/{len(backup_results)} trang thành công")
        
        if successful_backups == 0:
            logger.error("Không có trang nào backup thành công, dừng quá trình crawl")
            return {
                "success": False,
                "error": "No successful backups",
                "execution_time": (datetime.now() - start_time).total_seconds(),
                "backup_results": backup_results
            }
        
        # Phần 2: Parse HTML
        logger.info("--- Phần 2: Parse HTML ---")
        successful_files = [r.get("filename") for r in backup_results if r.get("success", False)]
        df = self.parser.parse_multiple_files(successful_files)
        
        if df.empty:
            logger.warning("Không có dữ liệu nào được parse từ HTML")
            return {
                "success": False,
                "error": "No data parsed",
                "execution_time": (datetime.now() - start_time).total_seconds(),
                "backup_results": backup_results
            }
        
        logger.info(f"Parse kết thúc: {len(df)} job được parse thành công")
        
        # Phần 3: Lưu vào database (nếu cần)
        if self.db_table:
            logger.info("--- Phần 3: Lưu vào database ---")
            
            try:
                # Lưu vào DB với bulk upsert
                db_result = self.db_ops.bulk_upsert(
                    df=df,
                    table_name=self.db_table,
                    key_columns=['job_id'],
                    schema=self.db_schema
                )
                
                logger.info(f"Database insert kết thúc: {db_result['inserted']} inserted, " + 
                           f"{db_result['updated']} updated ({db_result['execution_time']:.2f}s)")
                
                # Phần 4: Lưu CDC records (nếu được bật)
                if self.enable_cdc:
                    logger.info("--- Phần 4: Ghi CDC records ---")
                    cdc_stats = {'inserted': 0, 'updated': 0, 'failed': 0}
                    
                    try:
                        # Xử lý các records được insert
                        records_to_process = df.to_dict('records')
                        
                        for record in records_to_process:
                            job_id = record.get('job_id')
                            if not job_id:
                                logger.warning(f"Record không có job_id, bỏ qua CDC: {record}")
                                cdc_stats['failed'] += 1
                                continue
                            
                            # Định nghĩa action dựa trên kết quả DB
                            is_new = job_id in db_result.get('inserted_ids', [])
                            action = 'insert' if is_new else 'update'
                            
                            # Ghi CDC record
                            success = save_cdc_record(job_id, action, record)
                            
                            if success:
                                if action == 'insert':
                                    cdc_stats['inserted'] += 1
                                else:
                                    cdc_stats['updated'] += 1
                            else:
                                cdc_stats['failed'] += 1
                                
                        logger.info(f"CDC completed: {cdc_stats['inserted']} inserted, " +
                                    f"{cdc_stats['updated']} updated, {cdc_stats['failed']} failed")
                    except Exception as e:
                        logger.error(f"Lỗi khi ghi CDC records: {str(e)}")
                
            except Exception as e:
                logger.error(f"Lỗi khi lưu vào database: {str(e)}")
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "execution_time": (datetime.now() - start_time).total_seconds(),
                    "backup_results": backup_results,
                    "parse_results": {"total_jobs": len(df)}
                }
        
        # Tính thời gian thực hiện
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Kết quả cuối cùng
        result = {
            "success": True,
            "execution_time": execution_time,
            "backup": {
                "total": len(backup_results),
                "successful": successful_backups,
                "failed": len(backup_results) - successful_backups
            },
            "parse": {
                "total_jobs": len(df),
                "company_count": df['company_name'].nunique() if 'company_name' in df.columns else 0,
                "location_count": df['location'].nunique() if 'location' in df.columns else 0
            }
        }
        
        # Nếu lưu DB, thêm kết quả
        if self.db_table and 'db_result' in locals():
            result["database"] = {
                "inserted": db_result.get('inserted', 0),
                "updated": db_result.get('updated', 0),
                "execution_time": db_result.get('execution_time', 0)
            }
            
            # Thêm kết quả CDC nếu có
            if self.enable_cdc and 'cdc_stats' in locals():
                result["cdc"] = {
                    "inserted": cdc_stats.get('inserted', 0),
                    "updated": cdc_stats.get('updated', 0),
                    "failed": cdc_stats.get('failed', 0)
                }
        
        logger.info(f"Toàn bộ quá trình crawl kết thúc sau {execution_time:.2f}s")
        return result
    
    @classmethod
    def run(cls, num_pages=None, config=None):
        """
        Phương thức static để chạy crawler, xử lý asyncio
        
        Args:
            num_pages: Số trang cần crawl
            config: Config cho crawler
            
        Returns:
            Dict: Kết quả crawl
        """
        crawler = cls(config)
        return asyncio.run(crawler.crawl(num_pages))



# Test functionality if run directly
if __name__ == "__main__":
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Crawl job data from TopCV")
    parser.add_argument("--pages", type=int, default=2, help="Number of pages to crawl")
    parser.add_argument("--parallel", action="store_true", help="Use parallel crawling")
    parser.add_argument("--no-cdc", action="store_true", help="Disable CDC logging")
    args = parser.parse_args()
    
    # Configure crawler
    config = {
        'num_pages': args.pages,
        'use_parallel': args.parallel,
        'enable_cdc': not args.no_cdc,
    }
    
    # Run crawler
    print(f"Starting crawler with {args.pages} pages, parallel={args.parallel}, cdc={not args.no_cdc}")
    result = TopCVCrawler.run(config=config)
    print(f"Crawl completed: {result['success']}")
    print(f"Time taken: {result['execution_time']:.2f} seconds")
    print(f"Backup: {result['backup']['successful']}/{result['backup']['total']} successful")
    print(f"Parsed: {result['parse']['total_jobs']} jobs")
    
    if 'database' in result:
        print(f"Database: {result['database']['inserted']} inserted, {result['database']['updated']} updated")
    
    if 'cdc' in result:
        print(f"CDC: {result['cdc']['inserted']} inserted, {result['cdc']['updated']} updated, {result['cdc']['failed']} failed") 