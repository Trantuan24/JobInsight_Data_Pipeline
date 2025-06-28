#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Demo script cho JobInsight Crawler
Mẫu minh họa cách sử dụng API mới
"""

import os
import sys
import argparse
import json
from datetime import datetime
import pandas as pd


# Import các module
from src.crawler.crawler import TopCVCrawler
from src.utils.cleanup import cleanup_all_temp_files
from src.utils.logger import get_logger

logger = get_logger("demo")

def main():
    """Hàm main của demo script"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="JobInsight Crawler Demo")
    parser.add_argument("--pages", type=int, default=2, help="Số trang cần crawl (mặc định: 2)")
    parser.add_argument("--parallel", action="store_true", default=True, help="Sử dụng đa luồng (mặc định: True)")
    parser.add_argument("--save-to-db", action="store_true", help="Lưu vào database")
    parser.add_argument("--table", type=str, default="raw_jobs", help="Tên bảng database (mặc định: raw_jobs)")
    parser.add_argument("--max-workers", type=int, default=3, help="Số lượng workers tối đa (mặc định: 3)")
    parser.add_argument("--cleanup", action="store_true", help="Dọn dẹp các file tạm sau khi crawl")
    parser.add_argument("--days-to-keep", type=int, default=15, help="Số ngày dữ liệu cần giữ lại (mặc định: 15)")
    parser.add_argument("--save-csv", type=str, help="Đường dẫn để lưu kết quả dưới dạng CSV")
    
    args = parser.parse_args()
    
    # Hiển thị thông tin cấu hình
    print(f"\n{'='*50}")
    print(f"JOBINSIGHT CRAWLER DEMO")
    print(f"{'='*50}")
    print(f"Cấu hình:")
    print(f"- Số trang:      {args.pages}")
    print(f"- Đa luồng:      {'Có' if args.parallel else 'Không'}")
    print(f"- Lưu vào DB:    {'Có' if args.save_to_db else 'Không'}")
    if args.save_to_db:
        print(f"- Bảng DB:       {args.table}")
    print(f"- Max workers:   {args.max_workers}")
    print(f"- Dọn dẹp files: {'Có' if args.cleanup else 'Không'}")
    if args.cleanup:
        print(f"- Giữ dữ liệu:   {args.days_to_keep} ngày")
    if args.save_csv:
        print(f"- Lưu CSV:       {args.save_csv}")
    print(f"{'='*50}\n")
    
    # Cấu hình crawler
    config = {
        'num_pages': args.pages,
        'use_parallel': args.parallel,
        'concurrent_backups': args.max_workers,
        'db_table': args.table if args.save_to_db else None,  # Chỉ lưu vào DB nếu có flag
    }
    
    # Phần 1: Chạy crawler
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Bắt đầu crawl dữ liệu...")
    
    try:
        # Chạy crawler
        result = TopCVCrawler.run(config=config)
        
        # Hiển thị kết quả
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n[{end_time.strftime('%H:%M:%S')}] Hoàn thành crawl sau {duration:.2f} giây")
        print(f"{'='*50}")
        print(f"KẾT QUẢ CRAWL:")
        print(f"- Trạng thái:    {'Thành công' if result['success'] else 'Thất bại'}")
        print(f"- Thời gian:     {result['execution_time']:.2f} giây")
        print(f"- Backup:        {result['backup']['successful']}/{result['backup']['total']} trang")
        print(f"- Parse:         {result['parse']['total_jobs']} jobs")
        
        if 'parse' in result:
            parse_result = result['parse']
            print(f"- Companies:     {parse_result.get('company_count', 0)} công ty")
            print(f"- Locations:     {parse_result.get('location_count', 0)} địa điểm")
        
        if 'database' in result:
            db_result = result['database']
            print(f"- DB insert:     {db_result['inserted']} inserted")
            print(f"- DB update:     {db_result['updated']} updated")
        
        # Kiểm tra xem có DataFrame không
        if hasattr(result, 'df') and args.save_csv:
            # Lưu kết quả ra CSV
            result.df.to_csv(args.save_csv, index=False, encoding='utf-8-sig')
            print(f"- Đã lưu dữ liệu ra file: {args.save_csv}")
        
    except Exception as e:
        print(f"\n[ERROR] Lỗi khi chạy crawler: {str(e)}")
    
    # Phần 2: Dọn dẹp (nếu cần)
    if args.cleanup:
        print(f"\n{'='*50}")
        print(f"TIẾN HÀNH DỌN DẸP FILES TẠM:")
        
        try:
            cleanup_start = datetime.now()
            print(f"[{cleanup_start.strftime('%H:%M:%S')}] Bắt đầu dọn dẹp files cũ hơn {args.days_to_keep} ngày...")
            
            # Gọi hàm cleanup
            cleanup_result = cleanup_all_temp_files(args.days_to_keep)
            
            cleanup_end = datetime.now()
            cleanup_duration = (cleanup_end - cleanup_start).total_seconds()
            
            print(f"[{cleanup_end.strftime('%H:%M:%S')}] Hoàn thành dọn dẹp sau {cleanup_duration:.2f} giây")
            print(f"- CDC files:     {cleanup_result['cdc']['files_removed']} files")
            print(f"- HTML files:    {cleanup_result['html']['files_removed']} files")
            print(f"- Tổng cộng:     {cleanup_result['total_files_removed']} files")
            print(f"- Giải phóng:    {cleanup_result['total_bytes_freed'] / (1024*1024):.2f} MB")
            
        except Exception as e:
            print(f"\n[ERROR] Lỗi khi dọn dẹp files: {str(e)}")
    
    print(f"\n{'='*50}")
    print("DEMO HOÀN TẤT!")
    
if __name__ == "__main__":
    main() 