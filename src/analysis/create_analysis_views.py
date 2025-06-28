#!/usr/bin/env python3
"""
Script để tạo và test analysis views cho job lương 10-15 triệu
"""

import duckdb
import sys
import os
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))

from src.utils.logger import setup_logger
from src.utils.config import get_config

def create_analysis_views():
    """Tạo các view phân tích job lương 10-15 triệu"""
    
    logger = setup_logger("analysis_views")
    config = get_config()
    
    try:
        # Kết nối đến DuckDB
        db_path = config.get('duckdb', {}).get('path', 'data/duck_db/jobinsight_warehouse.duckdb')
        conn = duckdb.connect(db_path)
        
        logger.info("Đã kết nối đến DuckDB warehouse")
        
        # Đọc và execute analysis views SQL
        sql_file = project_root / "sql" / "analysis_views.sql"
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Tách các statement SQL
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--')]
        
        for i, statement in enumerate(statements):
            if statement.upper().startswith('CREATE'):
                try:
                    conn.execute(statement)
                    logger.info(f"Đã tạo view thành công: {i+1}")
                except Exception as e:
                    logger.error(f"Lỗi khi tạo view {i+1}: {e}")
        
        # Test các view đã tạo
        logger.info("=== TESTING ANALYSIS VIEWS ===")
        
        # Test 1: Kiểm tra view vw_job_salary_filter
        try:
            result = conn.execute("SELECT COUNT(*) as total_jobs FROM vw_job_salary_filter").fetchall()
            logger.info(f"View vw_job_salary_filter có {result[0][0]} jobs trong khoảng lương 10-15 triệu")
        except Exception as e:
            logger.error(f"Lỗi test view vw_job_salary_filter: {e}")
        
        # Test 2: Kiểm tra view vw_top10_hn
        try:
            result = conn.execute("SELECT COUNT(*) as hanoi_jobs FROM vw_top10_hn").fetchall()
            logger.info(f"View vw_top10_hn có {result[0][0]} jobs tại Hà Nội")
            
            # Hiển thị top 5 jobs để kiểm tra
            top_jobs = conn.execute("""
                SELECT 
                    job_id,
                    title_clean,
                    company_name_standardized,
                    salary_min/1000000 as salary_min_m,
                    salary_max/1000000 as salary_max_m,
                    due_date,
                    days_to_deadline
                FROM vw_top10_hn 
                ORDER BY due_date ASC
                LIMIT 5
            """).fetchall()
            
            logger.info("Top 5 jobs Hà Nội lương 10-15 triệu:")
            for job in top_jobs:
                logger.info(f"- {job[1]} tại {job[2]} | {job[3]}-{job[4]}M | Deadline: {job[5]} ({job[6]} ngày)")
                
        except Exception as e:
            logger.error(f"Lỗi test view vw_top10_hn: {e}")
        
        # Test 3: Phân tích phân bố job theo thành phố
        try:
            city_stats = conn.execute("""
                SELECT 
                    COALESCE(city, 'Unknown') as city,
                    COUNT(*) as job_count,
                    ROUND(AVG(salary_min)/1000000, 1) as avg_min_salary_m,
                    ROUND(AVG(salary_max)/1000000, 1) as avg_max_salary_m
                FROM vw_job_salary_filter
                GROUP BY city
                ORDER BY job_count DESC
                LIMIT 10
            """).fetchall()
            
            logger.info("Phân bố job lương 10-15 triệu theo thành phố:")
            for city in city_stats:
                logger.info(f"- {city[0]}: {city[1]} jobs | Lương TB: {city[2]}-{city[3]}M")
                
        except Exception as e:
            logger.error(f"Lỗi phân tích phân bố thành phố: {e}")
        
        # Test 4: Kiểm tra deadline distribution
        try:
            deadline_stats = conn.execute("""
                SELECT 
                    CASE 
                        WHEN days_to_deadline <= 7 THEN '1-7 ngày'
                        WHEN days_to_deadline <= 14 THEN '8-14 ngày'
                        WHEN days_to_deadline <= 30 THEN '15-30 ngày'
                        ELSE '>30 ngày'
                    END as deadline_range,
                    COUNT(*) as job_count
                FROM vw_job_salary_filter
                WHERE EXTRACT(DAYS FROM (due_date - CURRENT_DATE)) IS NOT NULL
                GROUP BY deadline_range
                ORDER BY job_count DESC
            """).fetchall()
            
            logger.info("Phân bố job theo thời gian deadline:")
            for stat in deadline_stats:
                logger.info(f"- {stat[0]}: {stat[1]} jobs")
                
        except Exception as e:
            logger.error(f"Lỗi phân tích deadline: {e}")
        
        conn.close()
        logger.info("Hoàn thành tạo và test analysis views")
        
    except Exception as e:
        logger.error(f"Lỗi khi tạo analysis views: {e}")
        raise

if __name__ == "__main__":
    create_analysis_views()
