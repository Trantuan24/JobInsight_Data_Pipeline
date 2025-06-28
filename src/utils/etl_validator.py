#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL Validation và Data Quality Check utilities
"""

import pandas as pd
import logging
import duckdb
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dimension_integrity(duck_conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Kiểm tra tính toàn vẹn của dimension tables
    
    Args:
        duck_conn: Kết nối DuckDB
        
    Returns:
        Dict validation results
    """
    results = {}
    
    # 1. Kiểm tra DimJob
    job_checks = {}
    
    # Check duplicate job_ids
    duplicate_jobs = duck_conn.execute("""
        SELECT job_id, COUNT(*) as count
        FROM DimJob 
        WHERE is_current = TRUE
        GROUP BY job_id
        HAVING COUNT(*) > 1
    """).fetchdf()
    
    job_checks['duplicate_job_ids'] = len(duplicate_jobs)
    job_checks['total_current_jobs'] = duck_conn.execute("SELECT COUNT(*) FROM DimJob WHERE is_current = TRUE").fetchone()[0]
    
    # 2. Kiểm tra DimCompany
    company_checks = {}
    
    # Check duplicate companies
    duplicate_companies = duck_conn.execute("""
        SELECT company_name_standardized, COUNT(*) as count
        FROM DimCompany 
        WHERE is_current = TRUE
        GROUP BY company_name_standardized
        HAVING COUNT(*) > 1
    """).fetchdf()
    
    company_checks['duplicate_companies'] = len(duplicate_companies)
    company_checks['total_current_companies'] = duck_conn.execute("SELECT COUNT(*) FROM DimCompany WHERE is_current = TRUE").fetchone()[0]
    
    # 3. Kiểm tra DimLocation
    location_checks = {}
    location_checks['total_locations'] = duck_conn.execute("SELECT COUNT(*) FROM DimLocation WHERE is_current = TRUE").fetchone()[0]
    location_checks['unknown_locations'] = duck_conn.execute("SELECT COUNT(*) FROM DimLocation WHERE city = 'Unknown' AND is_current = TRUE").fetchone()[0]
    
    # 4. Kiểm tra DimDate
    date_checks = {}
    date_checks['total_dates'] = duck_conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
    
    # Check for missing dates in a range
    min_date = duck_conn.execute("SELECT MIN(date_id) FROM DimDate").fetchone()[0]
    max_date = duck_conn.execute("SELECT MAX(date_id) FROM DimDate").fetchone()[0]
    
    if min_date and max_date:
        expected_days = (max_date - min_date).days + 1
        actual_days = date_checks['total_dates']
        date_checks['missing_dates'] = expected_days - actual_days
    
    results['DimJob'] = job_checks
    results['DimCompany'] = company_checks  
    results['DimLocation'] = location_checks
    results['DimDate'] = date_checks
    
    return results

def validate_fact_integrity(duck_conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Kiểm tra tính toàn vẹn của fact tables
    
    Args:
        duck_conn: Kết nối DuckDB
        
    Returns:
        Dict validation results
    """
    results = {}
    
    # 1. Kiểm tra FactJobPostingDaily
    fact_checks = {}
    
    # Total facts
    fact_checks['total_facts'] = duck_conn.execute("SELECT COUNT(*) FROM FactJobPostingDaily").fetchone()[0]
    
    # Check orphaned facts (missing dimension keys)
    orphaned_jobs = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobPostingDaily f
        LEFT JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
        WHERE j.job_sk IS NULL
    """).fetchone()[0]
    
    orphaned_companies = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobPostingDaily f
        LEFT JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
        WHERE c.company_sk IS NULL
    """).fetchone()[0]
    
    orphaned_dates = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobPostingDaily f
        LEFT JOIN DimDate d ON f.date_id = d.date_id
        WHERE d.date_id IS NULL
    """).fetchone()[0]
    
    fact_checks['orphaned_jobs'] = orphaned_jobs
    fact_checks['orphaned_companies'] = orphaned_companies
    fact_checks['orphaned_dates'] = orphaned_dates
    
    # Check load_month distribution
    load_month_stats = duck_conn.execute("""
        SELECT load_month, COUNT(*) as count
        FROM FactJobPostingDaily
        GROUP BY load_month
        ORDER BY load_month
    """).fetchdf()
    
    fact_checks['load_month_distribution'] = load_month_stats.to_dict('records')
    
    # 2. Kiểm tra FactJobLocationBridge
    bridge_checks = {}
    
    # Total bridges
    bridge_checks['total_bridges'] = duck_conn.execute("SELECT COUNT(*) FROM FactJobLocationBridge").fetchone()[0]
    
    # Check orphaned bridges
    orphaned_fact_bridges = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobLocationBridge b
        LEFT JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
        WHERE f.fact_id IS NULL
    """).fetchone()[0]
    
    orphaned_location_bridges = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobLocationBridge b
        LEFT JOIN DimLocation l ON b.location_sk = l.location_sk AND l.is_current = TRUE
        WHERE l.location_sk IS NULL
    """).fetchone()[0]
    
    bridge_checks['orphaned_fact_bridges'] = orphaned_fact_bridges
    bridge_checks['orphaned_location_bridges'] = orphaned_location_bridges
    
    results['FactJobPostingDaily'] = fact_checks
    results['FactJobLocationBridge'] = bridge_checks
    
    return results

def validate_data_quality(duck_conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Kiểm tra chất lượng dữ liệu
    
    Args:
        duck_conn: Kết nối DuckDB
        
    Returns:
        Dict data quality results
    """
    results = {}
    
    # 1. Null value checks
    null_checks = {}
    
    # Check critical null values
    null_job_titles = duck_conn.execute("SELECT COUNT(*) FROM DimJob WHERE title_clean IS NULL AND is_current = TRUE").fetchone()[0]
    null_company_names = duck_conn.execute("SELECT COUNT(*) FROM DimCompany WHERE company_name_standardized IS NULL AND is_current = TRUE").fetchone()[0]
    null_cities = duck_conn.execute("SELECT COUNT(*) FROM DimLocation WHERE city IS NULL AND is_current = TRUE").fetchone()[0]
    
    null_checks['null_job_titles'] = null_job_titles
    null_checks['null_company_names'] = null_company_names
    null_checks['null_cities'] = null_cities
    
    # 2. Data consistency checks
    consistency_checks = {}
    
    # Check salary consistency
    invalid_salaries = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobPostingDaily
        WHERE salary_min > salary_max 
        AND salary_min IS NOT NULL 
        AND salary_max IS NOT NULL
    """).fetchone()[0]
    
    consistency_checks['invalid_salary_ranges'] = invalid_salaries
    
    # Check date consistency
    future_posted_jobs = duck_conn.execute("""
        SELECT COUNT(*)
        FROM FactJobPostingDaily
        WHERE posted_time > CURRENT_TIMESTAMP
    """).fetchone()[0]
    
    consistency_checks['future_posted_jobs'] = future_posted_jobs
    
    # 3. Business rule checks
    business_checks = {}
    
    # Check for jobs with no locations
    jobs_no_location = duck_conn.execute("""
        SELECT COUNT(DISTINCT f.fact_id)
        FROM FactJobPostingDaily f
        LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
        WHERE b.fact_id IS NULL
    """).fetchone()[0]
    
    business_checks['jobs_without_location'] = jobs_no_location
    
    results['null_checks'] = null_checks
    results['consistency_checks'] = consistency_checks
    results['business_checks'] = business_checks
    
    return results

def generate_etl_report(duck_conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Tạo báo cáo tổng hợp ETL
    
    Args:
        duck_conn: Kết nối DuckDB
        
    Returns:
        Dict comprehensive ETL report
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'dimension_integrity': validate_dimension_integrity(duck_conn),
        'fact_integrity': validate_fact_integrity(duck_conn),
        'data_quality': validate_data_quality(duck_conn)
    }
    
    return report

def log_validation_results(validation_results: Dict[str, Any]):
    """
    Log validation results với format đẹp
    
    Args:
        validation_results: Results từ validation functions
    """
    logger.info("="*80)
    logger.info("🔍 ETL VALIDATION REPORT")
    logger.info("="*80)
    
    # Dimension Integrity
    if 'dimension_integrity' in validation_results:
        logger.info("📊 DIMENSION INTEGRITY:")
        for table, checks in validation_results['dimension_integrity'].items():
            logger.info(f"  {table}:")
            for check, value in checks.items():
                status = "❌" if (isinstance(value, int) and value > 0 and 'duplicate' in check) else "✅"
                logger.info(f"    {check}: {value} {status}")
    
    # Fact Integrity  
    if 'fact_integrity' in validation_results:
        logger.info("\n📈 FACT INTEGRITY:")
        for table, checks in validation_results['fact_integrity'].items():
            logger.info(f"  {table}:")
            for check, value in checks.items():
                if check == 'load_month_distribution':
                    logger.info(f"    load_month_distribution:")
                    for item in value:
                        logger.info(f"      {item['load_month']}: {item['count']} records")
                else:
                    status = "❌" if (isinstance(value, int) and value > 0 and 'orphaned' in check) else "✅"
                    logger.info(f"    {check}: {value} {status}")
    
    # Data Quality
    if 'data_quality' in validation_results:
        logger.info("\n🎯 DATA QUALITY:")
        for category, checks in validation_results['data_quality'].items():
            logger.info(f"  {category.replace('_', ' ').title()}:")
            for check, value in checks.items():
                status = "❌" if (isinstance(value, int) and value > 0) else "✅"
                logger.info(f"    {check}: {value} {status}")
    
    logger.info("="*80)

if __name__ == "__main__":
    # Example usage
    import os
    import sys
    
    # Add project root to path
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
        
    from src.utils.config import DUCKDB_PATH
    
    # Connect and validate
    conn = duckdb.connect(DUCKDB_PATH)
    
    # Run validation
    results = generate_etl_report(conn)
    log_validation_results(results)
    
    # Save report to JSON
    report_file = os.path.join(PROJECT_ROOT, "logs", f"etl_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"📄 Validation report saved to: {report_file}")
    
    conn.close() 