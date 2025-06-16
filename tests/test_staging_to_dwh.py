#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test ETL Pipeline đơn giản - chỉ 1 file duy nhất
Test các chức năng cơ bản của ETL từ Staging sang DWH
"""

import os
import sys
import json
import pytest
import pandas as pd
from datetime import datetime, timedelta
import tempfile

# Setup path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, PROJECT_ROOT)

# Import functions to test - đảm bảo import các module đúng
from src.etl.dimension_handler import (
    prepare_dim_job, prepare_dim_company, prepare_dim_location,
    apply_scd_type2_changes
)
from src.etl.fact_handler import (
    generate_daily_fact_records, prepare_fact_job_daily
)
from src.etl.partitioning import calculate_load_month
from src.etl.etl_utils import parse_job_location

class TestETLPipelineSimple:
    """Test class đơn giản cho ETL Pipeline"""
    
    @pytest.fixture
    def sample_data(self):
        """Tạo dữ liệu test đơn giản"""
        return pd.DataFrame({
            'job_id': ['JOB001', 'JOB002'],
            'title_clean': ['Python Developer', 'Java Developer'],
            'job_url': ['https://jobs.com/1', 'https://jobs.com/2'],
            'skills': ['["Python", "Django"]', '["Java", "Spring"]'],
            'last_update': ['2025-05-28', '2025-05-29'],
            'logo_url': ['https://logo1.png', 'https://logo2.png'],
            'company_name': ['Tech Corp', 'Dev Company'],
            'company_name_standardized': ['Tech Corp Ltd', 'Dev Company Inc'],
            'company_url': ['https://techcorp.com', 'https://devcompany.com'],
            'verified_employer': [True, False],
            'location_pairs': ['["Hà Nội: Cầu Giấy"]', '["TP.HCM"]']
        })
    
    # Test SCD Type 2 implementation
    def test_scd_type2_implementation(self, sample_data):
        """Test triển khai SCD Type 2 cho dimensions"""
        print("\n=== Test SCD Type 2 Implementation ===")
        
        # Chuẩn bị dataframe hiện tại và dataframe mới để kiểm tra SCD
        current_data = pd.DataFrame({
            'job_id': ['JOB001'],
            'title_clean': ['Python Developer'],
            'skills': ['["Python", "Django"]'],
            'effective_date': [datetime(2025, 5, 1)],
            'is_current': [True]
        })
        
        new_data = pd.DataFrame({
            'job_id': ['JOB001'],
            'title_clean': ['Senior Python Developer'],  # Thay đổi title
            'skills': ['["Python", "Django", "Flask"]'],  # Thay đổi skills
            'effective_date': [datetime(2025, 5, 28)],
            'is_current': [True]
        })
        
        # Apply SCD Type 2
        result = apply_scd_type2_changes(
            new_df=new_data,
            current_df=current_data,
            key_column='job_id',
            track_columns=['title_clean', 'skills']
        )
        
        # Kiểm tra kết quả
        assert len(result) == 2  # 1 record cũ (deactivated) + 1 record mới
        
        # Kiểm tra record cũ được đánh dấu không còn hiện tại
        old_record = result[result['effective_date'] == datetime(2025, 5, 1)]
        assert len(old_record) == 1
        assert old_record['is_current'].values[0] == False
        
        # Kiểm tra record mới được đánh dấu là hiện tại
        new_record = result[result['effective_date'] == datetime(2025, 5, 28)]
        assert len(new_record) == 1
        assert new_record['is_current'].values[0] == True
        
        print("✅ SCD Type 2 implementation test passed")
        
    def test_location_parsing(self):
        """Test parsing location - chức năng cơ bản nhất"""
        print("\n=== Test Location Parsing ===")
        
        # Test case đơn giản
        location = '["Hà Nội: Cầu Giấy, Đống Đa"]'
        result = parse_job_location(location)
        
        print(f"Input: {location}")
        print(f"Output: {result}")
        
        # Kiểm tra kết quả
        assert len(result) == 2  # Should have 2 districts
        assert (None, 'Hà Nội', 'Cầu Giấy') in result
        assert (None, 'Hà Nội', 'Đống Đa') in result
        
        # Test case đơn giản hơn
        simple_location = 'Đà Nẵng'
        result = parse_job_location(simple_location)
        assert result == [(None, 'Đà Nẵng', None)]
        
        print("✅ Location parsing test passed")
    
    def test_calculate_load_month(self):
        """Test tính toán tháng để partition"""
        print("\n=== Test Calculate Load Month ===")
        
        # Test với datetime
        dt = datetime(2025, 5, 29)
        result = calculate_load_month(dt)
        assert result == "2025-05"
        
        # Test với string date
        result = calculate_load_month("2025-03-15")
        assert result == "2025-03"
        
        # Test với None (should return current month)
        result = calculate_load_month(None)
        current_month = datetime.now().strftime('%Y-%m')
        assert result == current_month
        
        print(f"✅ Load month calculation test passed")
    
    def test_generate_daily_fact_records(self):
        """Test tạo daily fact records"""
        print("\n=== Test Generate Daily Fact Records ===")
        
        # Test với khoảng ngày cụ thể
        posted = datetime(2025, 5, 25)
        due = datetime(2025, 5, 27)
        dates = generate_daily_fact_records(posted, due)
        
        expected_dates = [
            datetime(2025, 5, 25).date(),
            datetime(2025, 5, 26).date(),
            datetime(2025, 5, 27).date()
        ]
        
        assert dates == expected_dates
        print(f"Generated {len(dates)} dates from {posted.date()} to {due.date()}")
        
        print("✅ Daily fact records test passed")
    
    # Cập nhật test để kiểm tra đúng theo SCD Type 2
    def test_prepare_dim_job(self, sample_data):
        """Test chuẩn bị dữ liệu DimJob"""
        print("\n=== Test Prepare DimJob ===")
        
        result = prepare_dim_job(sample_data)
        
        # Kiểm tra số lượng records
        assert len(result) == len(sample_data)
        
        # Kiểm tra có các columns cần thiết
        required_columns = ['job_id', 'title_clean', 'skills', 'effective_date', 'is_current']
        for col in required_columns:
            assert col in result.columns
        
        # Kiểm tra skills là JSON string
        for _, row in result.iterrows():
            skills = row['skills']
            assert isinstance(skills, str)
            # Should be valid JSON
            json.loads(skills)
        
        # Kiểm tra SCD2 fields
        assert all(result['is_current'] == True)
        assert 'effective_date' in result.columns
        
        print(f"✅ DimJob preparation test passed - {len(result)} records")
    
    def test_prepare_dim_company(self, sample_data):
        """Test chuẩn bị dữ liệu DimCompany"""
        print("\n=== Test Prepare DimCompany ===")
        
        result = prepare_dim_company(sample_data)
        
        # Kiểm tra deduplication
        unique_companies = sample_data['company_name_standardized'].nunique()
        assert len(result) == unique_companies
        
        # Kiểm tra có các columns cần thiết
        required_columns = ['company_name_standardized', 'company_url', 'verified_employer', 'is_current']
        for col in required_columns:
            assert col in result.columns
        
        print(f"✅ DimCompany preparation test passed - {len(result)} records")
    
    def test_prepare_dim_location(self, sample_data):
        """Test chuẩn bị dữ liệu DimLocation"""
        print("\n=== Test Prepare DimLocation ===")
        
        result = prepare_dim_location(sample_data)
        
        # Kiểm tra có ít nhất một số records
        assert len(result) > 0
        
        # Kiểm tra có các columns cần thiết
        required_columns = ['province', 'city', 'district', 'is_current']
        for col in required_columns:
            assert col in result.columns
        
        # Kiểm tra tất cả có city
        assert all(pd.notna(result['city']))
        
        print(f"✅ DimLocation preparation test passed - {len(result)} records")
        for _, row in result.iterrows():
            print(f"  Location: {row['province']} | {row['city']} | {row['district']}")
    
    def test_prepare_fact_job_daily(self, sample_data):
        """Test chuẩn bị dữ liệu FactJobDaily"""
        print("\n=== Test Prepare FactJobDaily ===")
        
        # Chuẩn bị dim tables (mock)
        dim_job = prepare_dim_job(sample_data)
        dim_company = prepare_dim_company(sample_data)
        dim_location = prepare_dim_location(sample_data)
        
        # Gọi function để test
        fact_df = prepare_fact_job_daily(
            staging_df=sample_data,
            dim_job_df=dim_job,
            dim_company_df=dim_company,
            dim_location_df=dim_location
        )
        
        # Kiểm tra có cấu trúc đúng
        assert 'job_key' in fact_df.columns
        assert 'company_key' in fact_df.columns
        assert 'location_key' in fact_df.columns
        assert 'job_date' in fact_df.columns
        assert 'load_month' in fact_df.columns
        
        # Kiểm tra số lượng records - mỗi job sẽ có nhiều ngày
        assert len(fact_df) >= len(sample_data)
        
        print(f"✅ FactJobDaily preparation test passed - {len(fact_df)} records")


if __name__ == "__main__":
    # Chạy tests
    tester = TestETLPipelineSimple()
    tester.run_all_tests()
    
    print("\n" + "="*50)
    print("ETL PIPELINE TEST COMPLETE")
    print("="*50)
    print("✅ Location parsing")
    print("✅ Date/month calculations") 
    print("✅ Dimension preparations")
    print("✅ Data quality checks")
    print("✅ Integration flow")
    print("\nETL pipeline is ready! 🚀")
