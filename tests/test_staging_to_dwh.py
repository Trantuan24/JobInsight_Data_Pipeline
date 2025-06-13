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

# Import functions to test
from src.processing.data_prepare import (
    parse_job_location, calculate_load_month, 
    prepare_dim_job, prepare_dim_company, prepare_dim_location,
    generate_daily_fact_records
)

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
    
    def test_data_quality_checks(self):
        """Test data quality validation"""
        print("\n=== Test Data Quality ===")
        
        # Test với dữ liệu có vấn đề - phải có đủ columns để test
        bad_data = pd.DataFrame({
            'job_id': ['JOB001', '', None],
            'title_clean': ['Good Title', '', None],
            'job_url': ['https://job1.com', '', None],
            'skills': ['["Python"]', '', None],
            'last_update': ['2025-05-28', '', None],
            'logo_url': ['https://logo1.png', '', None],
            'company_name_standardized': ['Good Company', '', None],
            'location_pairs': ['["Hà Nội"]', '', None]
        })
        
        # Test DimJob với bad data
        result = prepare_dim_job(bad_data)
        
        # Kiểm tra null title được handle
        titles = result['title_clean'].tolist()
        assert 'Unknown Title' in titles  # Should replace null/empty with this
        assert 'Good Title' in titles     # Should keep good data
        
        print("✅ Data quality checks passed")
    
    def test_integration_flow(self, sample_data):
        """Test integration của toàn bộ flow"""
        print("\n=== Test Integration Flow ===")
        
        # Step 1: Prepare all dimensions
        dim_job = prepare_dim_job(sample_data)
        dim_company = prepare_dim_company(sample_data)
        dim_location = prepare_dim_location(sample_data)
        
        print(f"Prepared dimensions:")
        print(f"  - DimJob: {len(dim_job)} records")
        print(f"  - DimCompany: {len(dim_company)} records") 
        print(f"  - DimLocation: {len(dim_location)} records")
        
        # Step 2: Check load month calculation
        load_months = []
        for _, row in sample_data.iterrows():
            load_month = calculate_load_month(row['last_update'])
            load_months.append(load_month)
        
        print(f"Load months: {set(load_months)}")
        
        # Step 3: Generate some fact dates
        posted = datetime(2025, 5, 28)
        fact_dates = generate_daily_fact_records(posted, None)
        
        print(f"Generated fact dates: {len(fact_dates)} days")
        
        # Basic validations
        assert len(dim_job) > 0
        assert len(dim_company) > 0
        assert len(dim_location) > 0
        assert len(load_months) > 0
        assert len(fact_dates) > 0
        
        print("✅ Integration flow test passed")
    
    def run_all_tests(self):
        """Chạy tất cả tests"""
        print("🚀 Starting Simple ETL Tests...")
        
        # Create sample data
        sample_data = pd.DataFrame({
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
        
        try:
            self.test_location_parsing()
            self.test_calculate_load_month()
            self.test_generate_daily_fact_records()
            self.test_prepare_dim_job(sample_data)
            self.test_prepare_dim_company(sample_data)
            self.test_prepare_dim_location(sample_data)
            self.test_data_quality_checks()
            self.test_integration_flow(sample_data)
            
            print("\n🎉 All tests passed successfully!")
            
        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            raise


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
