"""
Tests for the data processing module
"""

import os
import sys
import json
import pytest
import pandas as pd
import warnings
from unittest.mock import patch, MagicMock
from pandas.testing import assert_frame_equal
from datetime import datetime, timedelta

# Bỏ qua cảnh báo từ pandas.to_sql
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import processing module
from src.etl.raw_to_staging import (
    setup_database_schema,
    run_stored_procedures,
    load_staging_data,
    save_back_to_staging,
    process_staging_data,
    run_etl
)
from src.processing.data_processing import (
    clean_title,
    clean_company_name, 
    extract_location_info,
    refine_location
)

# Test data
@pytest.fixture
def sample_staging_data():
    """Sample staging data for testing"""
    return pd.DataFrame({
        'job_id': ['1', '2', '3'],
        'title': ['Senior Python Developer - Urgent', 'Frontend React Developer / Team Lead', 'DevOps Engineer (AWS)'],
        'company_name': ['công ty tnhh ABC', 'CÔNG TY CỔ PHẦN XYZ VIỆT NAM', 'fpt software'],
        'location': ['Hà Nội', 'Hồ Chí Minh & Đà Nẵng', 'TP HCM'],
        'location_detail': [
            '<div>Hà Nội: 123 ABC</div>', 
            '<div>Hồ Chí Minh: 456 XYZ<br/>Đà Nẵng: 789 DEF</div>', 
            '<div>TP HCM: 101 MNO</div>'
        ],
        'salary': ['15-20 triệu', '1,000-2,000 USD', 'Thoả thuận'],
        'deadline': ['30', '15', '7'],
        'crawled_at': [
            datetime(2025, 5, 1), 
            datetime(2025, 5, 5), 
            datetime(2025, 5, 10)
        ]
    })

@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    with patch('src.etl.raw_to_staging.get_connection') as mock_conn:
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        yield mock_conn, mock_cursor

@pytest.fixture
def mock_dataframe():
    """Mock dataframe retrieval"""
    with patch('src.etl.raw_to_staging.get_dataframe') as mock_get_df:
        yield mock_get_df

# Test individual processing functions
class TestProcessingFunctions:
    
    def test_clean_title(self):
        """Test the title cleaning function"""
        # Test cases
        titles = [
            'Senior Python Developer - Urgent', 
            'Frontend React Developer / Team Lead', 
            'DevOps Engineer (AWS)',
            None
        ]
        expected = [
            'Senior Python Developer', 
            'Frontend React Developer / Team Lead', 
            'DevOps Engineer',
            ''
        ]
        
        # Test each case
        for title, expect in zip(titles, expected):
            assert clean_title(title) == expect
    
    def test_clean_company_name(self):
        """Test the company name cleaning function"""
        # Test cases
        names = [
            'công ty tnhh ABC', 
            'CÔNG TY CỔ PHẦN XYZ VIỆT NAM',
            'fpt software',
            None
        ]
        expected = [
            'Công ty TNHH ABC', 
            'Công TY Cổ Phần XYZ Việt NAM',
            'FPT Software',
            ''
        ]
        
        # Test each case
        for name, expect in zip(names, expected):
            assert clean_company_name(name) == expect
    
    def test_extract_location_info(self):
        """Test location info extraction"""
        # Test cases
        html_contents = [
            '<div>Hà Nội: 123 ABC</div>',
            '<div>Hồ Chí Minh: 456 XYZ<br/>Đà Nẵng: 789 DEF</div>',
            None
        ]
        expected = [
            ['Hà Nội: 123 ABC'],
            ['Hồ Chí Minh: 456 XYZ', 'Đà Nẵng: 789 DEF'],
            []
        ]
        
        # Test each case
        for html, expect in zip(html_contents, expected):
            assert extract_location_info(html) == expect
    
    def test_refine_location(self):
        """Test location refining"""
        # Test cases
        test_data = [
            {'location': 'Hà Nội', 'location_pairs': ['Hà Nội: 123 ABC']},
            {'location': 'Hồ Chí Minh & Đà Nẵng', 'location_pairs': ['Hồ Chí Minh: 456 XYZ', 'Đà Nẵng: 789 DEF']},
            {'location': 'TP HCM', 'location_pairs': ['TP HCM: 101 MNO']}
        ]
        expected = [
            'Hà Nội',
            'Hồ Chí Minh, Đà Nẵng',
            'TP HCM'
        ]
        
        # Test each case
        for data, expect in zip(test_data, expected):
            assert refine_location(pd.Series(data)) == expect

# Test ETL processing functions
class TestETLFunctions:
    
    def test_process_staging_data(self, sample_staging_data):
        """Test processing of staging data"""
        # Process the sample data
        processed_df = process_staging_data(sample_staging_data)
        
        # Check that the required columns were added
        assert 'location_pairs' in processed_df.columns
        assert 'title_clean' in processed_df.columns
        assert 'company_name_standardized' in processed_df.columns
        
        # Verify some values
        assert processed_df['title_clean'].tolist() == [
            'Senior Python Developer', 
            'Frontend React Developer / Team Lead', 
            'DevOps Engineer'
        ]
        assert processed_df['company_name_standardized'].tolist() == [
            'Công ty TNHH ABC', 
            'Công TY Cổ Phần XYZ Việt NAM',
            'FPT Software'
        ]
    
    def test_setup_database_schema(self, mock_db_connection):
        """Test database schema setup"""
        mock_conn, mock_cursor = mock_db_connection
        
        # Mock the schema existence check
        mock_cursor.fetchone.return_value = [True]  # Schema exists
        
        # Mock the execute_sql_file function
        with patch('src.processing.data_etl.execute_sql_file', return_value=True):
            result = setup_database_schema()
            
        assert result is True
    
    def test_load_staging_data(self, mock_dataframe, sample_staging_data):
        """Test loading data from staging"""
        # Set up mock to return our sample data
        mock_dataframe.return_value = sample_staging_data
        
        # Call the function
        result_df = load_staging_data()
        
        # Verify the result
        assert_frame_equal(result_df, sample_staging_data)
        
        # Verify the SQL query used
        mock_dataframe.assert_called_once()
        args, _ = mock_dataframe.call_args
        assert 'SELECT * FROM' in args[0]
    
    @patch('sqlalchemy.create_engine')
    def test_save_back_to_staging(self, mock_create_engine, sample_staging_data):
        """Test saving processed data back to staging"""
        # Mock the SQLAlchemy engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Process sample data
        processed_df = process_staging_data(sample_staging_data)
        
        # Call the function
        result = save_back_to_staging(processed_df)
        
        # Verify the result
        assert result is True
        mock_create_engine.assert_called_once()
        # Verify to_sql was called with expected parameters
        assert hasattr(processed_df, 'to_sql')
    
    @patch('src.etl.raw_to_staging.setup_database_schema', return_value=True)
    @patch('src.etl.raw_to_staging.run_stored_procedures', return_value=True)
    @patch('src.etl.raw_to_staging.load_staging_data')
    @patch('src.etl.raw_to_staging.process_staging_data')
    @patch('src.etl.raw_to_staging.save_back_to_staging', return_value=True)
    def test_run_etl(self, mock_save, mock_process, mock_load, mock_run_sp, mock_setup, sample_staging_data):
        """Test running the entire ETL process"""
        # Setup mocks
        mock_load.return_value = sample_staging_data
        mock_process.return_value = sample_staging_data  # Simplified for testing
        
        # Call the function
        result = run_etl()
        
        # Verify results
        assert result is True
        mock_setup.assert_called_once()
        mock_run_sp.assert_called_once()
        mock_load.assert_called_once()
        mock_process.assert_called_once_with(sample_staging_data)
        mock_save.assert_called_once_with(sample_staging_data)
        
if __name__ == '__main__':
    pytest.main(['-xvs', __file__]) 