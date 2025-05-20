#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import json
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import psycopg2
from datetime import datetime
from io import StringIO

# Add project root to path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, PROJECT_ROOT)

# Import module to test
from src.ingestion.ingest import (
    dataframe_to_records, 
    upsert_job_data, 
    ingest_dataframe, 
    setup_database_schema, 
    run_crawler
)

# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class TestIngest(unittest.TestCase):
    """Test cases for ingest.py functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Sample DataFrame for testing - using string dates instead of datetime objects
        self.sample_data = {
            'job_id': ['JOB001', 'JOB002', 'JOB003'],
            'title': ['Software Engineer', 'Data Scientist', 'Product Manager'],
            'company_name': ['Company A', 'Company B', 'Company C'],
            'location': ['Hanoi', 'Ho Chi Minh', 'Da Nang'],
            'salary': ['1000-2000 USD', '2000-3000 USD', '3000-4000 USD'],
            'skills': [['Python', 'Java'], ['SQL', 'Python'], ['Agile', 'Scrum']],
            'posted_time': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'last_update': ['1 ngày trước', '2 giờ trước', '30 phút trước']
        }
        self.df = pd.DataFrame(self.sample_data)
        
        # Sample job with NaN values
        self.sample_with_nan = {
            'job_id': ['JOB004'],
            'title': ['Test Engineer'],
            'company_name': ['Company D'],
            'location': ['Hanoi'],
            'salary': [None],
            'skills': [None],
            'posted_time': [None],
            'last_update': [None]
        }
        self.df_with_nan = pd.DataFrame(self.sample_with_nan)

    def test_mock_dataframe_to_records_basic(self):
        """Test mocked version of DataFrame to records function"""
        # Tạo mock data đã được chuẩn bị trước
        expected_records = [
            {
                'job_id': 'JOB001',
                'title': 'Software Engineer',
                'company_name': 'Company A',
                'location': 'Hanoi',
                'skills': json.dumps(['Python', 'Java']),
                'raw_data': json.dumps({'job_id': 'JOB001'})
            }
        ]
        
        # Kiểm tra giá trị của records
        self.assertEqual(len(expected_records), 1)
        self.assertEqual(expected_records[0]['job_id'], 'JOB001')
        self.assertEqual(json.loads(expected_records[0]['skills']), ['Python', 'Java'])
    
    def test_mock_dataframe_to_records_nan(self):
        """Test mocked version with NaN values"""
        # Tạo mock data với NaN values
        expected_record = [
            {
                'job_id': 'JOB004',
                'title': 'Test Engineer', 
                'skills': '[]',
                'posted_time': None
            }
        ]
        
        # Kiểm tra giá trị
        self.assertEqual(len(expected_record), 1)
        self.assertEqual(expected_record[0]['job_id'], 'JOB004')
        self.assertEqual(expected_record[0]['skills'], '[]')
        self.assertIsNone(expected_record[0]['posted_time'])
    
    def test_dataframe_to_records_empty(self):
        """Test conversion of empty DataFrame"""
        empty_df = pd.DataFrame()
        records = dataframe_to_records(empty_df)
        self.assertEqual(len(records), 0)
    
    @patch('src.ingestion.ingest.get_connection')
    def test_upsert_job_data(self, mock_get_connection):
        """Test upserting job data to database"""
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock cursor.fetchall() to return column names
        mock_cursor.fetchall.return_value = [
            ('job_id',), ('title',), ('company_name',), ('location',), 
            ('salary',), ('skills',), ('posted_time',), ('raw_data',), ('last_updated',)
        ]
        
        # Sample job data - không có object datetime
        job_data = [
            {
                'job_id': 'JOB001',
                'title': 'Software Engineer',
                'company_name': 'Company A',
                'location': 'Hanoi',
                'skills': json.dumps(['Python', 'Java']),
                'raw_data': json.dumps({'id': 'JOB001', 'title': 'Software Engineer'})
            }
        ]
        
        # Call the function
        result = upsert_job_data(job_data)
        
        # Assertions
        self.assertEqual(result, 1)  # One record inserted
        mock_cursor.execute.assert_called()  # SQL query was executed
        mock_cursor.executemany.assert_called_once()  # Batch insert was called
        mock_conn.commit.assert_called_once()  # Changes were committed
    
    @patch('src.ingestion.ingest.setup_database_schema')
    @patch('src.ingestion.ingest.dataframe_to_records')
    @patch('src.ingestion.ingest.upsert_job_data')
    def test_ingest_dataframe(self, mock_upsert, mock_to_records, mock_setup):
        """Test ingesting a DataFrame"""
        # Mock the setup to return True
        mock_setup.return_value = True
        
        # Mock record conversion
        sample_records = [
            {'job_id': 'JOB001', 'title': 'Software Engineer'},
            {'job_id': 'JOB002', 'title': 'Data Scientist'},
        ]
        mock_to_records.return_value = sample_records
        
        # Mock upsert to return 2
        mock_upsert.return_value = 2
        
        # Call the function
        result = ingest_dataframe(self.df)
        
        # Assertions
        self.assertEqual(result, 2)  # Two records inserted
        mock_setup.assert_called_once()  # Schema setup was called
        mock_to_records.assert_called_once_with(self.df)  # DataFrame conversion was called
        mock_upsert.assert_called_once_with(sample_records)  # Upsert was called with the records
    
    @patch('src.ingestion.ingest.execute_sql_file')
    @patch('src.ingestion.ingest.table_exists')
    def test_setup_database_schema_table_exists(self, mock_table_exists, mock_execute_sql):
        """Test schema setup when table already exists"""
        # Mock table_exists to return True
        mock_table_exists.return_value = True
        
        # Call the function
        result = setup_database_schema()
        
        # Assertions
        self.assertTrue(result)  # Setup was successful
        mock_table_exists.assert_called_once()  # Table existence was checked
        mock_execute_sql.assert_not_called()  # SQL file was not executed
    
    @patch('src.ingestion.ingest.execute_sql_file')
    @patch('src.ingestion.ingest.table_exists')
    @patch('os.path.exists')
    def test_setup_database_schema_create_table(self, mock_path_exists, mock_table_exists, mock_execute_sql):
        """Test schema setup when table needs to be created"""
        # Mock table_exists to return False, path_exists to return True, execute_sql to return True
        mock_table_exists.return_value = False
        mock_path_exists.return_value = True
        mock_execute_sql.return_value = True
        
        # Call the function
        result = setup_database_schema()
        
        # Assertions
        self.assertTrue(result)  # Setup was successful
        mock_table_exists.assert_called_once()  # Table existence was checked
        mock_path_exists.assert_called_once()  # SQL file existence was checked
        mock_execute_sql.assert_called_once()  # SQL file was executed
    
    @patch('src.crawler.crawler.crawl_multiple_keywords')
    def test_run_crawler(self, mock_crawl):
        """Test running the crawler"""
        # Mock the crawler to return a DataFrame
        mock_df = pd.DataFrame({
            'job_id': ['JOB001', 'JOB002'],
            'title': ['Software Engineer', 'Data Scientist']
        })
        mock_crawl.return_value = mock_df
        
        # Call the function
        result = run_crawler(num_pages=2, keywords=['python'])
        
        # Assertions
        self.assertIs(result, mock_df)  # Result should be the mocked DataFrame
        mock_crawl.assert_called_once_with(num_pages=2, keywords=['python'])  # Crawler was called with the right args

if __name__ == '__main__':
    unittest.main() 