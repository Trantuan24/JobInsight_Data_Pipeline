#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL Package cho JobInsight Data Warehouse
"""

from .etl_runner import run_staging_to_dwh_etl, main
from .etl_base import get_duckdb_connection, setup_duckdb_schema, get_staging_batch
from .etl_dimensions import process_all_dimensions
from .etl_facts import process_facts_and_bridge
from .etl_cleanup import cleanup_duplicate_fact_records, validate_data_quality

__all__ = [
    'run_staging_to_dwh_etl',
    'main',
    'get_duckdb_connection',
    'setup_duckdb_schema', 
    'get_staging_batch',
    'process_all_dimensions',
    'process_facts_and_bridge',
    'cleanup_duplicate_fact_records',
    'validate_data_quality'
]

__version__ = "1.0.0"
__author__ = "JobInsight ETL Team"
