# JobInsight Crawler System - API Reference

## Overview

This document provides comprehensive API specifications for the JobInsight Crawler System, including class interfaces, method signatures, configuration parameters, and integration patterns for developers and system integrators.

## Core API Classes

### 1. TopCVCrawler - Main Orchestrator

**Class Definition**:
```python
class TopCVCrawler:
    """
    Central orchestrator for the JobInsight crawler system.
    Coordinates HTML backup, parsing, and database ingestion operations.
    """
```

**Constructor**:
```python
def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Initialize TopCVCrawler with optional configuration.
    
    Args:
        config: Configuration dictionary with crawler settings
                If None, uses default configuration from Config class
    
    Raises:
        ConfigurationError: If configuration validation fails
        ComponentInitializationError: If component initialization fails
    """
```

**Primary Methods**:

**`run(config: Optional[Dict] = None) -> Dict[str, Any]`**
```python
@staticmethod
def run(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Execute complete crawling workflow.
    
    Args:
        config: Runtime configuration overrides
    
    Returns:
        Dict containing execution results:
        {
            'success': bool,
            'execution_time': float,
            'backup': {
                'successful': int,
                'total': int,
                'failed': int
            },
            'parse': {
                'total_jobs': int,
                'company_count': int,
                'location_count': int
            },
            'database': {
                'inserted': int,
                'updated': int,
                'total_processed': int
            },
            'cdc': {
                'inserted': int,
                'updated': int,
                'failed': int
            }
        }
    
    Raises:
        CrawlerExecutionError: If critical errors occur during execution
        DatabaseConnectionError: If database operations fail
        NetworkError: If network connectivity issues occur
    """
```

**Configuration Parameters**:
```python
# TopCVCrawler configuration schema
crawler_config = {
    'num_pages': int,           # Number of pages to crawl (default: 5)
    'use_parallel': bool,       # Enable parallel processing (default: True)
    'db_table': str,           # Target database table (default: 'raw_jobs')
    'db_schema': str,          # Database schema (default: None)
    'enable_cdc': bool,        # Enable CDC logging (default: True)
    'backup_dir': str,         # HTML backup directory path
    'max_retry': int,          # Maximum retry attempts (default: 3)
    'timeout': int             # Operation timeout in seconds (default: 300)
}
```

### 2. HTMLBackupManager - Web Scraping Engine

**Class Definition**:
```python
class HTMLBackupManager:
    """
    Manages HTML backup operations with anti-detection capabilities.
    Implements concurrent web scraping using Playwright.
    """
```

**Constructor**:
```python
def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
    """
    Initialize HTMLBackupManager with configuration.
    
    Args:
        config: Configuration dictionary for backup operations
    
    Configuration Keys:
        - base_url: Target website URL
        - min_delay: Minimum delay between requests (seconds)
        - max_delay: Maximum delay between requests (seconds)
        - concurrent_backups: Number of concurrent backup operations
        - page_load_timeout: Page load timeout (milliseconds)
        - selector_timeout: Element selector timeout (milliseconds)
        - backup_dir: Directory for storing HTML backups
    """
```

**Primary Methods**:

**`backup_html_pages_parallel(num_pages: int = 5) -> List[Dict[str, Any]]`**
```python
async def backup_html_pages_parallel(self, num_pages: int = 5) -> List[Dict[str, Any]]:
    """
    Backup multiple HTML pages concurrently.
    
    Args:
        num_pages: Number of pages to backup
    
    Returns:
        List of backup results:
        [
            {
                'page_num': int,
                'success': bool,
                'file_path': str,
                'file_size': int,
                'execution_time': float,
                'error': Optional[str]
            }
        ]
    
    Raises:
        NetworkError: If network connectivity fails
        AntiDetectionError: If anti-bot measures are triggered
        FileSystemError: If file operations fail
    """
```

**`backup_single_page(page_num: int) -> Dict[str, Any]`**
```python
async def backup_single_page(self, page_num: int) -> Dict[str, Any]:
    """
    Backup a single HTML page with anti-detection measures.
    
    Args:
        page_num: Page number to backup (1-based)
    
    Returns:
        Backup result dictionary:
        {
            'page_num': int,
            'success': bool,
            'file_path': str,
            'file_size': int,
            'execution_time': float,
            'user_agent': str,
            'viewport': Dict[str, int],
            'error': Optional[str]
        }
    """
```

### 3. TopCVParser - Data Extraction Engine

**Class Definition**:
```python
class TopCVParser:
    """
    High-performance HTML parser for job data extraction.
    Implements multi-threaded parsing with memory management.
    """
```

**Constructor**:
```python
def __init__(self, max_workers: int = 10) -> None:
    """
    Initialize TopCVParser with worker configuration.
    
    Args:
        max_workers: Maximum number of worker threads for parsing
    """
```

**Primary Methods**:

**`parse_multiple_files(html_files: Optional[List[Union[str, Path]]] = None) -> pd.DataFrame`**
```python
def parse_multiple_files(self, html_files: Optional[List[Union[str, Path]]] = None) -> pd.DataFrame:
    """
    Parse multiple HTML files concurrently.
    
    Args:
        html_files: List of HTML file paths to parse
                   If None, scans backup directory for HTML files
    
    Returns:
        pandas.DataFrame with columns:
        - job_id: Unique job identifier
        - title: Job title
        - job_url: Job posting URL
        - company_name: Company name
        - company_url: Company profile URL
        - salary: Salary information
        - skills: List of required skills
        - location: Job location
        - location_detail: Detailed location information
        - deadline: Application deadline
        - verified_employer: Boolean verification status
        - last_update: Last update timestamp
        - logo_url: Company logo URL
        - posted_time: Job posting time
        - crawled_at: Crawl timestamp
    
    Raises:
        ParsingError: If HTML parsing fails
        FileNotFoundError: If HTML files are not found
        MemoryError: If memory limits are exceeded
    """
```

**`parse_html_file(html_file: Union[str, Path]) -> List[Dict[str, Any]]`**
```python
def parse_html_file(self, html_file: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    Parse a single HTML file for job data.
    
    Args:
        html_file: Path to HTML file
    
    Returns:
        List of job data dictionaries
    
    Raises:
        FileNotFoundError: If HTML file doesn't exist
        ParsingError: If HTML structure is invalid
    """
```

### 4. DBBulkOperations - Database Engine

**Class Definition**:
```python
class DBBulkOperations:
    """
    High-performance database operations for bulk data processing.
    Implements PostgreSQL COPY and upsert operations.
    """
```

**Primary Methods**:

**`bulk_upsert(df: pd.DataFrame, table_name: str, key_columns: List[str]) -> Dict[str, Any]`**
```python
def bulk_upsert(self, df: pd.DataFrame, table_name: str, key_columns: List[str]) -> Dict[str, Any]:
    """
    Perform bulk upsert operation using temporary table strategy.
    
    Args:
        df: DataFrame containing data to upsert
        table_name: Target table name
        key_columns: List of columns for conflict resolution
    
    Returns:
        Operation result:
        {
            'inserted': int,        # Number of new records inserted
            'updated': int,         # Number of existing records updated
            'execution_time': float, # Operation duration in seconds
            'total_processed': int   # Total records processed
        }
    
    Raises:
        DatabaseError: If database operation fails
        ValidationError: If data validation fails
        ConnectionError: If database connection fails
    """
```

**`bulk_insert_with_copy(df: pd.DataFrame, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]`**
```python
def bulk_insert_with_copy(self, df: pd.DataFrame, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
    """
    High-performance bulk insert using PostgreSQL COPY.
    
    Args:
        df: DataFrame containing data to insert
        table_name: Target table name
        schema: Database schema (optional)
    
    Returns:
        Insert result:
        {
            'rows_inserted': int,
            'execution_time': float
        }
    """
```

## Configuration API

### Configuration Classes

**`Config` - Central Configuration Management**:
```python
class Config:
    """Central configuration management with environment variable support."""
    
    class Database:
        HOST: str = "postgres"
        PORT: int = 5432
        USER: str = "jobinsight"
        PASSWORD: str = "jobinsight"
        NAME: str = "jobinsight"
        
        @classmethod
        def get_connection_params(cls) -> Dict[str, Any]:
            """Get database connection parameters."""
    
    class Crawler:
        BASE_URL: str = "https://www.topcv.vn/viec-lam-it"
        NUM_PAGES: int = 5
        MIN_DELAY: float = 3.0
        MAX_DELAY: float = 6.0
        PAGE_LOAD_TIMEOUT: int = 60000
        SELECTOR_TIMEOUT: int = 20000
        MAX_RETRY: int = 3
    
    class Threading:
        MAX_WORKERS: int = min(32, (os.cpu_count() or 1) * 5)
    
    class CDC:
        DAYS_TO_KEEP: int = 15
        FILE_LOCK_TIMEOUT: int = 10
```

### Environment Variable Mapping

**Database Configuration**:
```bash
DB_HOST=postgres                    # Database host
DB_PORT=5432                       # Database port
DB_USER=jobinsight                 # Database username
DB_PASSWORD=jobinsight             # Database password
DB_NAME=jobinsight                 # Database name
```

**Crawler Configuration**:
```bash
CRAWLER_BASE_URL=https://www.topcv.vn/viec-lam-it
CRAWLER_NUM_PAGES=5
CRAWLER_MIN_DELAY=3
CRAWLER_MAX_DELAY=6
CRAWLER_PAGE_LOAD_TIMEOUT=60000
CRAWLER_SELECTOR_TIMEOUT=20000
CRAWLER_MAX_RETRY=3
```

**Performance Configuration**:
```bash
MAX_WORKERS=10                     # Maximum worker threads
CONCURRENT_BACKUPS=3               # Concurrent backup operations
CDC_DAYS_TO_KEEP=15               # CDC retention period
```

## CDC (Change Data Capture) API

### CDC Functions

**`save_cdc_record(job_id: str, action: str, data: Dict[str, Any]) -> bool`**
```python
def save_cdc_record(job_id: str, action: str, data: Dict[str, Any]) -> bool:
    """
    Save Change Data Capture record for audit trail.
    
    Args:
        job_id: Unique job identifier
        action: Operation type ('insert' or 'update')
        data: Job data dictionary
    
    Returns:
        bool: True if CDC record saved successfully
    
    CDC Record Format:
    {
        'timestamp': str,           # ISO format timestamp
        'job_id': str,             # Job identifier
        'action': str,             # 'insert' or 'update'
        'data': Dict[str, Any],    # Complete job data
        'metadata': {
            'source': 'crawler',
            'version': '1.0'
        }
    }
    """
```

**`cleanup_old_cdc_files(days_to_keep: int = 30) -> Dict[str, int]`**
```python
def cleanup_old_cdc_files(days_to_keep: int = 30) -> Dict[str, int]:
    """
    Clean up old CDC files beyond retention period.
    
    Args:
        days_to_keep: Number of days to retain CDC files
    
    Returns:
        Cleanup statistics:
        {
            'dirs_removed': int,
            'files_removed': int,
            'errors': int,
            'bytes_freed': int
        }
    """
```

## Error Handling API

### Exception Classes

**`CrawlerExecutionError`**:
```python
class CrawlerExecutionError(Exception):
    """Raised when critical errors occur during crawler execution."""
    
    def __init__(self, message: str, component: str, details: Optional[Dict] = None):
        self.component = component
        self.details = details or {}
        super().__init__(message)
```

**`DatabaseConnectionError`**:
```python
class DatabaseConnectionError(Exception):
    """Raised when database connectivity issues occur."""
    
    def __init__(self, message: str, connection_params: Optional[Dict] = None):
        self.connection_params = connection_params
        super().__init__(message)
```

**`AntiDetectionError`**:
```python
class AntiDetectionError(Exception):
    """Raised when anti-bot detection is triggered."""
    
    def __init__(self, message: str, detection_type: str, page_url: str):
        self.detection_type = detection_type
        self.page_url = page_url
        super().__init__(message)
```

## Integration Patterns

### Airflow Integration

**DAG Task Definition**:
```python
def crawl_and_process_task(**kwargs):
    """Airflow task for crawler execution."""
    from src.crawler.crawler import TopCVCrawler
    
    # Execute crawler
    result = TopCVCrawler.run()
    
    # Log results
    logger.info(f"Crawl completed: {result['success']}")
    logger.info(f"Jobs processed: {result['parse']['total_jobs']}")
    
    return result
```

### Practical Usage Examples

#### **Basic TopCVCrawler Usage**
```python
from src.crawler.crawler import TopCVCrawler

# Simple crawl with defaults
result = TopCVCrawler.run()
print(f"Success: {result['success']}")
print(f"Jobs parsed: {result['parse']['total_jobs']}")

# Custom configuration
config = {
    'num_pages': 3,           # Crawl 3 pages instead of default 5
    'use_parallel': False,    # Sequential processing
    'db_table': 'raw_jobs',   # Target table
    'enable_cdc': True        # Enable change data capture
}
result = TopCVCrawler.run(config=config)

# Error handling
if result['success']:
    print(f"Backup: {result['backup']['successful']}/{result['backup']['total']}")
    print(f"Database: {result['database']['inserted']} inserted")
else:
    print(f"Error: {result.get('error', 'Unknown error')}")
```

#### **HTMLBackupManager Usage**
```python
from src.crawler.backup_manager import HTMLBackupManager
import asyncio

async def backup_example():
    config = {
        'concurrent_backups': 3,
        'min_delay': 3,
        'max_delay': 6
    }
    manager = HTMLBackupManager(config)

    # Sequential backup
    results = await manager.backup_html_pages(3, parallel=False)
    print(f"Backed up {len(results)} pages")

    # Parallel backup
    results = await manager.backup_html_pages_parallel(5)
    successful = sum(1 for r in results if r.get('success'))
    print(f"Parallel backup: {successful}/{len(results)} successful")

# Run the example
asyncio.run(backup_example())
```

#### **TopCVParser Usage**
```python
from src.crawler.parser import TopCVParser
import pandas as pd

# Initialize parser
parser = TopCVParser()

# Parse single file
jobs = parser.parse_html_file('data/raw_backup/it_p1_20250127154513.html')
print(f"Parsed {len(jobs)} jobs from single file")

# Parse multiple files
html_files = [
    'data/raw_backup/it_p1_20250127154513.html',
    'data/raw_backup/it_p2_20250127154513.html'
]
df = parser.parse_multiple_files(html_files)
print(f"Total jobs parsed: {len(df)}")

# Access parsed data
if not df.empty:
    print("Sample job data:")
    print(df[['title', 'company_name', 'location']].head())
```

#### **Error Handling Patterns**
```python
from src.crawler.crawler import TopCVCrawler
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def robust_crawl():
    try:
        config = {
            'num_pages': 5,
            'use_parallel': True,
            'db_table': 'raw_jobs',
            'enable_cdc': True
        }

        result = TopCVCrawler.run(config=config)

        if result['success']:
            logger.info(f"Crawl successful: {result['parse']['total_jobs']} jobs")

            # Check for warnings
            if result['backup']['failed'] > 0:
                logger.warning(f"Some pages failed: {result['backup']['failed']}")

            return result
        else:
            logger.error(f"Crawl failed: {result.get('error')}")
            return None

    except Exception as e:
        logger.error(f"Critical error: {e}")
        return None

# Usage
result = robust_crawl()
if result:
    print("Crawl completed successfully")
```

#### **Configuration Examples**
```python
# Development configuration
dev_config = {
    'num_pages': 2,
    'use_parallel': False,
    'concurrent_backups': 1,
    'min_delay': 2,
    'max_delay': 4,
    'enable_cdc': False
}

# Production configuration
prod_config = {
    'num_pages': 5,
    'use_parallel': True,
    'concurrent_backups': 3,
    'min_delay': 3,
    'max_delay': 6,
    'enable_cdc': True,
    'db_table': 'raw_jobs'
}

# Testing configuration
test_config = {
    'num_pages': 1,
    'use_parallel': False,
    'concurrent_backups': 1,
    'min_delay': 1,
    'max_delay': 2,
    'enable_cdc': False,
    'db_table': 'test_jobs'
}

# Usage
result = TopCVCrawler.run(config=prod_config)
```

## Monitoring API

### Performance Metrics Collection

**`get_performance_metrics() -> Dict[str, Any]`**
```python
def get_performance_metrics() -> Dict[str, Any]:
    """
    Collect current performance metrics.
    
    Returns:
        Performance metrics dictionary:
        {
            'system': {
                'cpu_percent': float,
                'memory_percent': float,
                'disk_usage': float
            },
            'crawler': {
                'last_execution_time': float,
                'success_rate_24h': float,
                'jobs_processed_today': int
            },
            'database': {
                'connection_pool_size': int,
                'active_connections': int,
                'query_performance': Dict[str, float]
            }
        }
    """
```

---

*This API reference provides comprehensive technical specifications for integrating with the JobInsight Crawler System. For additional examples and use cases, refer to the example scripts in the `/examples` directory.*
