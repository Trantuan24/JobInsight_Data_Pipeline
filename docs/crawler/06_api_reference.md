# JobInsight Crawler - API Reference

## Overview

This document provides comprehensive API reference for the JobInsight Crawler system components, including method signatures, parameters, return values, and usage examples.

## Core Classes

### 1. TopCVCrawler

Main orchestrator class for the crawling process.

#### Constructor

```python
class TopCVCrawler:
    def __init__(self, config=None)
```

**Parameters:**
- `config` (dict, optional): Configuration dictionary overriding defaults
  - `num_pages` (int): Number of pages to crawl (1-50)
  - `use_parallel` (bool): Enable parallel processing
  - `db_table` (str): Target database table name
  - `db_schema` (str, optional): Database schema name
  - `enable_cdc` (bool): Enable Change Data Capture logging

**Example:**
```python
config = {
    'num_pages': 5,
    'use_parallel': True,
    'db_table': 'raw_jobs',
    'enable_cdc': True
}
crawler = TopCVCrawler(config=config)
```

#### Methods

##### `crawl()`

```python
async def crawl(self, num_pages=None)
```

**Description:** Execute the complete crawling pipeline (4 phases).

**Parameters:**
- `num_pages` (int, optional): Số trang cần crawl, nếu None sẽ dùng từ config

**Returns:**
```python
{
    "success": bool,
    "duration_seconds": float,  # seconds
    "backup": {
        "successful": int,
        "total": int,
        "results": List[Dict]
    },
    "parse": {
        "total_jobs": int,
        "successful_files": int
    },
    "database": {
        "inserted": int,
        "updated": int,
        "failed": int
    },
    "cdc": {
        "inserted": int,
        "updated": int,
        "failed": int
    },
    "error": str  # if success=False
}
```

**Example:**
```python
import asyncio
from src.crawler.crawler import TopCVCrawler

async def main():
    crawler = TopCVCrawler({'num_pages': 3, 'use_parallel': True})
    result = await crawler.crawl()

    if result["success"]:
        print(f"Crawled {result['parse']['total_jobs']} jobs in {result['execution_time']:.2f}s")
    else:
        print(f"Failed: {result.get('error')}")

asyncio.run(main())
```

##### `run()`

```python
@classmethod
def run(cls, num_pages=None, config=None)
```

**Description:** Phương thức static để chạy crawler, xử lý asyncio

**Parameters:**
- `num_pages` (int, optional): Số trang cần crawl
- `config` (dict, optional): Config cho crawler

**Returns:**
- `dict`: Kết quả crawl (same structure as crawl() method)

**Example:**
```python
# Synchronous execution
result = TopCVCrawler.run(num_pages=3, config={'use_parallel': True})
print(f"Found {result['parse']['total_jobs']} jobs" if result["success"] else f"Failed: {result.get('error')}")
```

### 2. HTMLBackupManager

Manages HTML page backup with anti-detection measures.

#### Constructor

```python
class HTMLBackupManager:
    def __init__(self, config: Optional[Dict[str, Any]] = None)
```

#### Methods

##### `backup_html_pages()`

```python
async def backup_html_pages(self, num_pages=5, parallel=True)
```

**Description:** Backup HTML của các trang (wrapper function)

**Parameters:**
- `num_pages` (int): Số trang cần backup (default: 5)
- `parallel` (bool): True để chạy song song, False để chạy tuần tự (default: True)

**Returns:**
```python
# List[Dict] với format:

# Success case:
{
    "success": True,
    "page": int,
    "filename": str,
    "timestamp": str,  # ISO format datetime
    "size_bytes": int
}

# Error case:
{
    "success": False,
    "error": str,
    "error_type": str,  # Exception class name
    "page": int
}
```

**Example:**
```python
backup_manager = HTMLBackupManager()
results = await backup_manager.backup_html_pages(num_pages=3, parallel=True)
successful = [r for r in results if r["success"]]
print(f"Backed up {len(successful)}/{len(results)} pages")
```

##### `backup_single_page()`

```python
async def backup_single_page(self, page_num)
```

**Description:** Backup HTML của một trang với session mới và retry logic

**Parameters:**
- `page_num` (int): Số trang cần backup (1-based)

**Returns:**
- `dict`: Single page result dictionary (same format as backup_html_pages)

#### Validation Methods

##### `_validate_num_pages()`

```python
def _validate_num_pages(self, num_pages)
```

**Description:** Validate số trang crawl - Range: 1-50

**Parameters:**
- `num_pages` (int): Number of pages to validate

**Returns:**
- `int`: Validated number of pages

**Raises:**
- `ValueError`: If num_pages is not integer or outside range 1-50

##### `_validate_db_table()`

```python
def _validate_db_table(self, db_table)
```

**Description:** Validate tên bảng database

**Parameters:**
- `db_table` (str): Database table name to validate

**Returns:**
- `str`: Validated and stripped table name

**Raises:**
- `ValueError`: If db_table is not string or empty

### 3. TopCVParser

Extracts structured data from HTML files.

#### Constructor

```python
class TopCVParser:
    def __init__(self, max_workers: Optional[int] = None)
```

**Parameters:**
- `max_workers` (int, optional): ThreadPoolExecutor worker count

#### Methods

##### `parse_multiple_files()`

```python
def parse_multiple_files(self, html_files: List[Union[str, Path]]) -> pd.DataFrame
```

**Description:** Parse multiple HTML files concurrently.

**Parameters:**
- `html_files` (list): List of HTML file paths

**Returns:** pandas DataFrame with columns:
```python
{
    'job_id': str,
    'title': str,
    'company_name': str,
    'salary': str,
    'skills': str,  # JSON string
    'location': str,
    'deadline': str,
    'posted_time': datetime,
    'crawled_at': datetime
}
```

**Example:**
```python
parser = TopCVParser(max_workers=8)
html_files = ['page1.html', 'page2.html', 'page3.html']
df = parser.parse_multiple_files(html_files)

print(f"Parsed {len(df)} jobs")
print(df[['title', 'company_name', 'salary']].head())
```

##### `parse_html_file()`

```python
def parse_html_file(self, html_file: Union[str, Path]) -> List[Dict[str, Any]]
```

**Description:** Parse a single HTML file.

**Parameters:**
- `html_file` (str|Path): Path to HTML file

**Returns:** List of job dictionaries

##### `extract_job_data()`

```python
def extract_job_data(self, job_item) -> Dict[str, Any]
```

**Description:** Extract data from a single job HTML element.

**Parameters:**
- `job_item` (BeautifulSoup element): Job container element

**Returns:** Job data dictionary

### 4. CaptchaHandler

Handles anti-detection and CAPTCHA scenarios.

#### Constructor

```python
class CaptchaHandler:
    def __init__(self)
```

#### Methods

##### `detect_captcha()`

```python
def detect_captcha(self, html_content: str) -> bool
```

**Description:** Detect if page contains CAPTCHA or blocking content.

**Parameters:**
- `html_content` (str): HTML content to analyze

**Returns:** True if CAPTCHA/blocking detected

**Example:**
```python
handler = CaptchaHandler()
if handler.detect_captcha(html_content):
    print("CAPTCHA detected")
```

##### `handle_captcha()`

```python
async def handle_captcha(self, page) -> Tuple[bool, Dict[str, Any]]
```

**Description:** Handle CAPTCHA detection with retry logic.

**Parameters:**
- `page` (Playwright page): Browser page object

**Returns:**
```python
(
    success: bool,
    {
        "retry_count": int,
        "delay_applied": float,
        "error": str  # if success=False
    }
)
```

##### `apply_anti_detection()`

```python
async def apply_anti_detection(self, page) -> None
```

**Description:** Apply anti-detection measures to browser page.

**Parameters:**
- `page` (Playwright page): Browser page object

### 5. DBBulkOperations

High-performance database operations.

#### Constructor

```python
class DBBulkOperations:
    def __init__(self)
```

#### Methods

##### `bulk_upsert()`

```python
def bulk_upsert(
    self,
    df: pd.DataFrame,
    table_name: str,
    key_columns: List[str],
    schema: str = "public"
) -> Dict[str, Any]
```

**Description:** Perform bulk upsert operation using temporary tables.

**Parameters:**
- `df` (DataFrame): Data to upsert
- `table_name` (str): Target table name
- `key_columns` (list): Columns for conflict resolution
- `schema` (str): Database schema

**Returns:**
```python
{
    "success": bool,
    "inserted": int,
    "updated": int,
    "total": int,
    "execution_time": float,
    "inserted_ids": List[str],
    "updated_ids": List[str],
    "error": str  # if success=False
}
```

**Example:**
```python
db_ops = DBBulkOperations()
result = db_ops.bulk_upsert(
    df=jobs_df,
    table_name="raw_jobs",
    key_columns=["job_id"],
    schema="public"
)

print(f"Inserted: {result['inserted']}, Updated: {result['updated']}")
```

##### `bulk_insert_with_copy()`

```python
def bulk_insert_with_copy(
    self,
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public"
) -> Dict[str, Any]
```

**Description:** High-performance bulk insert using PostgreSQL COPY.

**Parameters:**
- `df` (DataFrame): Data to insert
- `table_name` (str): Target table name
- `schema` (str): Database schema

**Returns:** Operation result dictionary

## Utility Classes

### 1. UserAgentManager

Manages user agent rotation for anti-detection.

#### Methods

##### `get_random_user_agent()`

```python
def get_random_user_agent(self) -> Tuple[str, Dict[str, int]]
```

**Description:** Get random user agent with matching viewport.

**Returns:**
```python
(
    user_agent: str,
    viewport: {"width": int, "height": int}
)
```

**Example:**
```python
ua_manager = UserAgentManager.from_config()
user_agent, viewport = ua_manager.get_random_user_agent()
```

### 2. Configuration Classes

**Key configuration values:**
- `Config.Crawler.MIN_DELAY`: 4.0 seconds
- `Config.Crawler.MAX_DELAY`: 8.0 seconds
- `Config.Crawler.NUM_PAGES`: 5
- `Config.Database.RAW_JOBS_TABLE`: "raw_jobs"

## Error Handling

**Common exceptions:** `CrawlerError`, `CaptchaDetectedError`, `ParseError`, `DatabaseError`

```python
try:
    result = await crawler.crawl()
except Exception as e:
    print(f"Crawling failed: {str(e)}")
```

## Required Dependencies

**Core packages:** `playwright`, `beautifulsoup4`, `psycopg2`, `pandas`

**Main imports:**
```python
import asyncio
from src.crawler.crawler import TopCVCrawler
from src.crawler.backup_manager import HTMLBackupManager
from src.crawler.parser import TopCVParser
```

**Note:** The crawler implements fallback handling for missing dependencies.

## Complete Usage Examples

### Async và Sync Usage

```python
import asyncio
from src.crawler.crawler import TopCVCrawler

# Async usage
async def async_crawl():
    config = {'num_pages': 3, 'use_parallel': True, 'enable_cdc': True}
    crawler = TopCVCrawler(config=config)
    result = await crawler.crawl()

    if result["success"]:
        print(f"Async: {result['parse']['total_jobs']} jobs in {result['execution_time']:.2f}s")
    return result

# Sync usage
def sync_crawl():
    result = TopCVCrawler.run(num_pages=2, config={'use_parallel': False})
    if result["success"]:
        print(f"Sync: {result['parse']['total_jobs']} jobs in {result['execution_time']:.2f}s")
    return result

# Run examples
if __name__ == "__main__":
    asyncio.run(async_crawl())
    sync_crawl()
```

### Parser Usage

```python
from src.crawler.parser import TopCVParser

parser = TopCVParser(max_workers=8)
df = parser.parse_multiple_files(['backup1.html', 'backup2.html'])
print(f"Parsed {len(df)} jobs from {df['company_name'].nunique()} companies")
```

This API reference provides the foundation for developing with and extending the JobInsight Crawler system. For architectural overview, see [System Architecture](01_system_architecture.md).
