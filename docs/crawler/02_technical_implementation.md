# JobInsight Crawler - Technical Implementation Guide

## Overview

This guide provides detailed technical implementation details for each component of the JobInsight Crawler system, based on deep analysis of the production codebase and real-world performance data.

## 1. TopCVCrawler - Main Orchestrator

### Overview
TopCVCrawler is the central coordinator that orchestrates the entire crawling process using a 4-phase pipeline. It implements the Facade pattern to provide a simple interface for complex operations.

### Usage Example
```python
import asyncio
from src.crawler.crawler import TopCVCrawler

async def main():
    # Basic usage
    crawler = TopCVCrawler()
    result = await crawler.crawl()

    # With custom configuration
    config = {'num_pages': 3, 'use_parallel': True}
    crawler = TopCVCrawler(config=config)
    result = await crawler.crawl()

    # Check results
    if result["success"]:
        print(f"Crawled {result['parse']['total_jobs']} jobs successfully")
    else:
        print(f"Crawling failed: {result.get('error')}")

# Alternative: Using classmethod for synchronous execution
def sync_usage():
    result = TopCVCrawler.run(num_pages=2)
    return result

if __name__ == "__main__":
    asyncio.run(main())
```

### 4-Phase Execution Pipeline

The crawler executes in four sequential phases:

1. **HTML Backup**: Concurrent download of web pages with anti-detection measures
2. **HTML Parsing**: Extract structured data from downloaded HTML files
3. **Database Operations**: Bulk insert/update operations with conflict resolution
4. **CDC Logging**: Track changes for downstream ETL pipeline consumption

Each phase includes comprehensive error handling and can recover from partial failures.

### Error Handling Strategy

The system implements multi-layer error handling:
- **Component-level**: Each component handles its specific error types
- **Pipeline-level**: Orchestrator manages overall execution flow
- **Recovery mechanisms**: Retry logic, circuit breakers, and graceful degradation
- **Comprehensive logging**: Detailed error tracking for troubleshooting

## 2. HTMLBackupManager - Web Scraping Engine

### Concurrent Backup Architecture

```python
class HTMLBackupManager:
    def __init__(self, config=None):
        self.config = config or {}

        # Concurrency control - Dynamic calculation
        self.concurrent_backups = self.config.get('concurrent_backups',
                                                min(5, max(3, (os.cpu_count() or 1))))
        self._semaphore = None  # Initialized during execution

        # Anti-detection configuration
        self.min_delay = self.config.get('min_delay', 4.0)  # seconds
        self.max_delay = self.config.get('max_delay', 8.0)  # seconds
        self.max_retry = self.config.get('max_retry', 3)

        # Note: Fallback values in backup_manager.py may differ:
        # MIN_DELAY = 3, MAX_DELAY = 6 (lines 41-42)
```

### Semaphore-Controlled Concurrency

```python
async def backup_html_pages_parallel(self, num_pages=5):
    # Initialize semaphore for concurrency control
    self._semaphore = asyncio.Semaphore(self.concurrent_backups)
    
    async def backup_with_semaphore(page_num):
        async with self._semaphore:
            return await self.backup_single_page(page_num)
    
    # Create tasks for all pages
    tasks = [backup_with_semaphore(page_num) for page_num in range(1, num_pages + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
```

### Circuit Breaker Implementation

```python
# Circuit breaker pattern for IP protection
failed = num_pages - successful
if failed >= 3 and num_pages >= 4:
    logger.warning(f"Circuit Breaker triggered: {failed}/{num_pages} pages failed")
    await asyncio.sleep(300)  # Pause 5 minutes
    logger.info("Circuit Breaker: Resuming after pause")
```

### Playwright Browser Management

```python
async def _backup_page_impl(self, page_num, user_agent, viewport):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport=viewport,
        )
        
        page = await context.new_page()
        
        # Apply anti-detection measures
        await self.captcha_handler.apply_anti_detection(page)
        
        # Navigate with timeout
        await page.goto(url, wait_until='domcontentloaded', 
                       timeout=self.page_load_timeout)
```

## 3. CaptchaHandler - Anti-Detection System

### Pattern-Based Detection

```python
class CaptchaHandler:
    def __init__(self):
        self.captcha_patterns = [
            r'captcha', r'robot', r'automated\s+access',
            r'unusual\s+traffic', r'suspicious\s+activity',
            r'access\s+denied', r'security\s+check'
        ]
        self.captcha_regex = re.compile('|'.join(self.captcha_patterns), re.IGNORECASE)
```

### Multi-Level Detection Logic

```python
def detect_captcha(self, html_content: str) -> bool:
    # 1. Pattern-based detection
    if self.captcha_regex.search(html_content):
        return True
    
    # 2. Content length validation
    if len(html_content) < 1000:
        logger.warning(f"HTML content too short ({len(html_content)} bytes)")
        return True
    
    # 3. Expected element validation
    if "<div class='job-item-2'" not in html_content:
        logger.warning("Expected job elements not found")
        return True
    
    return False
```

### Exponential Backoff Retry

```python
async def handle_captcha(self, page) -> Tuple[bool, Dict[str, Any]]:
    self.retry_count += 1
    
    if self.retry_count > self.max_retries:
        return False, {"error": "max_retries_exceeded"}
    
    # Exponential backoff with jitter
    delay = self.delay_between_retries[min(self.retry_count - 1, len(self.delay_between_retries) - 1)]
    actual_delay = delay + random.uniform(0, 2)
    
    await asyncio.sleep(actual_delay)
```

### Browser Fingerprint Spoofing

```python
async def apply_anti_detection(self, page):
    await page.add_init_script("""
        () => {
            // Hide webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false
            });
            
            // Simulate Chrome environment
            window.chrome = {
                runtime: {}, loadTimes: function() {}, csi: function() {}
            };
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const plugins = [];
                    for (let i = 0; i < 5; i++) {
                        plugins.push({
                            name: `Plugin ${i}`,
                            description: `Plugin description ${i}`
                        });
                    }
                    return plugins;
                }
            });
        }
    """)
```

## 4. TopCVParser - Data Extraction Engine

### Multi-Threaded Parsing Architecture

```python
class TopCVParser:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) * 5)
        self._job_id_processed: Set[str] = set()
        self._job_data_lock = threading.Lock()
        self._max_processed_ids = 10000  # Memory leak prevention
```

### Concurrent File Processing

```python
def parse_multiple_files(self, html_files: List[Union[str, Path]]) -> pd.DataFrame:
    all_jobs = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        # Submit all files for parsing
        future_to_file = {
            executor.submit(self.parse_html_file, file): file 
            for file in html_files
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_file):
            try:
                jobs_from_file = future.result()
                if jobs_from_file:
                    all_jobs.extend(jobs_from_file)
            except Exception as e:
                logger.error(f"Error processing file: {str(e)}")
```

### Robust HTML Parsing with Fallbacks

```python
def extract_job_data(self, job_item) -> Dict[str, Any]:
    # Multi-selector approach with fallbacks
    
    # Title extraction with fallback
    title_span = job_item.select_one('h3.title a span[data-original-title]')
    if title_span and title_span.has_attr('data-original-title'):
        job_data['title'] = title_span['data-original-title'].strip()
    else:
        # Fallback to text content
        title_elem = job_item.find('h3', class_='title')
        if title_elem:
            job_data['title'] = title_elem.get_text(strip=True)
```

### Memory Management

```python
def _cleanup_processed_ids(self):
    """Prevent memory leaks in long-running processes"""
    current_size = len(self._job_id_processed)
    if current_size > self._max_processed_ids:
        logger.info(f"Memory cleanup triggered: {current_size} > {self._max_processed_ids}")
        self._job_id_processed.clear()
```

## 5. DBBulkOperations - High-Performance Database Layer

### PostgreSQL COPY-Based Bulk Insert

```python
def bulk_insert_with_copy(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    with conn.cursor() as cur:
        # Prepare CSV data with proper escaping
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False,
                 quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        output.seek(0)
        
        # Execute COPY operation
        columns = ", ".join([f'"{col}"' for col in df.columns])
        copy_query = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')"
        cur.copy_expert(copy_query, output)
```

### Sophisticated Upsert with Temporary Tables

```python
def bulk_upsert(self, df: pd.DataFrame, table_name: str, key_columns: List[str]):
    with conn.cursor() as cur:
        # 1. Create temporary table
        temp_table = f"tmp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        cur.execute(f"CREATE TEMP TABLE {temp_table} AS SELECT * FROM {table_name} WHERE 1=0")
        
        # 2. COPY data to temp table
        # ... (COPY operation as above)
        
        # 3. UPSERT from temp to main table
        conflict_constraint = ", ".join([f'"{col}"' for col in key_columns])
        update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        
        upsert_query = f"""
            INSERT INTO {table_name} ({columns})
            SELECT {columns} FROM {temp_table}
            ON CONFLICT ({conflict_constraint})
            DO UPDATE SET {update_clause}
            RETURNING (xmax = 0) AS inserted, (xmax <> 0) AS updated;
        """
        
        cur.execute(upsert_query)
        results = cur.fetchall()
```

## Performance Characteristics

### Measured Performance (Production Data)
- **Total Execution**: ~111 seconds
- **HTML Backup**: ~102 seconds (92% of time)
- **HTML Parsing**: ~8 seconds (7% of time)
- **Database Ops**: ~0.3 seconds (0.3% of time)

### Optimization Opportunities
1. **Reduce backup delays**: Current 4-8s could be optimized to 3-6s
2. **Increase concurrency**: Test 4-5 concurrent backups vs current 3
3. **Optimize selectors**: Improve parse success rate from current 20%
4. **Cache management**: Implement intelligent HTML caching

## Error Handling Patterns

### Retry Decorators
```python
@retry(max_tries=3, delay_seconds=1.0, backoff_factor=2.0)
def parse_html_file(self, html_file):
    # Implementation with automatic retry on failure
```

### Transaction Management
```python
try:
    with conn.cursor() as cur:
        # Database operations
        conn.commit()
except Exception as e:
    conn.rollback()
    logger.error(f"Database error: {str(e)}")
    raise
```

This implementation guide provides the technical foundation for understanding and maintaining the crawler system. For configuration details, see the [Configuration Guide](03_configuration_guide.md).
