# JobInsight Crawler - Troubleshooting Guide

## Overview

This guide provides solutions for common issues encountered in the JobInsight Crawler system, based on real production logs and error patterns observed in the field.

## Common Issues and Solutions

### 1. High CAPTCHA Detection Rate (80% of pages)

**Symptoms:**
```
[2025-08-02T15:45:41.712] WARNING - Phát hiện captcha/block pattern trong nội dung HTML
[2025-08-02T15:45:41.811] INFO - Xử lý captcha/block, retry 1/4, delay 4.01s
```

**Root Causes:**
- Anti-bot detection triggered by request patterns
- User-Agent or fingerprint detection
- Rate limiting by target website

**Solutions:**

#### Immediate Fix (Conservative Approach)
```bash
# Increase delays to appear more human-like
export CRAWLER_MIN_DELAY=6.0
export CRAWLER_MAX_DELAY=12.0

# Reduce concurrency
export CRAWLER_CONCURRENT_BACKUPS=2

# Increase retry attempts
export CRAWLER_MAX_RETRY=5
```

#### Medium-term Fix (Optimize Detection)
```python
# Adjust CAPTCHA detection sensitivity in captcha_handler.py
def detect_captcha(self, html_content: str) -> bool:
    # Reduce threshold (current: 1000 bytes)
    if len(html_content) < 800:  # Reduced from 1000
        return True
    
    # Add more specific patterns
    job_indicators = [
        "<div class='job-item-2'",
        "data-job-id=",
        "class=\"job-title\"",
        "salary-label"
    ]
    
    # Require at least 2 job indicators
    found_indicators = sum(1 for indicator in job_indicators if indicator in html_content)
    return found_indicators < 2
```

#### Long-term Fix (Enhanced Anti-Detection)
```python
# Implement more sophisticated user agent rotation
class EnhancedUserAgentManager:
    def get_user_agent_with_session(self):
        # Maintain session consistency
        # Rotate user agents less frequently
        # Add browser-specific headers
        pass
```

### 2. Low Parse Success Rate (20%)

**Symptoms:**
```
[2025-08-02T15:45:42.123] INFO - Parsed 0 jobs from page 1
[2025-08-02T15:45:42.234] INFO - Parsed 0 jobs from page 2
[2025-08-02T15:45:42.345] INFO - Parsed 50 jobs from page 4
```

**Root Causes:**
- HTML structure changes on target website
- Selector timeouts
- Content blocked by anti-bot measures

**Solutions:**

#### Immediate Diagnosis
```python
# Add debug logging to parser
def extract_job_data(self, job_item) -> Dict[str, Any]:
    logger.debug(f"Processing job item: {job_item.get('class', 'no-class')}")
    
    # Log selector attempts
    title_span = job_item.select_one('h3.title a span[data-original-title]')
    if not title_span:
        logger.warning(f"Primary title selector failed, trying fallback")
        # Try fallback selectors
```

#### Selector Robustness
```python
# Implement multi-level fallback selectors
SELECTOR_FALLBACKS = {
    'job_title': [
        'h3.title a span[data-original-title]',  # Primary
        'h3.title a',                            # Fallback 1
        '.job-title',                            # Fallback 2
        '[data-job-title]',                      # Fallback 3
        'h3 a'                                   # Last resort
    ],
    'company_name': [
        '.company-name a',
        '.company-name',
        '[data-company]',
        '.employer-name'
    ]
}
```

#### Timeout Optimization
```bash
# Increase timeouts for slow-loading pages
export CRAWLER_PAGE_LOAD_TIMEOUT=90000  # 90 seconds
export CRAWLER_SELECTOR_TIMEOUT=30000   # 30 seconds
```

### 3. Slow Execution Time (111+ seconds)

**Symptoms:**
```
[2025-08-02T15:47:32.123] INFO - Crawler execution completed in 111.06 seconds
```

**Root Causes:**
- Excessive delays between requests (92% of time in backup phase)
- Conservative anti-detection settings
- Sequential processing bottlenecks

**Solutions:**

#### Performance Optimization
```bash
# Optimize delays (test carefully)
export CRAWLER_MIN_DELAY=3.0  # Reduced from 4.0
export CRAWLER_MAX_DELAY=6.0   # Reduced from 8.0

# Increase concurrency
export CRAWLER_CONCURRENT_BACKUPS=4  # Increased from 3

# Optimize timeouts
export CRAWLER_PAGE_LOAD_TIMEOUT=45000  # Reduced from 60000
```

#### Parallel Processing Enhancement
```python
# Implement smarter concurrency control
class AdaptiveConcurrencyManager:
    def __init__(self):
        self.success_rate = 1.0
        self.base_concurrency = 3
        
    def get_optimal_concurrency(self):
        if self.success_rate > 0.8:
            return min(self.base_concurrency + 1, 6)
        elif self.success_rate < 0.5:
            return max(self.base_concurrency - 1, 1)
        return self.base_concurrency
```

### 4. Database Connection Issues

**Symptoms:**
```
psycopg2.OperationalError: could not connect to server
```

**Solutions:**

#### Connection Pool Configuration
```python
# Implement connection pooling
from psycopg2 import pool

class DatabaseManager:
    def __init__(self):
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=Config.Database.HOST,
            database=Config.Database.NAME,
            user=Config.Database.USER,
            password=Config.Database.PASSWORD
        )
```

#### Retry Logic for Database Operations
```python
@retry(max_tries=3, delay_seconds=2.0, backoff_factor=2.0)
def bulk_upsert(self, df, table_name, key_columns):
    try:
        # Database operations
        pass
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
```

### 5. Memory Issues

**Symptoms:**
```
MemoryError: Unable to allocate array
```

**Solutions:**

#### Memory Management
```python
# Implement memory cleanup in parser
class TopCVParser:
    def __init__(self):
        self._max_processed_ids = 5000  # Reduced from 10000
        
    def _cleanup_processed_ids(self):
        if len(self._job_id_processed) > self._max_processed_ids:
            # Keep only recent 50% of IDs
            recent_ids = list(self._job_id_processed)[-self._max_processed_ids//2:]
            self._job_id_processed = set(recent_ids)
```

#### Batch Processing
```python
# Process files in smaller batches
def parse_multiple_files(self, html_files: List[str]) -> pd.DataFrame:
    batch_size = 10  # Process 10 files at a time
    all_jobs = []
    
    for i in range(0, len(html_files), batch_size):
        batch = html_files[i:i + batch_size]
        batch_jobs = self._parse_file_batch(batch)
        all_jobs.extend(batch_jobs)
        
        # Force garbage collection
        import gc
        gc.collect()
```

## Error Pattern Analysis

### 1. Retry Pattern Analysis

**Common Retry Scenarios:**
```python
# From production logs analysis
retry_patterns = {
    "captcha_detection": {
        "frequency": "80% of pages",
        "typical_retries": 2-3,
        "success_after_retry": "30%"
    },
    "timeout_errors": {
        "frequency": "15% of pages", 
        "typical_retries": 1-2,
        "success_after_retry": "70%"
    },
    "network_errors": {
        "frequency": "5% of pages",
        "typical_retries": 1,
        "success_after_retry": "90%"
    }
}
```

### 2. Circuit Breaker Activation

**When Circuit Breaker Triggers:**
```python
# Production pattern: 3+ failures in 5 pages
if failed_pages >= 3 and total_pages >= 4:
    logger.warning("Circuit breaker activated - pausing 5 minutes")
    # Typical recovery: 60% success rate after pause
```

## Monitoring and Alerting

### 1. Key Metrics to Monitor

```python
# Critical metrics for alerting
ALERT_THRESHOLDS = {
    "duration_seconds": {
        "warning": 120,    # seconds
        "critical": 180    # seconds
    },
    "parse_success_rate": {
        "warning": 15,     # percent
        "critical": 10     # percent
    },
    "captcha_detection_rate": {
        "warning": 70,     # percent
        "critical": 90     # percent
    },
    "memory_usage": {
        "warning": 1500,   # MB
        "critical": 2000   # MB
    }
}
```

### 2. Log Analysis Queries

```bash
# Find CAPTCHA detection patterns
grep "captcha/block pattern" /opt/airflow/logs/dag_id=crawl_topcv_jobs/*/task_id=crawl_and_process/attempt=*.log

# Analyze execution times
grep "Crawler execution completed" /opt/airflow/logs/dag_id=crawl_topcv_jobs/*/task_id=crawl_and_process/attempt=*.log

# Check parse success rates
grep "Parsed.*jobs from page" /opt/airflow/logs/dag_id=crawl_topcv_jobs/*/task_id=crawl_and_process/attempt=*.log
```

## Diagnostic Tools

### 1. Health Check Script

```python
#!/usr/bin/env python3
"""Crawler health check script"""

def check_crawler_health():
    checks = {
        "database_connection": check_db_connection(),
        "target_website_accessibility": check_website_access(),
        "configuration_validity": validate_configuration(),
        "recent_execution_success": check_recent_runs(),
        "resource_availability": check_system_resources()
    }
    
    return checks

def check_db_connection():
    try:
        # Test database connection
        return {"status": "healthy", "latency_ms": 45}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

### 2. Performance Profiler

```python
import cProfile
import pstats

def profile_crawler_execution():
    """Profile crawler execution to identify bottlenecks"""
    profiler = cProfile.Profile()
    profiler.enable()
    
    # Run crawler
    crawler = TopCVCrawler()
    result = crawler.crawl()
    
    profiler.disable()
    
    # Analyze results
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(20)  # Top 20 functions by time
```

## Emergency Procedures

### 1. Complete System Failure

```bash
# Emergency recovery steps
1. Check system resources: df -h, free -m, top
2. Verify database connectivity: psql -h postgres -U jobinsight
3. Check Airflow status: docker ps | grep airflow
4. Review recent logs: tail -f /opt/airflow/logs/dag_id=crawl_topcv_jobs/*/task_id=crawl_and_process/attempt=*.log
5. Restart crawler DAG: airflow dags unpause crawl_topcv_jobs
```

### 2. Data Quality Issues

```python
# Data validation script
def validate_crawled_data():
    """Validate recent crawler data quality"""
    
    # Check for duplicate job_ids
    duplicates = check_duplicate_jobs()
    
    # Validate required fields
    missing_fields = check_missing_required_fields()
    
    # Check data freshness
    stale_data = check_data_freshness()
    
    return {
        "duplicates": duplicates,
        "missing_fields": missing_fields,
        "stale_data": stale_data
    }
```

## Best Practices for Issue Prevention

1. **Regular Monitoring**: Check logs daily for error patterns
2. **Configuration Testing**: Test configuration changes in development first
3. **Gradual Optimization**: Make incremental performance improvements
4. **Backup Strategies**: Maintain configuration backups
5. **Documentation**: Keep troubleshooting logs for future reference

For API reference and development guidelines, see [API Reference Guide](06_api_reference.md).
