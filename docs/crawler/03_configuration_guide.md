# JobInsight Crawler - Configuration Guide

## Overview

This guide provides comprehensive configuration options for the JobInsight Crawler system, including performance tuning recommendations based on production analysis and real-world performance data.

## Configuration Overview

The crawler uses environment variables for configuration with the following precedence:
1. **Environment Variables** (highest priority)
2. **Configuration Files** (fallback)
3. **Default Values** (system defaults)

Configuration is organized into logical groups: execution settings, anti-detection parameters, performance tuning, and database connections.

## Core Configuration Parameters

### 1. Crawler Execution Settings

| Parameter | Environment Variable | Default | Description | Tuning Notes |
|-----------|---------------------|---------|-------------|--------------|
| `num_pages` | `CRAWLER_NUM_PAGES` | 5 | Number of pages to crawl | Increase for more data, decrease for faster execution |
| `use_parallel` | `CRAWLER_USE_PARALLEL` | true | Enable concurrent page processing | Always recommended for performance |
| `enable_cdc` | `CRAWLER_ENABLE_CDC` | true | Enable Change Data Capture logging | Required for ETL pipeline integration |

**Example Configuration:**
```bash
# Environment variables
export CRAWLER_NUM_PAGES=5
export CRAWLER_USE_PARALLEL=true
export CRAWLER_ENABLE_CDC=true
```

### 2. Anti-Detection Settings

| Parameter | Environment Variable | Default | Description | Production Recommendations |
|-----------|---------------------|---------|-------------|---------------------------|
| `min_delay` | `CRAWLER_MIN_DELAY` | 4.0 | Minimum delay between requests (seconds) | **Optimize to 3.0** for better performance |
| `max_delay` | `CRAWLER_MAX_DELAY` | 8.0 | Maximum delay between requests (seconds) | **Optimize to 6.0** for better performance |
| `concurrent_backups` | N/A | min(5, max(3, cpu_count)) | Number of concurrent page backups | **Dynamic based on CPU cores** |
| `max_retry` | `CRAWLER_MAX_RETRY` | 3 | Maximum retry attempts per page | Increase to 4 for better reliability |

**⚠️ Note**: Some modules have fallback configurations that may differ from main config.
Check `src/crawler/backup_manager.py` lines 41-42 for fallback values (MIN_DELAY=3, MAX_DELAY=6).

**Optimized Configuration (Based on Analysis):**
```bash
# Recommended optimizations
export CRAWLER_MIN_DELAY=3.0      # Reduced from 4.0
export CRAWLER_MAX_DELAY=6.0       # Reduced from 8.0
export CRAWLER_MAX_RETRY=4         # Increased from 3

# Note: concurrent_backups is calculated dynamically in code
# To override, modify the config dict when initializing crawler:
# config = {'concurrent_backups': 4}  # Override dynamic calculation
```

### 3. Browser & Timeout Settings

| Parameter | Environment Variable | Default | Description | Notes |
|-----------|---------------------|---------|-------------|-------|
| `page_load_timeout` | `CRAWLER_PAGE_LOAD_TIMEOUT` | 60000 | Page load timeout (milliseconds) | Increase if pages load slowly |
| `selector_timeout` | `CRAWLER_SELECTOR_TIMEOUT` | 20000 | Element selector timeout (milliseconds) | Current value appropriate |
| `max_workers` | `CRAWLER_MAX_WORKERS` | 10 | ThreadPoolExecutor worker count | Auto-calculated based on CPU cores |

### 4. Database Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `db_table` | N/A | raw_jobs | Target database table (set via config) |
| `db_schema` | N/A | None | Database schema (optional) |
| `batch_size` | `RAW_BATCH_SIZE` | 20 | Bulk operation batch size |

### 5. Advanced Configuration Parameters

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `base_url` | `CRAWLER_BASE_URL` | "https://www.topcv.vn/viec-lam-it" | Base URL for crawling |
| `topcv_search_url` | N/A | "https://www.topcv.vn/tim-viec-lam-moi-nhat" | Search URL pattern |
| `crawl_delay` | `CRAWL_DELAY` | 2 | Additional delay between operations |
| `max_jobs_per_crawl` | `MAX_JOBS_PER_CRAWL` | 50 | Maximum jobs to process per crawl |
| `retry_delays` | N/A | [2, 4, 8] | Backoff delays for retries (seconds) |

### 6. Threading Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `max_workers` | `MAX_WORKERS` | min(32, cpu_count * 5) | ThreadPoolExecutor worker count |
| `concurrent_backups` | N/A | min(5, max(3, cpu_count)) | Concurrent backup operations |

### 7. CDC Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `cdc_days_to_keep` | `CDC_DAYS_TO_KEEP` | 15 | Days to keep CDC files |
| `cdc_file_lock_timeout` | `CDC_LOCK_TIMEOUT` | 10 | File lock timeout (seconds) |

## User Agent Configuration

### User Agent Pool Management

```python
# User agent distribution (based on crawler_config.py)
USER_AGENTS = [
    # Desktop Chrome (70% of pool)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
    # Desktop Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101...",
    # Mobile agents (30% of pool)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X)...",
]

# Viewport configurations
VIEWPORTS = {
    "desktop": {"width": 1366, "height": 768},
    "mobile": {"width": 390, "height": 844}
}
```

### Customizing User Agents

```python
# Add custom user agents
CUSTOM_USER_AGENTS = [
    "Your-Custom-User-Agent-String",
    # Add more as needed
]

# Configuration
config = {
    'user_agents': CUSTOM_USER_AGENTS,
    'desktop_ratio': 0.7,  # 70% desktop, 30% mobile
}
```

## Performance Tuning Guide

### Based on Production Analysis

**Current Performance Bottlenecks:**
- **HTML Backup Phase**: 92% of execution time (~102s out of 111s)
- **Parse Success Rate**: Only 20% (1/5 pages successful)
- **CAPTCHA Detection**: 80% of pages trigger anti-bot measures

### Recommended Optimizations

#### 1. Reduce Execution Time
```bash
# Optimize delays (can reduce ~20-30% execution time)
export CRAWLER_MIN_DELAY=3.0
export CRAWLER_MAX_DELAY=6.0

# Increase concurrency (test carefully)
export CRAWLER_CONCURRENT_BACKUPS=4
```

#### 2. Improve Parse Success Rate
```bash
# Increase retry attempts
export CRAWLER_MAX_RETRY=4

# Adjust CAPTCHA detection sensitivity
export CAPTCHA_MIN_CONTENT_LENGTH=800  # Reduced from 1000
```

#### 3. Circuit Breaker Tuning
```python
# In HTMLBackupManager configuration
circuit_breaker_config = {
    'failure_threshold': 3,      # Trigger after 3 failures
    'pause_duration': 300,       # 5 minutes pause
    'recovery_threshold': 2,     # Resume after 2 successes
}
```

## Environment-Specific Configurations

### Development Environment
```bash
# Faster execution for development
export CRAWLER_NUM_PAGES=2
export CRAWLER_MIN_DELAY=1.0
export CRAWLER_MAX_DELAY=3.0
export CRAWLER_CONCURRENT_BACKUPS=2
export CRAWLER_ENABLE_CDC=false
```

### Production Environment
```bash
# Optimized production settings
export CRAWLER_NUM_PAGES=5
export CRAWLER_MIN_DELAY=3.0
export CRAWLER_MAX_DELAY=6.0
export CRAWLER_CONCURRENT_BACKUPS=4
export CRAWLER_MAX_RETRY=4
export CRAWLER_ENABLE_CDC=true

# Database optimization
export CRAWLER_BATCH_SIZE=1000
export CRAWLER_MAX_WORKERS=16
```

### Testing Environment
```bash
# Conservative settings for testing
export CRAWLER_NUM_PAGES=1
export CRAWLER_MIN_DELAY=5.0
export CRAWLER_MAX_DELAY=10.0
export CRAWLER_CONCURRENT_BACKUPS=1
export CRAWLER_USE_PARALLEL=false
```

## Advanced Configuration

### Custom Configuration Files

Create `config/crawler.json`:
```json
{
  "crawler": {
    "num_pages": 5,
    "use_parallel": true,
    "min_delay": 3.0,
    "max_delay": 6.0,
    "concurrent_backups": 4,
    "anti_detection": {
      "user_agent_rotation": true,
      "fingerprint_spoofing": true,
      "behavioral_simulation": true
    },
    "performance": {
      "max_workers": 16,
      "batch_size": 1000,
      "memory_limit_mb": 2048
    }
  }
}
```

### Runtime Configuration Override

```python
# Override configuration at runtime
runtime_config = {
    'num_pages': 3,
    'min_delay': 2.0,
    'max_delay': 4.0,
    'concurrent_backups': 5,
}

crawler = TopCVCrawler(config=runtime_config)
result = await crawler.crawl()
```

## Monitoring Configuration

### Logging Configuration
```python
# Configure detailed logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['console', 'file'],
    'performance_metrics': True,
    'error_tracking': True,
}
```

### Metrics Collection
```bash
# Enable performance metrics
export CRAWLER_ENABLE_METRICS=true
export CRAWLER_METRICS_INTERVAL=10  # seconds
export CRAWLER_METRICS_OUTPUT=/opt/airflow/logs/crawler_metrics.log
```

## Configuration Validation

### Automatic Validation
```python
def validate_crawler_config(config):
    """Validate crawler configuration parameters"""
    errors = []
    
    # Validate num_pages
    if not 1 <= config.get('num_pages', 5) <= 50:
        errors.append("num_pages must be between 1 and 50")
    
    # Validate delays
    min_delay = config.get('min_delay', 4.0)
    max_delay = config.get('max_delay', 8.0)
    if min_delay >= max_delay:
        errors.append("min_delay must be less than max_delay")
    
    # Validate concurrency
    concurrent_backups = config.get('concurrent_backups', 3)
    if not 1 <= concurrent_backups <= 10:
        errors.append("concurrent_backups must be between 1 and 10")
    
    return errors
```

## Troubleshooting Configuration Issues

### Common Configuration Problems

1. **High CAPTCHA Detection Rate**
   ```bash
   # Increase delays
   export CRAWLER_MIN_DELAY=5.0
   export CRAWLER_MAX_DELAY=10.0

   # Reduce concurrency (set via config dict)
   config = {'concurrent_backups': 2}
   crawler = TopCVCrawler(config=config)
   ```

2. **Slow Execution Time**
   ```bash
   # Optimize delays (carefully)
   export CRAWLER_MIN_DELAY=2.0
   export CRAWLER_MAX_DELAY=4.0

   # Increase concurrency (test thoroughly, set via config dict)
   config = {'concurrent_backups': 5}
   crawler = TopCVCrawler(config=config)
   ```

3. **Parse Failures**
   ```bash
   # Increase timeouts
   export CRAWLER_PAGE_LOAD_TIMEOUT=90000
   export CRAWLER_SELECTOR_TIMEOUT=30000
   
   # Increase retries
   export CRAWLER_MAX_RETRY=5
   ```

## Configuration Best Practices

1. **Start Conservative**: Begin with default settings and optimize gradually
2. **Monitor Metrics**: Track success rates and execution times
3. **Test Changes**: Validate configuration changes in development first
4. **Document Changes**: Keep track of configuration modifications
5. **Environment Separation**: Use different configs for dev/staging/prod

For performance analysis and optimization strategies, see [Performance Analysis Guide](04_performance_analysis.md).
