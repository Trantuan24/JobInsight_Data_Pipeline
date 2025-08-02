# JobInsight Crawler System - Configuration & Operations Guide

## Overview

This guide provides comprehensive operational guidance for deploying, configuring, monitoring, and troubleshooting the JobInsight Crawler System in production environments.

## System Requirements

### Hardware Requirements

**Minimum Configuration**:
- **CPU**: 4 cores, 2.4GHz
- **Memory**: 8GB RAM
- **Storage**: 50GB available space
- **Network**: Stable internet connection (10Mbps+)

**Recommended Configuration**:
- **CPU**: 8+ cores, 3.0GHz+
- **Memory**: 16GB+ RAM
- **Storage**: 100GB+ SSD storage
- **Network**: High-speed internet (50Mbps+)

### Software Dependencies

**Core Dependencies**:
- **Python**: 3.9+ (tested with 3.9, 3.10, 3.11)
- **PostgreSQL**: 13+ (recommended: 14+)
- **Apache Airflow**: 2.5+ (tested with 2.7+)
- **Docker**: 20.10+ (for containerized deployment)

**Python Packages**:
- **playwright**: 1.40+ (browser automation)
- **pandas**: 1.5+ (data processing)
- **psycopg2**: 2.9+ (PostgreSQL connectivity)
- **beautifulsoup4**: 4.11+ (HTML parsing)
- **filelock**: 3.12+ (concurrent file access)

## Configuration Management

### Environment Variables

**Database Configuration**:
```bash
# PostgreSQL Connection
DB_HOST=postgres                    # Database host
DB_PORT=5432                       # Database port
DB_USER=jobinsight                 # Database username
DB_PASSWORD=jobinsight             # Database password
DB_NAME=jobinsight                 # Database name

# Connection Pool Settings
DB_POOL_SIZE=10                    # Connection pool size
DB_MAX_OVERFLOW=20                 # Maximum overflow connections
```

**Crawler Configuration**:
```bash
# Target Website Settings
CRAWLER_BASE_URL=https://www.topcv.vn/viec-lam-it
CRAWLER_NUM_PAGES=5                # Number of pages to crawl

# Performance Tuning
CRAWLER_MIN_DELAY=3                # Minimum delay between requests (seconds)
CRAWLER_MAX_DELAY=6                # Maximum delay between requests (seconds)
MAX_WORKERS=10                     # Maximum worker threads
CONCURRENT_BACKUPS=3               # Concurrent backup operations

# Anti-Detection Settings
CRAWLER_PAGE_LOAD_TIMEOUT=60000    # Page load timeout (milliseconds)
CRAWLER_SELECTOR_TIMEOUT=20000     # Element selector timeout (milliseconds)
CRAWLER_MAX_RETRY=3                # Maximum retry attempts
```

**CDC Configuration**:
```bash
# Change Data Capture Settings
CDC_DAYS_TO_KEEP=15               # Days to retain CDC files
CDC_LOCK_TIMEOUT=10               # File lock timeout (seconds)
```

**Airflow Configuration**:
```bash
# Airflow Settings
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://jobinsight:jobinsight@postgres:5432/jobinsight
```

### Configuration Templates

#### **Development Environment (.env.development)**
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=jobinsight
DB_PASSWORD=jobinsight
DB_NAME=jobinsight

# Crawler Settings - Conservative for development
CRAWLER_NUM_PAGES=2          # Small number for testing
CRAWLER_MIN_DELAY=2          # Shorter delays for faster testing
CRAWLER_MAX_DELAY=4
MAX_WORKERS=3                # Fewer workers for development
CONCURRENT_BACKUPS=1         # Sequential processing

# Directories
BACKUP_DIR=./data/raw_backup
CDC_DIR=./data/cdc

# CDC Settings
CDC_DAYS_TO_KEEP=7           # Shorter retention for development
CDC_LOCK_TIMEOUT=5

# Logging
LOG_LEVEL=DEBUG              # Verbose logging for development
```

#### **Production Environment (.env.production)**
```bash
# Database Configuration
DB_HOST=postgres-cluster.internal
DB_PORT=5432
DB_USER=jobinsight_prod
DB_PASSWORD=${DB_PASSWORD_SECRET}
DB_NAME=jobinsight_prod

# Connection Pool Settings
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=30

# Crawler Settings - Optimized for production
CRAWLER_NUM_PAGES=5          # Full page crawling
CRAWLER_MIN_DELAY=3          # Balanced delays
CRAWLER_MAX_DELAY=6
MAX_WORKERS=10               # Full worker utilization
CONCURRENT_BACKUPS=3         # Parallel processing

# Anti-Detection Settings
CRAWLER_PAGE_LOAD_TIMEOUT=60000
CRAWLER_SELECTOR_TIMEOUT=20000
CRAWLER_MAX_RETRY=3

# Directories
BACKUP_DIR=/app/data/raw_backup
CDC_DIR=/app/data/cdc

# CDC Settings
CDC_DAYS_TO_KEEP=30          # Longer retention for production
CDC_LOCK_TIMEOUT=10

# Logging
LOG_LEVEL=INFO               # Standard logging for production

# Airflow Production Settings
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CELERY__BROKER_URL=redis://redis-cluster:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://jobinsight_prod:${DB_PASSWORD_SECRET}@postgres-cluster.internal:5432/jobinsight_prod
```

#### **Testing Environment (.env.testing)**
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5433                 # Different port for test database
DB_USER=jobinsight_test
DB_PASSWORD=jobinsight_test
DB_NAME=jobinsight_test

# Crawler Settings - Minimal for testing
CRAWLER_NUM_PAGES=1          # Single page for unit tests
CRAWLER_MIN_DELAY=1          # Minimal delays for fast tests
CRAWLER_MAX_DELAY=2
MAX_WORKERS=1                # Single worker for predictable tests
CONCURRENT_BACKUPS=1         # Sequential processing

# Directories
BACKUP_DIR=./tests/data/raw_backup
CDC_DIR=./tests/data/cdc

# CDC Settings
CDC_DAYS_TO_KEEP=1           # Minimal retention for tests
CDC_LOCK_TIMEOUT=5

# Logging
LOG_LEVEL=WARNING            # Minimal logging for tests
```

#### **Docker Compose Template (docker-compose.yml)**
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: jobinsight
      POSTGRES_USER: jobinsight
      POSTGRES_PASSWORD: jobinsight
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U jobinsight"]
      interval: 30s
      timeout: 10s
      retries: 3

  jobinsight:
    build: .
    environment:
      - DB_HOST=postgres
      - CRAWLER_NUM_PAGES=5
      - CRAWLER_MIN_DELAY=3
      - CRAWLER_MAX_DELAY=6
      - MAX_WORKERS=10
      - CONCURRENT_BACKUPS=3
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped

  airflow-webserver:
    build: .
    command: airflow webserver
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://jobinsight:jobinsight@postgres:5432/jobinsight
      - AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-scheduler:
    build: .
    command: airflow scheduler
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://jobinsight:jobinsight@postgres:5432/jobinsight
      - AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

volumes:
  postgres_data:
```

#### **Production Docker Compose (docker-compose.production.yml)**
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: jobinsight_prod
      POSTGRES_USER: jobinsight_prod
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
    secrets:
      - db_password
    volumes:
      - postgres_prod_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    networks:
      - jobinsight_network
    deploy:
      replicas: 1
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 1G

  jobinsight:
    image: jobinsight:latest
    environment:
      - DB_HOST=postgres
      - DB_PASSWORD_FILE=/run/secrets/db_password
      - CRAWLER_NUM_PAGES=5
      - LOG_LEVEL=INFO
    secrets:
      - db_password
    depends_on:
      - postgres
    volumes:
      - jobinsight_data:/app/data
      - jobinsight_logs:/app/logs
    networks:
      - jobinsight_network
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 1G
        reservations:
          memory: 512M

secrets:
  db_password:
    external: true

volumes:
  postgres_prod_data:
  jobinsight_data:
  jobinsight_logs:

networks:
  jobinsight_network:
    driver: overlay
```

### Configuration Classes

**Hierarchical Configuration Structure**:
```python
# src/utils/config.py - Centralized configuration management
class Config:
    class Database:
        HOST = os.getenv("DB_HOST", "postgres")
        PORT = int(os.getenv("DB_PORT", "5432"))
        # ... additional database settings
    
    class Crawler:
        BASE_URL = os.getenv("CRAWLER_BASE_URL", "https://www.topcv.vn/viec-lam-it")
        NUM_PAGES = int(os.getenv("CRAWLER_NUM_PAGES", "5"))
        # ... additional crawler settings
```

**Configuration Validation**:
- **Type Checking**: Automatic type conversion and validation
- **Range Validation**: Bounds checking for numeric parameters
- **Dependency Validation**: Cross-parameter consistency checks
- **Environment Detection**: Automatic environment-specific configuration

## Deployment Guide

### Docker Deployment (Recommended)

**Docker Compose Configuration**:
```yaml
# docker-compose.yml
version: '3.8'
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: jobinsight
      POSTGRES_USER: jobinsight
      POSTGRES_PASSWORD: jobinsight
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    ports:
      - "5432:5432"

  airflow-webserver:
    build: .
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://jobinsight:jobinsight@postgres:5432/jobinsight
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    depends_on:
      - postgres
```

**Deployment Steps**:
1. **Environment Setup**: Configure environment variables
2. **Database Initialization**: Run database schema creation scripts
3. **Container Build**: Build Docker images with dependencies
4. **Service Startup**: Start services using docker-compose
5. **Health Verification**: Verify all services are healthy

### Manual Deployment

**Installation Steps**:
```bash
# 1. Clone repository
git clone <repository-url>
cd jobinsight

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers
playwright install

# 5. Initialize database
python scripts/init_database.py

# 6. Initialize Airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# 7. Start services
airflow webserver --port 8080 &
airflow scheduler &
```

## Monitoring & Observability

### Performance Metrics

**Key Performance Indicators (KPIs)**:
- **Daily Success Rate**: Target >99%
- **Average Execution Time**: Target 45-60 seconds
- **Jobs Processed Daily**: Target 125+ jobs
- **Error Rate**: Target <1%

**System Metrics**:
- **Memory Usage**: Monitor for memory leaks
- **CPU Utilization**: Track processing efficiency
- **Disk Usage**: Monitor data and log storage
- **Network I/O**: Track bandwidth utilization

### Logging Configuration

**Log Levels and Usage**:
```python
# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/crawler.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
    }
}
```

**Log Categories**:
- **INFO**: Normal operation events and milestones
- **WARNING**: Recoverable issues and performance concerns
- **ERROR**: Operation failures requiring attention
- **DEBUG**: Detailed diagnostic information (development only)

### Health Checks

**Automated Health Monitoring**:
```bash
# Database connectivity check
python -c "
from src.db.core import get_connection
try:
    with get_connection() as conn:
        print('Database: HEALTHY')
except Exception as e:
    print(f'Database: UNHEALTHY - {e}')
"

# Crawler functionality check
python -c "
from src.crawler.crawler import TopCVCrawler
try:
    crawler = TopCVCrawler()
    print('Crawler: HEALTHY')
except Exception as e:
    print(f'Crawler: UNHEALTHY - {e}')
"
```

**Health Check Endpoints**:
- **Database**: Connection pool status and query performance
- **External Dependencies**: TopCV website availability
- **Resource Usage**: Memory, CPU, and disk utilization
- **Service Status**: Airflow scheduler and webserver status

## Troubleshooting Guide

### Common Issues and Solutions

**1. Parse Failures (2/5 pages not parsing jobs)**

**Symptoms**:
- Log shows "Found 50 job items" but "Parsed 0 jobs"
- Missing job_id or title in extracted data

**Diagnosis**:
```bash
# Enable debug logging
export PYTHONPATH=/path/to/jobinsight
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from src.crawler.parser import TopCVParser
parser = TopCVParser()
# Check specific HTML file
result = parser.parse_html_file('data/raw_backup/it_p1_20250127154513.html')
print(f'Parsed {len(result)} jobs')
"
```

**Solutions**:
- **HTML Structure Changes**: Update CSS selectors in parser
- **Anti-Bot Detection**: Verify user-agent and delay settings
- **Data Validation**: Review validation criteria in extract_job_data()

**2. Slow Backup Performance (>60 seconds)**

**Symptoms**:
- Backup phase taking longer than expected
- High CPU usage during backup

**Diagnosis**:
```bash
# Check current delay settings
python -c "
from src.utils.config import Config
print(f'Min Delay: {Config.Crawler.MIN_DELAY}s')
print(f'Max Delay: {Config.Crawler.MAX_DELAY}s')
print(f'Concurrent Backups: {Config.Threading.MAX_WORKERS}')
"
```

**Solutions**:
- **Reduce Delays**: Adjust MIN_DELAY and MAX_DELAY (current: 3-6s)
- **Increase Concurrency**: Adjust concurrent_backups setting
- **Network Optimization**: Verify network connectivity and bandwidth

**3. Database Connection Issues**

**Symptoms**:
- Connection timeout errors
- "Too many connections" errors

**Diagnosis**:
```bash
# Check database connections
psql -h postgres -U jobinsight -d jobinsight -c "
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';
"
```

**Solutions**:
- **Connection Pool Tuning**: Adjust DB_POOL_SIZE and DB_MAX_OVERFLOW
- **Connection Cleanup**: Verify proper connection cleanup in code
- **Database Configuration**: Increase max_connections in PostgreSQL

**4. Memory Leaks**

**Symptoms**:
- Gradually increasing memory usage
- Out of memory errors during processing

**Diagnosis**:
```bash
# Monitor memory usage
python -c "
import psutil
import time
from src.crawler.parser import TopCVParser

parser = TopCVParser()
initial_memory = psutil.Process().memory_info().rss / 1024 / 1024

# Simulate processing
for i in range(10):
    parser.clear_memory_cache()
    current_memory = psutil.Process().memory_info().rss / 1024 / 1024
    print(f'Iteration {i}: {current_memory:.1f}MB')
    time.sleep(1)
"
```

**Solutions**:
- **Memory Cleanup**: Ensure proper cleanup in parser component
- **Resource Limits**: Set appropriate memory limits for containers
- **Garbage Collection**: Force garbage collection in long-running processes

### Performance Optimization

**Tuning Parameters**:
```bash
# High-performance configuration
export CRAWLER_MIN_DELAY=2          # Faster crawling
export CRAWLER_MAX_DELAY=4          # Reduced delays
export MAX_WORKERS=16               # More worker threads
export CONCURRENT_BACKUPS=5         # Higher concurrency
export DB_POOL_SIZE=20              # Larger connection pool
```

**Monitoring Commands**:
```bash
# Real-time performance monitoring
watch -n 5 'docker stats --no-stream'
watch -n 10 'tail -n 20 logs/crawler.log'
```

## Maintenance Procedures

### Regular Maintenance Tasks

**Daily Tasks**:
- Monitor DAG execution status in Airflow UI
- Check log files for errors or warnings
- Verify data quality in raw_jobs table
- Monitor disk usage for log and data directories

**Weekly Tasks**:
- Review performance metrics and trends
- Clean up old log files and temporary data
- Update user-agent pool if necessary
- Verify backup and recovery procedures

**Monthly Tasks**:
- Review and update configuration parameters
- Analyze performance trends and optimization opportunities
- Update dependencies and security patches
- Conduct disaster recovery testing

### Backup and Recovery

**Data Backup Strategy**:
```bash
# Database backup
pg_dump -h postgres -U jobinsight jobinsight > backup_$(date +%Y%m%d).sql

# Configuration backup
tar -czf config_backup_$(date +%Y%m%d).tar.gz \
    docker-compose.yml \
    .env \
    dags/ \
    sql/
```

**Recovery Procedures**:
```bash
# Database recovery
psql -h postgres -U jobinsight jobinsight < backup_20250127.sql

# Configuration recovery
tar -xzf config_backup_20250127.tar.gz
```

---

*This guide provides comprehensive operational guidance for the JobInsight Crawler System. For performance benchmarks and optimization strategies, refer to the Performance & Optimization documentation.*
