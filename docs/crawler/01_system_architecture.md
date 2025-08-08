# JobInsight Crawler System - Architecture Overview

## Executive Summary

The JobInsight Crawler System (Phase 1) is a production-ready data acquisition pipeline that autonomously extracts job posting data from TopCV. The system runs daily at 11:00 AM Vietnam time, processing **50+ job postings** with sophisticated anti-detection mechanisms.

## Performance Overview

### 📊 **Production Metrics**
- **Daily Schedule**: 11:00 AM Vietnam time (17:40 UTC)
- **Execution Time**: ~2 minutes average
- **Data Volume**: 50+ job postings per run
- **Success Rate**: Varies based on anti-bot detection

### 🚨 **Operational Considerations**
- **Anti-Detection**: System includes CAPTCHA handling and retry mechanisms
- **Reliability**: Built-in circuit breakers and error recovery
- **Monitoring**: Comprehensive logging for troubleshooting

## System Architecture

### High-Level DAG Structure

```mermaid
graph TD
    A[Airflow Scheduler] --> B[crawl_topcv_jobs DAG]
    B --> C[start]
    C --> D[crawl_and_process]
    D --> E[cleanup_temp_files]
    E --> F[end]
    F --> G[ExternalTaskSensor]
    G --> H[jobinsight_etl_pipeline DAG]
    
    subgraph "crawl_and_process Task"
        D --> D1[TopCVCrawler.run]
        D1 --> D2[Phase 1: HTML Backup]
        D2 --> D3[Phase 2: HTML Parsing]
        D3 --> D4[Phase 3: Database Save]
        D4 --> D5[Phase 4: CDC Logging]
    end
    
    style D fill:#e1f5fe
    style D2 fill:#ffecb3
    style D3 fill:#c8e6c9
    style D4 fill:#f3e5f5
    style D5 fill:#fff3e0
```

### Component Architecture

```mermaid
graph TB
    subgraph "TopCVCrawler (Main Orchestrator)"
        A[TopCVCrawler]
        A --> B[UserAgentManager]
        A --> C[CaptchaHandler]
        A --> D[HTMLBackupManager]
        A --> E[TopCVParser]
        A --> F[DBBulkOperations]
    end
    
    subgraph "Anti-Detection Layer"
        B --> B1[40 User Agents]
        B --> B2[Random Viewports]
        C --> C1[Pattern Detection]
        C --> C2[Retry Logic]
        C --> C3[Fingerprint Spoofing]
    end
    
    subgraph "Data Processing Layer"
        D --> D1[Playwright Browser]
        D --> D2[Concurrent Backup]
        D --> D3[Circuit Breaker]
        E --> E1[BeautifulSoup Parser]
        E --> E2[ThreadPoolExecutor]
        E --> E3[Data Validation]
    end
    
    subgraph "Persistence Layer"
        F --> F1[PostgreSQL COPY]
        F --> F2[Bulk Upsert]
        F --> F3[Transaction Management]
    end
```

## Data Flow Architecture

### 4-Phase Processing Pipeline

```mermaid
sequenceDiagram
    participant DAG as Airflow DAG
    participant TC as TopCVCrawler
    participant HBM as HTMLBackupManager
    participant TCP as TopCVParser
    participant DBO as DBBulkOperations
    participant CDC as CDC System
    
    DAG->>TC: Execute crawl_and_process
    TC->>HBM: Phase 1: Backup HTML (5 pages)
    HBM->>HBM: Concurrent scraping with anti-detection
    HBM-->>TC: HTML files + success metrics
    
    TC->>TCP: Phase 2: Parse HTML files
    TCP->>TCP: Multi-threaded parsing
    TCP-->>TC: DataFrame with job data
    
    TC->>DBO: Phase 3: Bulk upsert to raw_jobs
    DBO->>DBO: COPY + ON CONFLICT operations
    DBO-->>TC: Insert/update statistics
    
    TC->>CDC: Phase 4: Log changes for ETL
    CDC->>CDC: JSONL file creation
    CDC-->>TC: CDC completion status
    
    TC-->>DAG: Final execution report
```

## Component Responsibilities

### 1. TopCVCrawler (Orchestrator)
- **Purpose**: Central coordinator for entire crawling process
- **Responsibilities**: Pipeline orchestration, error handling, configuration management

### 2. HTMLBackupManager (Web Scraping)
- **Purpose**: Concurrent HTML page backup with anti-detection
- **Responsibilities**: Browser automation, concurrency control, circuit breaker protection

### 3. CaptchaHandler (Anti-Detection)
- **Purpose**: Detect and handle anti-bot measures
- **Responsibilities**: CAPTCHA detection, retry logic, browser fingerprint spoofing

### 4. TopCVParser (Data Extraction)
- **Purpose**: Extract structured data from HTML
- **Responsibilities**: HTML parsing, data validation, concurrent processing

### 5. DBBulkOperations (Data Persistence)
- **Purpose**: High-performance database operations
- **Responsibilities**: Bulk inserts/updates, transaction management, performance optimization

## Configuration Overview

### Key Configuration Areas
- **Execution Settings**: Number of pages, parallel processing, scheduling
- **Anti-Detection**: Delays, user agents, retry logic, circuit breakers
- **Performance**: Timeouts, worker threads, memory management
- **Database**: Connection settings, bulk operation parameters

*For detailed configuration options, see [Configuration Guide](03_configuration_guide.md)*

## Deployment Architecture

### Container Environment
- **Runtime**: Docker container with Python 3.13+
- **Dependencies**: Playwright, BeautifulSoup, psycopg2, pandas
- **Resource Requirements**: 
  - Memory: ~1.2GB during execution
  - CPU: Multi-core for concurrent processing
  - Storage: Temporary HTML files (~5MB per run)

### Integration Points
- **Upstream**: Scheduled execution via Airflow
- **Downstream**: ETL pipeline triggered via ExternalTaskSensor
- **Monitoring**: Structured logging to Airflow logs
- **Alerting**: Task failure notifications through Airflow

## Security & Compliance

### Anti-Detection Measures
- **User-Agent Rotation**: 40 realistic browser signatures
- **Fingerprint Masking**: JavaScript injection to hide automation
- **Behavioral Simulation**: Human-like scrolling and timing
- **IP Rotation**: Circuit breaker to prevent IP bans

### Data Privacy
- **No PII Collection**: Only public job posting data
- **Audit Trail**: CDC system tracks all data changes
- **Retention Policy**: 15-day cleanup for temporary files

## Next Steps

This architecture documentation provides the foundation for understanding the crawler system. For detailed implementation guides, see:
- [Technical Implementation Guide](02_technical_implementation.md)
- [Configuration Guide](03_configuration_guide.md)
- [Performance Analysis](04_performance_analysis.md)
- [Troubleshooting Guide](05_troubleshooting_guide.md)
