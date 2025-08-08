# JobInsight ETL - Raw to Staging Documentation

## Overview

Documentation for **Phase 2 - Raw to Staging ETL** based on production analysis and real performance data.

## 🎯 **Key Highlights**

### **System Capabilities**
- **5-Phase Processing**: Schema Setup → SQL Procedures → Data Loading → Python Processing → Data Saving
- **Performance Monitoring**: Built-in tracking với performance_monitor context manager
- **Data Integrity**: Verification với configurable threshold
- **Error Handling**: Comprehensive exception handling per phase

### **Configuration**
- **Batch Processing**: Configurable batch_size (default: None = process all)
- **Incremental Mode**: only_unprocessed parameter for processing new records only
- **Environment Variables**: ETL_BATCH_SIZE, ETL_TIMEOUT, ETL_MAX_WORKERS
- **Database**: PostgreSQL với staging schema (jobinsight_staging.staging_jobs)

## Documentation Structure

### 📋 [01. System Architecture](01_system_architecture.md)
5-phase pipeline overview, performance metrics, và bottleneck analysis

### 🔧 [02. Implementation & Performance](02_implementation_performance.md)
Technical details và optimization opportunities

### ⚙️ [03. Configuration Guide](03_configuration_guide.md)
Environment variables, performance tuning, và deployment settings

### 🔧 [04. Troubleshooting Guide](04_troubleshooting_guide.md)
Reliability issues, common problems, và solutions

### 📖 [05. API Reference](05_api_reference.md)
Essential functions và usage patterns

## Quick Start

### **Operations Team** → [Troubleshooting Guide](04_troubleshooting_guide.md)
### **Developers** → [API Reference](05_api_reference.md)
### **Performance Engineers** → [Implementation & Performance](02_implementation_performance.md)

## 🔧 **Potential Optimization Opportunities**

1. **Batch Database Operations**: Implement batch upserts (theoretical improvement)
2. **Schema Validation Caching**: Cache schema existence checks (not yet implemented)
3. **Parallel Processing**: Concurrent transformations (not yet implemented)

## 📊 **Monitoring và Dependencies**

### **Performance Monitoring**
- **Per-phase tracking**: Duration, Memory usage, CPU usage
- **Data integrity validation**: verify_etl_integrity() với configurable threshold
- **Comprehensive logging**: Detailed metrics per phase

### **Key Dependencies**
- **Upstream**: Crawler completion (ExternalTaskSensor)
- **Database**: PostgreSQL availability
- **SQL Files**: schema_staging.sql, insert_raw_to_staging.sql, stored_procedures.sql
- **Downstream**: Phase 3 ETL (staging_to_dwh)

---

**Last Updated**: August 2025 | **Based On**: Code analysis và implementation review
