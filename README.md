# JobInsight ETL Pipeline

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Trantuan24/JobInsight_Data_Pipeline)
[![Build Status](https://img.shields.io/github/actions/workflow/status/Trantuan24/JobInsight_Data_Pipeline/ci.yml?branch=main)](../../actions)
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Data Pipeline](https://img.shields.io/badge/pipeline-ETL-orange)](docs/System_Architecture_Overview.md)
[![DuckDB](https://img.shields.io/badge/warehouse-DuckDB-yellow)](https://duckdb.org/)

> **A production-ready, enterprise-grade ETL pipeline for Vietnamese job market intelligence**

JobInsight ETL Pipeline is a sophisticated **3-phase data engineering platform** that transforms raw job market data into actionable business insights. Built for scale, reliability, and performance, it processes **~383 jobs daily** through automated crawling, standardization, and dimensional modeling.

## 🎯 **Key Highlights**

- **🚀 Production Performance**: Efficient end-to-end processing với high reliability
- **📊 Dimensional Modeling**: Star schema with SCD Type 2 historical tracking
- **🔄 Automated Processing**: Daily crawling, ETL, and analytics pipeline
- **📈 Business Intelligence**: Powers executive dashboards and market analysis
- **🛡️ Enterprise-Grade**: Comprehensive monitoring, error handling, and data quality

## 📋 **Table of Contents**

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Key Features](#key-features)
4. [Performance Metrics](#performance-metrics)
5. [Quick Start](#quick-start)
6. [Installation](#installation)
7. [Configuration](#configuration)
8. [Usage](#usage)
9. [Development](#development)
10. [Documentation](#documentation)
11. [Contributing](#contributing)
12. [License](#license)

## Overview

JobInsight ETL Pipeline delivers **end-to-end job market intelligence** through a sophisticated 3-phase data processing architecture. From web crawling to dimensional modeling, it transforms raw recruitment data into strategic business insights.

### **Business Value**
- **Market Intelligence**: Daily monitoring of 383+ job postings
- **Competitive Analysis**: Company hiring patterns, salary benchmarking, skills demand
- **Strategic Insights**: Executive dashboards, HR analytics, predictive modeling
- **Operational Efficiency**: Automated processing vs hours of manual work

## System Architecture

### **3-Phase Processing Pipeline**

```mermaid
graph TD
    subgraph "Phase 1: Data Acquisition"
        A[TopCV Website] --> B[Web Crawler]
        B --> C[Anti-Detection Layer]
        C --> D[HTML Parser]
        D --> E[PostgreSQL Raw Storage]
    end

    subgraph "Phase 2: Data Standardization"
        E --> F[Data Extractor]
        F --> G[Cleaning & Validation]
        G --> H[Deduplication]
        H --> I[PostgreSQL Staging]
    end

    subgraph "Phase 3: Analytics Preparation"
        I --> J[Dimensional Modeling]
        J --> K[SCD Type 2 Processing]
        K --> L[Star Schema Generation]
        L --> M[DuckDB Data Warehouse]
    end

    subgraph "Analytics Layer"
        M --> N[BI Dashboards]
        M --> O[Data Science Models]
        M --> P[Automated Reports]
    end

    style A fill:#e1f5fe
    style E fill:#fff3e0
    style I fill:#f3e5f5
    style M fill:#e8f5e8
```

### **Technology Stack**
- **Web Scraping**: Playwright + BeautifulSoup + Chromium (anti-detection)
- **Processing**: Python + Pandas (data transformation)
- **Storage**: PostgreSQL (raw/staging) + DuckDB (warehouse)
- **Orchestration**: Apache Airflow (workflow management)
- **Analytics**: Star schema with SCD Type 2 historical tracking

## Key Features

### **🏗️ Enterprise Architecture**
- **3-Phase Pipeline**: Crawling → Standardization → Dimensional Modeling
- **Star Schema Design**: 4 dimensions + 2 fact tables optimized for analytics
- **SCD Type 2**: Complete historical tracking of job titles, company info, locations
- **Cross-Database ETL**: PostgreSQL staging → DuckDB data warehouse

### **🚀 Production-Grade Features**
| Feature | Description | Business Impact |
|---------|-------------|-----------------|
| **Anti-Detection Crawling** | Sophisticated CAPTCHA handling & rate limiting | Reliable data acquisition |
| **Real-time Processing** | Automated ETL triggers & incremental updates | Fresh data for decisions |
| **Data Quality Assurance** | Comprehensive validation & integrity checks | 100% reliable analytics |
| **Dimensional Modeling** | Star schema with daily grain facts | Optimized for BI queries |
| **Historical Tracking** | SCD Type 2 for trend analysis | Complete audit trail |
| **Scalable Storage** | Partitioned data with Parquet optimization | Cost-effective scaling |
| **Comprehensive Testing** | Unit tests with 20 real job descriptions | Production reliability |

### **📊 Data Warehouse Model**
- **Dimensions**: `DimJob`, `DimCompany`, `DimLocation`, `DimDate`
- **Facts**: `FactJobPostingDaily` (daily grain), `FactJobLocationBridge` (many-to-many)
- **Grain**: Each fact record represents one job posting for one day
- **Storage**: Monthly partitioning with Parquet format for analytics optimization

## Performance Metrics

### **Production Performance Overview**

| Phase | Execution Time | Throughput | Main Bottleneck | Optimization Potential |
|-------|----------------|------------|-----------------|----------------------|
| **Phase 1: Crawler** | ~111s (93%) | ~3.5 jobs/sec | CAPTCHA solving | **Potential 50-70%** |
| **Phase 2: Raw to Staging** | ~1.14s (1%) | ~336 rec/sec | Database operations | **Potential 30-50%** |
| **Phase 3: Staging to DWH** | ~7.5s (6%) | ~51 rec/sec | Fact processing | **Potential 50-70%** |
| **Total Pipeline** | **~120s** | **~3.2 jobs/sec** | Phase 1 dominates | **Potential 50% system-wide** |

### **Data Volume & Quality Metrics**
```
📊 Current Scale (Daily Processing):
├── Input: ~383 job postings from TopCV
├── Success Rate: High end-to-end processing reliability
├── Data Quality: Comprehensive validation với integrity checks
├── Storage: DuckDB warehouse với ~1,915 daily facts
└── Coverage: 243+ companies, 53+ locations, comprehensive skills tracking

📈 Scaling Projections (10x Growth):
├── Input: ~3,830 job postings
├── Processing Time: Proportional scaling với optimization opportunities
├── Storage: Scalable warehouse với partitioning
└── Infrastructure: Horizontal scaling capabilities
```

### **System Reliability**
- **Error Handling**: Comprehensive retry logic and circuit breakers
- **Data Integrity**: Multi-layer validation với rollback capabilities
- **Monitoring**: Performance tracking and logging
- **Recovery**: Automated backup and restore procedures

## Quick Start

### **🚀 One-Command Setup**
```bash
# Clone repository
git clone https://github.com/Trantuan24/JobInsight_Data_Pipeline.git
cd JobInsight_Data_Pipeline

# Setup environment
cp env.example .env
# Edit .env with your database credentials

# Launch entire stack
docker-compose up -d
```

### **🎯 Access Points**
After containers start:
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Database**: PostgreSQL on localhost:5432
- **Data Warehouse**: DuckDB files in `data/duck_db/`

### **⚡ Quick Test**
```bash
# Run crawler manually
python -m src.crawler.crawler

# Run ETL pipeline
python -m src.etl.raw_to_staging
python -m src.etl.staging_to_dwh

# Check results
python -c "import duckdb; conn=duckdb.connect('data/duck_db/jobinsight_warehouse.duckdb'); print('Facts:', conn.execute('SELECT COUNT(*) FROM FactJobPostingDaily').fetchone()[0])"
```

## Installation

### **System Requirements**
- **Python**: 3.8+ (3.9+ recommended)
- **PostgreSQL**: 13+ (for raw/staging data)
- **DuckDB**: 0.9+ (embedded analytics warehouse)
- **Apache Airflow**: 2.7+ (workflow orchestration)
- **Docker**: 20.10+ (containerized deployment)

### **Development Setup**
```bash
# 1. Clone repository
git clone https://github.com/Trantuan24/JobInsight_Data_Pipeline.git
cd JobInsight_Data_Pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup environment variables
cp env.example .env
# Edit .env with your configuration

# 5. Initialize data directories (will be created automatically)
# The pipeline will create necessary subdirectories in data/ and logs/
```

### **Database Setup**
```bash
# PostgreSQL schemas & stored procedures
psql -U $DB_USER -h $DB_HOST -d $DB_NAME -f sql/schema_raw_jobs.sql
psql -U $DB_USER -h $DB_HOST -d $DB_NAME -f sql/schema_staging.sql
psql -U $DB_USER -h $DB_HOST -d $DB_NAME -f sql/stored_procedures.sql

# Initialize DuckDB warehouse
python -m src.utils.setup_dwh
```

### **Docker Deployment**
```bash
# Production deployment
docker-compose -f docker-compose.prod.yml up -d

# Development with hot reload
docker-compose up -d
```

## Configuration

### **Environment Variables**
| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | localhost | PostgreSQL host for raw/staging data |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_USER` | jobinsight | Database user |
| `POSTGRES_PASSWORD` | jobinsight | Database password |
| `POSTGRES_DB` | jobinsight | Database name |
| `DUCKDB_PATH` | data/duck_db/jobinsight_warehouse.duckdb | DuckDB warehouse path |
| `DISCORD_WEBHOOK_URL` | - | Discord notifications webhook |
| `AIRFLOW_UID` | 50000 | Airflow container UID |
| `AIRFLOW_GID` | 0 | Airflow container GID |

> See complete configuration in [`env.example`](env.example)

### **Performance Tuning**
```bash
# Crawler optimization
export CRAWLER_DELAY_MIN=2
export CRAWLER_MAX_PAGES=5
export CAPTCHA_TIMEOUT=30

# ETL optimization
export ETL_BATCH_SIZE=500
export ETL_MAX_WORKERS=4
export ETL_TIMEOUT=600

# DuckDB optimization (via connection settings)
# Memory limit và threads configured in code
```

## Usage

### **Manual Execution**
```bash
# Run individual phases
python -m src.crawler.crawler          # Phase 1: Web crawling
python -m src.etl.raw_to_staging        # Phase 2: Data standardization
python -m src.etl.etl_main              # Phase 3: Dimensional modeling

# Or use Airflow DAGs for orchestrated execution
```

### **Airflow Orchestration**
```bash
# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &

# Trigger DAGs
airflow dags trigger crawl_jobs
airflow dags trigger etl_pipeline
```

### **Data Analysis**
```python
import duckdb

# Connect to warehouse
conn = duckdb.connect('data/duck_db/jobinsight_warehouse.duckdb')

# Sample queries
conn.execute("""
    SELECT
        dc.company_name_standardized,
        COUNT(*) as job_count,
        AVG(f.salary_avg) as avg_salary
    FROM FactJobPostingDaily f
    JOIN DimCompany dc ON f.company_sk = dc.company_sk
    WHERE dc.is_current = TRUE
    GROUP BY dc.company_name_standardized
    ORDER BY job_count DESC
    LIMIT 10
""").fetchdf()
```

## Development

### **Development Commands**
| Command | Purpose |
|---------|---------|
| `black .` | Auto-format code |
| `flake8` | Check code style & potential bugs |
| `pre-commit install` | Enable pre-commit hooks |
| `pytest -v` | Run all tests with verbose output |
| `pytest tests/test_crawler.py` | Run specific test module |
| `airflow dags test <dag_id> <exec_date>` | Test specific DAG |

### **Code Quality**
```bash
# Setup development environment
pre-commit install

# Run quality checks
black . --check
flake8 src/ tests/
mypy src/

# Run tests
pytest --cov=src tests/
```

### **Project Structure**
```
JobInsight_Data_Pipeline/
├── 📁 dags/                    # Airflow DAG definitions
│   ├── crawl_jobs.py          # Job crawling DAG
│   └── etl_pipeline.py        # ETL processing DAG
├── 📁 docs/                    # Comprehensive documentation
│   ├── README.md              # Documentation overview
│   ├── System_Architecture_Overview.md
│   ├── Project_Structure_Documentation.md
│   ├── crawler/               # Phase 1 documentation
│   ├── etl-raw-to-staging/    # Phase 2 documentation
│   └── etl-staging-to-dwh/    # Phase 3 documentation
├── 📁 data/                    # Data storage (excluded from Git)
│   ├── cdc/                   # Change Data Capture files
│   ├── duck_db/               # DuckDB warehouse files
│   ├── parquet/               # Parquet data files
│   └── raw_backup/            # HTML backup files from crawling
├── 📁 sql/                     # Database schemas & procedures
│   ├── schema_raw_jobs.sql    # Raw data schema
│   ├── schema_staging.sql     # Staging schema
│   ├── schema_dwh.sql         # Data warehouse schema
│   ├── stored_procedures.sql  # ETL stored procedures
│   └── views.sql              # Analytics views
├── 📁 src/                     # Source code
│   ├── crawler/               # Phase 1: Web crawling
│   ├── etl/                   # Phase 2 & 3: ETL processing
│   ├── db/                    # Database operations
│   ├── processing/            # Data processing utilities
│   ├── ingestion/             # Data ingestion modules
│   ├── utils/                 # Shared utilities
│   └── common/                # Common decorators & helpers
├── 📁 tests/                   # Unit & integration tests
│   ├── test_crawler.py        # Crawler tests
│   ├── test_ingest.py         # Ingestion tests
│   ├── test_raw_staging.py    # Raw to staging tests
│   └── test_staging_to_dwh.py # Staging to DWH tests
├── 📁 logs/                    # Application logs (excluded from Git)
├── 🐳 docker-compose.yml       # Docker orchestration
├── 🐳 Dockerfile              # Container definition
├── ⚙️ airflow.cfg             # Airflow configuration
├── 📋 requirements.txt        # Python dependencies
├── 📄 env.example             # Environment variables template
├── 📄 LICENSE                 # MIT License
└── 📖 README.md               # This file
```

## Documentation

### **📋 System Overview**
- **[System Architecture Overview](docs/System_Architecture_Overview.md)** - High-level architecture, business value, performance metrics
- **[Project Structure Documentation](docs/Project_Structure_Documentation.md)** - Codebase organization, directory structure, components

### **🔧 Phase-Specific Documentation**
- **[Phase 1: Crawler System](docs/crawler/README.md)** - Web crawling, anti-detection, data acquisition (7 documents)
- **[Phase 2: Raw to Staging ETL](docs/etl-raw-to-staging/README.md)** - Data cleaning, standardization, staging (6 documents)
- **[Phase 3: Staging to Data Warehouse ETL](docs/etl-staging-to-dwh/README.md)** - Dimensional modeling, analytics preparation (8 documents)

### **🚀 Quick Navigation**
- **[Documentation Overview](docs/README.md)** - Entry point for all documentation
- **New to the project?** → Start with [System Architecture Overview](docs/System_Architecture_Overview.md)
- **Need to understand codebase?** → Check [Project Structure Documentation](docs/Project_Structure_Documentation.md)
- **Working on specific phase?** → Go to phase-specific documentation above

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### **Development Guidelines**
- Follow PEP 8 code style
- Add tests for new features
- Update documentation for changes
- Use type hints where appropriate
- Write descriptive commit messages

## License

MIT © 2025

---

**Built with ❤️ for Vietnamese job market intelligence**