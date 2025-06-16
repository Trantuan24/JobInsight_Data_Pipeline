# Data Model

This document describes the relational schemas that underpin the JobInsight Data Warehouse and the transitional layers used during the ETL process.

## Layer Overview

| Layer | Key Tables | Purpose |
|-------|------------|---------|
| Raw | `raw_jobs` | Immutable storage of crawled job postings (semi-structured). |
| Staging | `jobinsight_staging.staging_jobs` | Cleansed, type-casted records used as the single source of truth for the warehouse load. |
| DWH Dimensions | `DimJob`, `DimCompany`, `DimLocation`, `DimDate` | Conformed dimensions that capture slowly-changing attributes (Type 2). |
| DWH Facts | `FactJobPostingDaily` | Transactional fact (grain: job-date) for analytics. |
| Bridge | `FactJobLocationBridge` | Resolves many-to-many between job postings and locations. |
| Views | `vw_current_jobs`, `vw_top_companies`, etc. | Serve BI queries with denormalised datasets. |

## Entity-Relationship Diagram

```mermaid
erDiagram
    DimJob {
        int job_sk PK
        varchar job_id
        varchar title_clean
        text job_url
        json skills
        date effective_date
        date expiry_date
        bool is_current
    }
    DimCompany {
        int company_sk PK
        varchar company_name_standardized
        text company_url
        bool verified_employer
        date effective_date
        date expiry_date
        bool is_current
    }
    DimLocation {
        int location_sk PK
        varchar province
        varchar city
        varchar district
        date effective_date
        date expiry_date
        bool is_current
    }
    DimDate {
        date date_id PK
        int day
        int month
        int quarter
        int year
        varchar weekday
    }
    FactJobPostingDaily {
        int fact_id PK
        int job_sk FK
        int company_sk FK
        date date_id FK
        numeric salary_min
        numeric salary_max
        varchar salary_type
        timestamp due_date
        timestamp posted_time
        varchar load_month
    }
    FactJobLocationBridge {
        int fact_id FK
        int location_sk FK
    }

    DimJob ||--o{ FactJobPostingDaily : has
    DimCompany ||--o{ FactJobPostingDaily : has
    DimDate ||--o{ FactJobPostingDaily : on
    FactJobPostingDaily ||--o{ FactJobLocationBridge : contains
    DimLocation ||--o{ FactJobLocationBridge : categorises
}
```

## Dimension Details

### DimJob
Captures job-specific attributes (title, link, skills). Slowly Changing Dimension Type 2 with `effective_date`, `expiry_date`, and `is_current` flag.

### DimCompany
Standardised company names to support consistent roll-ups. Includes `verified_employer` flag.

### DimLocation
Normalised locations down to city/district level, enabling geo-based analytics.

### DimDate
Calendar dimension generated once and reused across facts.

## Fact Tables

### FactJobPostingDaily
Grain: *one row per job per date*. Allows time-series analysis (advert lifespan, daily salary changes). Partitioned by `load_month` for query performance.

### FactJobLocationBridge
Bridge table to associate a job posting with multiple locations (some postings list several offices).

## Data Lifecycle
1. **Raw Capture** – Crawlers write into `raw_jobs`.
2. **Staging Transform** – `raw_to_staging` cleans and enhances data.
3. **Dim/Facts Load** – `staging_to_dwh` manages SCD2 for dimensions, generates surrogate keys, and inserts facts.
4. **Views** – Provide denormalised access patterns for BI tools.

## Partitioning & Indexing
- Partition on `load_month` in `FactJobPostingDaily` reduces scan range for monthly reporting.
- Indexes on `is_current`, surrogate keys, and high-cardinality columns speed up joins and filters.

## Audit & Lineage
- Surrogate key sequences (`seq_dim_*`) reset via ETL utilities for deterministic testing.
- Source record hashes tracked in ETL logs (`src/etl/etl_utils.py`) to detect change data capture (CDC).
