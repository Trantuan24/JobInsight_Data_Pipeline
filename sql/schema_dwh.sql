CREATE SEQUENCE IF NOT EXISTS seq_personid START 1;

-- PHẦN 1: TẠO BẢNG
CREATE TABLE IF NOT EXISTS DimJob (
    job_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_personid'),
    job_id VARCHAR(20) NOT NULL UNIQUE,
    title_clean VARCHAR(255) NOT NULL,
    job_url TEXT,
    skills JSON,
    last_update VARCHAR(100),
    logo_url TEXT,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS DimCompany (
    company_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_personid'),
    company_name_standardized VARCHAR(200) NOT NULL,
    company_url TEXT,
    verified_employer BOOLEAN,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS DimLocation (
    location_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_personid'),
    province VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS DimDate (
    date_id DATE PRIMARY KEY,
    day INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    weekday VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS FactJobPostingDaily (
    fact_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_personid'),
    job_sk INTEGER NOT NULL,
    company_sk INTEGER NOT NULL,
    date_id DATE NOT NULL,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),
    due_date TIMESTAMP,
    time_remaining TEXT,
    verified_employer BOOLEAN,
    posted_time TIMESTAMP,
    crawled_at TIMESTAMP,
    load_month VARCHAR(7) NOT NULL
    -- FOREIGN KEY (job_sk) REFERENCES DimJob(job_sk),
    -- FOREIGN KEY (company_sk) REFERENCES DimCompany(company_sk),
    -- FOREIGN KEY (date_id) REFERENCES DimDate(date_id)
);

CREATE TABLE IF NOT EXISTS FactJobLocationBridge (
    fact_id INTEGER NOT NULL,
    location_sk INTEGER NOT NULL,
    PRIMARY KEY (fact_id, location_sk)
    -- FOREIGN KEY (fact_id) REFERENCES FactJobPostingDaily(fact_id),
    -- FOREIGN KEY (location_sk) REFERENCES DimLocation(location_sk)
);

-- PHẦN 2: TẠO INDEXES
CREATE INDEX IF NOT EXISTS idx_dimjob_current ON DimJob(is_current);
CREATE INDEX IF NOT EXISTS idx_dimcompany_current ON DimCompany(is_current);
CREATE INDEX IF NOT EXISTS idx_dimlocation_current ON DimLocation(is_current);
CREATE INDEX IF NOT EXISTS idx_fact_date ON FactJobPostingDaily(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_load_month ON FactJobPostingDaily(load_month);
CREATE INDEX IF NOT EXISTS idx_fact_job_date ON FactJobPostingDaily(job_sk, date_id);

-- PHẦN 3: TẠO VIEWS
CREATE VIEW IF NOT EXISTS vw_current_jobs AS
SELECT j.*, c.company_name_standardized, c.verified_employer
FROM DimJob j
JOIN FactJobPostingDaily f ON j.job_sk = f.job_sk
JOIN DimCompany c ON f.company_sk = c.company_sk
WHERE j.is_current = TRUE
AND c.is_current = TRUE;

CREATE VIEW IF NOT EXISTS vw_job_locations AS
SELECT f.fact_id, f.job_sk, f.date_id, l.province, l.city, l.district
FROM FactJobPostingDaily f
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE l.is_current = TRUE;