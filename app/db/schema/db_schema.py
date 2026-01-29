#!/usr/bin/env python3
"""
Database Schema Definitions
Single source of truth for all database schemas.
"""

# Schema for companies.db
COMPANIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,  -- 'comeet', 'lever', 'greenhouse', etc.
    company_page_url TEXT UNIQUE NOT NULL,  -- The main job listings page for this company
    title TEXT,  -- Page title from search results
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP,  -- When we last scraped jobs from this page
    is_active BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'google_serper'  -- How we found this company
);

CREATE INDEX IF NOT EXISTS idx_company_name ON companies(company_name);
CREATE INDEX IF NOT EXISTS idx_domain ON companies(domain);
CREATE INDEX IF NOT EXISTS idx_company_page_url ON companies(company_page_url);
CREATE INDEX IF NOT EXISTS idx_is_active ON companies(is_active);
CREATE INDEX IF NOT EXISTS idx_last_scraped ON companies(last_scraped);
CREATE INDEX IF NOT EXISTS idx_discovered_at ON companies(discovered_at);
"""

# Schema for jobs.db (comprehensive with normalization and reference data)
JOBS_SCHEMA = """
-- ============================================================================
-- REFERENCE TABLES: Departments
-- ============================================================================

-- Canonical department names with categories
CREATE TABLE IF NOT EXISTS departments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    category TEXT  -- e.g., 'Engineering', 'Business', 'Operations', etc.
);

-- Maps department variations to canonical names
CREATE TABLE IF NOT EXISTS department_synonyms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    synonym TEXT NOT NULL UNIQUE,
    department_id INTEGER NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dept_synonym ON department_synonyms(synonym);
CREATE INDEX IF NOT EXISTS idx_dept_synonym_dept_id ON department_synonyms(department_id);

-- ============================================================================
-- REFERENCE TABLES: Locations
-- ============================================================================

-- Canonical location names with geographic hierarchy
CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    country TEXT,       -- e.g., 'Israel', 'United States'
    region TEXT         -- e.g., 'Center', 'North', 'California'
);

-- Maps location variations to canonical names
CREATE TABLE IF NOT EXISTS location_synonyms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    synonym TEXT NOT NULL UNIQUE,
    location_id INTEGER NOT NULL,
    FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_loc_synonym ON location_synonyms(synonym);
CREATE INDEX IF NOT EXISTS idx_loc_synonym_loc_id ON location_synonyms(location_id);

-- ============================================================================
-- CORE JOBS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Basic job information
    title TEXT NOT NULL,
    company_name TEXT,

    -- Department (both raw and normalized)
    department TEXT,                    -- Raw text from scraping
    department_id INTEGER,              -- Normalized reference

    -- Location (both raw and normalized)
    location TEXT,                      -- Raw text from scraping
    location_id INTEGER,                -- Normalized reference

    -- Job characteristics (indexed for filtering)
    workplace_type TEXT,                -- 'On-site', 'Hybrid', 'Remote'
    experience_level TEXT,              -- 'Intern', 'Junior', 'Mid-level', 'Senior', 'Lead', 'Manager'
    employment_type TEXT DEFAULT 'Full-time',  -- 'Full-time', 'Part-time', 'Contract', 'Temporary'

    -- Timestamps
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publish_date TIMESTAMP,             -- Job posting date or last_updated

    -- Content
    description TEXT,                   -- Full job description for embedding/matching
    embedding BLOB,                     -- Vector embedding of description (numpy array as pickle)

    -- Source tracking
    uid TEXT,                           -- Source's unique identifier
    url TEXT UNIQUE NOT NULL,           -- Job posting URL (primary unique identifier)
    url_hash TEXT UNIQUE,               -- MD5 hash of URL
    from_domain TEXT,                   -- e.g., 'comeet.com', 'lever.co'
    original_website_job_url TEXT,      -- Original job URL from company website (url_active_page or url_detected_page)

    -- Metadata
    email TEXT,                         -- Application email if available
    is_ai_inferred BOOLEAN DEFAULT 0,   -- Flag if AI filled missing fields

    -- Foreign keys
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
    FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE SET NULL
);

-- ============================================================================
-- INDEXES for Jobs Table (Optimized for Filtering)
-- ============================================================================

-- Filter indexes
CREATE INDEX IF NOT EXISTS idx_jobs_workplace_type ON jobs(workplace_type);
CREATE INDEX IF NOT EXISTS idx_jobs_experience_level ON jobs(experience_level);
CREATE INDEX IF NOT EXISTS idx_jobs_employment_type ON jobs(employment_type);
CREATE INDEX IF NOT EXISTS idx_jobs_department_id ON jobs(department_id);
CREATE INDEX IF NOT EXISTS idx_jobs_location_id ON jobs(location_id);

-- Search and lookup indexes
CREATE INDEX IF NOT EXISTS idx_jobs_company_name ON jobs(company_name);
CREATE INDEX IF NOT EXISTS idx_jobs_title ON jobs(title);
CREATE INDEX IF NOT EXISTS idx_jobs_from_domain ON jobs(from_domain);
CREATE INDEX IF NOT EXISTS idx_jobs_scraped_at ON jobs(scraped_at);
CREATE INDEX IF NOT EXISTS idx_jobs_publish_date ON jobs(publish_date);

-- Composite indexes for app.common filter combinations
CREATE INDEX IF NOT EXISTS idx_jobs_location_workplace ON jobs(location_id, workplace_type);
CREATE INDEX IF NOT EXISTS idx_jobs_dept_experience ON jobs(department_id, experience_level);

-- ============================================================================
-- INITIAL REFERENCE DATA: Departments
-- ============================================================================

-- Insert canonical departments
INSERT OR IGNORE INTO departments (canonical_name, category) VALUES
    ('Engineering', 'R&D'),
    ('Software Development', 'R&D'),
    ('Research & Development', 'R&D'),
    ('Product Management', 'Product'),
    ('Design', 'Product'),
    ('Data Science', 'R&D'),
    ('Quality Assurance', 'R&D'),
    ('DevOps', 'R&D'),
    ('Marketing', 'Marketing'),
    ('Sales', 'Sales'),
    ('Business Development', 'Sales'),
    ('Customer Success', 'Customer'),
    ('Customer Support', 'Customer'),
    ('Operations', 'Operations'),
    ('Finance', 'Finance'),
    ('Human Resources', 'HR'),
    ('Legal', 'Legal'),
    ('Executive', 'Executive'),
    ('Strategy', 'Strategy'),
    ('Clinical', 'Clinical'),
    ('Regulatory Affairs', 'Regulatory'),
    ('Manufacturing', 'Operations'),
    ('Supply Chain', 'Operations');

-- Insert app.common department synonyms
INSERT OR IGNORE INTO department_synonyms (synonym, department_id)
SELECT 'R&D', id FROM departments WHERE canonical_name = 'Research & Development'
UNION ALL
SELECT 'Engineering', id FROM departments WHERE canonical_name = 'Engineering'
UNION ALL
SELECT 'Software Engineering', id FROM departments WHERE canonical_name = 'Software Development'
UNION ALL
SELECT 'SWE', id FROM departments WHERE canonical_name = 'Software Development'
UNION ALL
SELECT 'Product', id FROM departments WHERE canonical_name = 'Product Management'
UNION ALL
SELECT 'PM', id FROM departments WHERE canonical_name = 'Product Management'
UNION ALL
SELECT 'QA', id FROM departments WHERE canonical_name = 'Quality Assurance'
UNION ALL
SELECT 'Quality', id FROM departments WHERE canonical_name = 'Quality Assurance'
UNION ALL
SELECT 'Data', id FROM departments WHERE canonical_name = 'Data Science'
UNION ALL
SELECT 'Analytics', id FROM departments WHERE canonical_name = 'Data Science'
UNION ALL
SELECT 'BD', id FROM departments WHERE canonical_name = 'Business Development'
UNION ALL
SELECT 'BizDev', id FROM departments WHERE canonical_name = 'Business Development'
UNION ALL
SELECT 'CS', id FROM departments WHERE canonical_name = 'Customer Success'
UNION ALL
SELECT 'Support', id FROM departments WHERE canonical_name = 'Customer Support'
UNION ALL
SELECT 'HR', id FROM departments WHERE canonical_name = 'Human Resources'
UNION ALL
SELECT 'People', id FROM departments WHERE canonical_name = 'Human Resources'
UNION ALL
SELECT 'People Ops', id FROM departments WHERE canonical_name = 'Human Resources'
UNION ALL
SELECT 'Ops', id FROM departments WHERE canonical_name = 'Operations'
UNION ALL
SELECT 'CEO Office', id FROM departments WHERE canonical_name = 'Executive'
UNION ALL
SELECT 'Executive Office', id FROM departments WHERE canonical_name = 'Executive'
UNION ALL
SELECT 'Strategy and Corporate Affairs', id FROM departments WHERE canonical_name = 'Strategy'
UNION ALL
SELECT 'Corporate Affairs', id FROM departments WHERE canonical_name = 'Strategy';

-- ============================================================================
-- INITIAL REFERENCE DATA: Locations (Israel-focused)
-- ============================================================================

-- Insert canonical locations
INSERT OR IGNORE INTO locations (canonical_name, country, region) VALUES
    ('Tel Aviv', 'Israel', 'Center'),
    ('Jerusalem', 'Israel', 'Jerusalem'),
    ('Haifa', 'Israel', 'North'),
    ('Petah Tikva', 'Israel', 'Center'),
    ('Yokneam', 'Israel', 'North'),
    ('Rishon LeZion', 'Israel', 'Center'),
    ('Herzliya', 'Israel', 'Center'),
    ('Ramat Gan', 'Israel', 'Center'),
    ('Netanya', 'Israel', 'Center'),
    ('Beer Sheva', 'Israel', 'South'),
    ('Holon', 'Israel', 'Center'),
    ('Bnei Brak', 'Israel', 'Center'),
    ('Raanana', 'Israel', 'Center');

-- Insert app.common location synonyms
INSERT OR IGNORE INTO location_synonyms (synonym, location_id)
SELECT 'TLV', id FROM locations WHERE canonical_name = 'Tel Aviv'
UNION ALL
SELECT 'Tel-Aviv', id FROM locations WHERE canonical_name = 'Tel Aviv'
UNION ALL
SELECT 'Tel Aviv-Yafo', id FROM locations WHERE canonical_name = 'Tel Aviv'
UNION ALL
SELECT 'Petah Tiqva', id FROM locations WHERE canonical_name = 'Petah Tikva'
UNION ALL
SELECT 'Petach Tikva', id FROM locations WHERE canonical_name = 'Petah Tikva'
UNION ALL
SELECT 'Yokne''am', id FROM locations WHERE canonical_name = 'Yokneam'
UNION ALL
SELECT 'Yokne''am Illit', id FROM locations WHERE canonical_name = 'Yokneam'
UNION ALL
SELECT 'Yokneam Illit', id FROM locations WHERE canonical_name = 'Yokneam'
UNION ALL
SELECT 'Ramat-Gan', id FROM locations WHERE canonical_name = 'Ramat Gan'
UNION ALL
SELECT 'Ra''anana', id FROM locations WHERE canonical_name = 'Raanana'
UNION ALL
SELECT 'Ra-anana', id FROM locations WHERE canonical_name = 'Raanana'
UNION ALL
SELECT 'Beersheba', id FROM locations WHERE canonical_name = 'Beer Sheva'
UNION ALL
SELECT 'Be''er Sheva', id FROM locations WHERE canonical_name = 'Beer Sheva';
"""


def get_companies_schema():
    """Return the SQL schema for companies.db"""
    return COMPANIES_SCHEMA


def get_jobs_schema():
    """Return the SQL schema for jobs.db"""
    return JOBS_SCHEMA
