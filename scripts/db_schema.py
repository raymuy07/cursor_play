#!/usr/bin/env python3
"""
Database Schema Definitions
Defines the schema for both search_queries.db and companies_pages.db
"""

# Schema for search_queries.db
SEARCH_QUERIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    source TEXT NOT NULL,  -- 'google', 'indeed', etc.
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    results_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_query ON search_queries(query);
CREATE INDEX IF NOT EXISTS idx_source ON search_queries(source);
CREATE INDEX IF NOT EXISTS idx_searched_at ON search_queries(searched_at);
"""

# Schema for companies_pages.db
JOBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_hash TEXT UNIQUE NOT NULL,  -- Prevents duplicates
    url TEXT UNIQUE NOT NULL,        -- Also unique
    title TEXT,
    company TEXT,
    description TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    source TEXT,  -- 'google_api', 'indeed', 'comeet', etc.
    
    -- Additional fields from our current job structure
    department TEXT,
    location TEXT,
    employment_type TEXT,
    experience_level TEXT,
    workplace_type TEXT,
    uid TEXT,
    company_name TEXT,
    last_updated TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_hash ON jobs(job_hash);
CREATE INDEX IF NOT EXISTS idx_url ON jobs(url);
CREATE INDEX IF NOT EXISTS idx_company ON jobs(company);
CREATE INDEX IF NOT EXISTS idx_is_active ON jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_fetched_at ON jobs(fetched_at);
"""


def get_search_queries_schema():
    """Return the SQL schema for search_queries.db"""
    return SEARCH_QUERIES_SCHEMA


def get_jobs_schema():
    """Return the SQL schema for jobs table in companies_pages.db"""
    return JOBS_SCHEMA
