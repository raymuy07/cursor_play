#!/usr/bin/env python3
"""
Database Schema Definitions
Defines the schema for both search_queries.db and companies_pages.db
"""

# Schema for search_queries.db
SEARCH_QUERIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS search_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,  -- 'comeet', 'lever', 'greenhouse', etc.
    query TEXT NOT NULL,
    source TEXT NOT NULL,  -- 'google_serper', 'indeed', etc.
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    results_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_domain ON search_queries(domain);
CREATE INDEX IF NOT EXISTS idx_query ON search_queries(query);
CREATE INDEX IF NOT EXISTS idx_source ON search_queries(source);
CREATE INDEX IF NOT EXISTS idx_searched_at ON search_queries(searched_at);
"""

# Schema for companies.db
COMPANIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,  -- 'comeet', 'lever', 'greenhouse', etc.
    job_page_url TEXT UNIQUE NOT NULL,  -- The main job listings page for this company
    title TEXT,  -- Page title from search results
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP,  -- When we last scraped jobs from this page
    is_active BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'google_serper'  -- How we found this company
);

CREATE INDEX IF NOT EXISTS idx_company_name ON companies(company_name);
CREATE INDEX IF NOT EXISTS idx_domain ON companies(domain);
CREATE INDEX IF NOT EXISTS idx_job_page_url ON companies(job_page_url);
CREATE INDEX IF NOT EXISTS idx_is_active ON companies(is_active);
CREATE INDEX IF NOT EXISTS idx_last_scraped ON companies(last_scraped);
CREATE INDEX IF NOT EXISTS idx_discovered_at ON companies(discovered_at);
"""


def get_search_queries_schema():
    """Return the SQL schema for search_queries.db"""
    return SEARCH_QUERIES_SCHEMA


def get_companies_schema():
    """Return the SQL schema for companies table in companies.db"""
    return COMPANIES_SCHEMA
