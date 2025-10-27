#!/usr/bin/env python3
"""
Database Utilities
Provides connection management and common operations for SQLite databases
"""

import sqlite3
import hashlib
import os
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

# Database paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
SEARCH_QUERIES_DB = os.path.join(DATA_DIR, 'search_queries.db')
COMPANIES_DB = os.path.join(DATA_DIR, 'companies.db')


def ensure_data_directory():
    """Ensure the data directory exists"""
    os.makedirs(DATA_DIR, exist_ok=True)


@contextmanager
def get_db_connection(db_path: str):
    """
    Context manager for database connections.
    Automatically handles connection closing and commits.
    
    Args:
        db_path: Path to the SQLite database file
        
    Yields:
        sqlite3.Connection: Database connection
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def initialize_database(db_path: str, schema: str):
    """
    Initialize a database with the given schema.
    
    Args:
        db_path: Path to the SQLite database file
        schema: SQL schema definition string
    """
    ensure_data_directory()
    
    with get_db_connection(db_path) as conn:
        cursor = conn.cursor()
        # Execute each statement in the schema
        cursor.executescript(schema)
        conn.commit()


def generate_job_hash(url: str, title: str = "") -> str:
    """
    Generate a unique hash for a job posting.
    Uses URL as primary identifier, with title as backup.
    
    Args:
        url: Job URL
        title: Job title (optional)
        
    Returns:
        MD5 hash string
    """
    if not url:
        # If no URL, use title + timestamp (less ideal but handles edge cases)
        content = f"{title}_{datetime.now().isoformat()}"
    else:
        content = url
    
    return hashlib.md5(content.encode('utf-8')).hexdigest()


class SearchQueriesDB:
    """Interface for search_queries.db operations"""
    
    def __init__(self, db_path: str = SEARCH_QUERIES_DB):
        self.db_path = db_path
    
    def log_search(self, domain: str, query: str, source: str, results_count: int = 0) -> int:
        """
        Log a search query to the database.
        
        Args:
            domain: Domain being searched (e.g., 'comeet', 'lever')
            query: The search query string
            source: Source of the search (e.g., 'google_serper', 'indeed')
            results_count: Number of results returned
            
        Returns:
            ID of the inserted record
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO search_queries (domain, query, source, results_count)
                VALUES (?, ?, ?, ?)
                """,
                (domain, query, source, results_count)
            )
            return cursor.lastrowid
    
    def get_recent_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent search queries.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of search query records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM search_queries
                ORDER BY searched_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_search_by_domain(self, domain: str, source: str = 'google_serper') -> Optional[Dict[str, Any]]:
        """
        Get the most recent search for a specific domain.
        
        Args:
            domain: The domain (e.g., 'comeet', 'lever')
            source: Source of the search
            
        Returns:
            Search record as dictionary or None if not found
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM search_queries
                WHERE domain = ? AND source = ?
                ORDER BY searched_at DESC
                LIMIT 1
                """,
                (domain, source)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_domains_to_search(self, max_age_hours: int = 24) -> List[str]:
        """
        Get list of domains that haven't been searched recently.
        
        Args:
            max_age_hours: Consider domains not searched in this many hours
            
        Returns:
            List of domain names that need to be searched
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT domain 
                FROM search_queries
                WHERE searched_at < datetime('now', '-' || ? || ' hours')
                ORDER BY searched_at ASC
                """,
                (max_age_hours,)
            )
            return [row['domain'] for row in cursor.fetchall()]


class CompaniesDB:
    """Interface for companies.db operations"""
    
    def __init__(self, db_path: str = COMPANIES_DB):
        self.db_path = db_path
    
    def insert_company(self, company_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a new company into the database.
        Handles duplicate prevention via job_page_url uniqueness.
        
        Args:
            company_data: Dictionary containing company information
                Required: company_name, domain, job_page_url
                Optional: title, source
            
        Returns:
            ID of the inserted record, or None if duplicate
        """
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    INSERT INTO companies (company_name, domain, job_page_url, title, source)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        company_data.get('company_name'),
                        company_data.get('domain'),
                        company_data.get('job_page_url'),
                        company_data.get('title'),
                        company_data.get('source', 'google_serper')
                    )
                )
                return cursor.lastrowid
                
        except sqlite3.IntegrityError:
            # Duplicate company (job_page_url already exists)
            return None
    
    def update_last_scraped(self, job_page_url: str) -> bool:
        """
        Update the last_scraped timestamp for a company.
        
        Args:
            job_page_url: URL of the company's job page
            
        Returns:
            True if company was updated, False otherwise
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE companies
                SET last_scraped = CURRENT_TIMESTAMP
                WHERE job_page_url = ?
                """,
                (job_page_url,)
            )
            return cursor.rowcount > 0
    
    def mark_company_inactive(self, job_page_url: str) -> bool:
        """
        Mark a company as inactive (page no longer available).
        
        Args:
            job_page_url: URL of the company's job page
            
        Returns:
            True if company was updated, False otherwise
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE companies
                SET is_active = 0
                WHERE job_page_url = ?
                """,
                (job_page_url,)
            )
            return cursor.rowcount > 0
    
    def get_company_by_url(self, job_page_url: str) -> Optional[Dict[str, Any]]:
        """
        Get a company by its job page URL.
        
        Args:
            job_page_url: URL of the company's job page
            
        Returns:
            Company record as dictionary or None if not found
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM companies WHERE job_page_url = ?",
                (job_page_url,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_companies_by_domain(self, domain: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all companies for a specific domain.
        
        Args:
            domain: Domain to filter by (e.g., 'comeet', 'lever')
            active_only: If True, only return active companies
            
        Returns:
            List of company records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM companies WHERE domain = ?"
            params = [domain]
            
            if active_only:
                query += " AND is_active = 1"
            
            query += " ORDER BY discovered_at DESC"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_companies_to_scrape(self, limit: Optional[int] = None, max_age_hours: int = 168) -> List[Dict[str, Any]]:
        """
        Get companies that need to be scraped (never scraped or not scraped recently).
        
        Args:
            limit: Optional limit on number of results
            max_age_hours: Consider companies not scraped in this many hours (default 7 days)
            
        Returns:
            List of company records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT * FROM companies 
                WHERE is_active = 1 
                AND (last_scraped IS NULL OR last_scraped < datetime('now', '-' || ? || ' hours'))
                ORDER BY last_scraped ASC NULLS FIRST
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, (max_age_hours,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_companies(self, active_only: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all companies.
        
        Args:
            active_only: If True, only return active companies
            limit: Optional limit on number of results
            
        Returns:
            List of company records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM companies"
            if active_only:
                query += " WHERE is_active = 1"
            
            query += " ORDER BY discovered_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def count_companies(self, domain: Optional[str] = None, active_only: bool = True) -> int:
        """
        Count total number of companies.
        
        Args:
            domain: Optional domain filter
            active_only: If True, count only active companies
            
        Returns:
            Number of companies
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT COUNT(*) FROM companies WHERE 1=1"
            params = []
            
            if active_only:
                query += " AND is_active = 1"
            
            if domain:
                query += " AND domain = ?"
                params.append(domain)
            
            cursor.execute(query, params)
            return cursor.fetchone()[0]
