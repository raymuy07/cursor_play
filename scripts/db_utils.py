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
COMPANIES_PAGES_DB = os.path.join(DATA_DIR, 'companies_pages.db')


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
    
    def log_search(self, query: str, source: str, results_count: int = 0) -> int:
        """
        Log a search query to the database.
        
        Args:
            query: The search query string
            source: Source of the search (e.g., 'google', 'indeed')
            results_count: Number of results returned
            
        Returns:
            ID of the inserted record
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO search_queries (query, source, results_count)
                VALUES (?, ?, ?)
                """,
                (query, source, results_count)
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
    
    def get_search_by_query(self, query: str, source: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent search for a specific query and source.
        
        Args:
            query: The search query string
            source: Source of the search
            
        Returns:
            Search record as dictionary or None if not found
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM search_queries
                WHERE query = ? AND source = ?
                ORDER BY searched_at DESC
                LIMIT 1
                """,
                (query, source)
            )
            row = cursor.fetchone()
            return dict(row) if row else None


class JobsDB:
    """Interface for companies_pages.db (jobs table) operations"""
    
    def __init__(self, db_path: str = COMPANIES_PAGES_DB):
        self.db_path = db_path
    
    def insert_job(self, job_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a new job into the database.
        Handles duplicate prevention via job_hash and URL uniqueness.
        
        Args:
            job_data: Dictionary containing job information
            
        Returns:
            ID of the inserted record, or None if duplicate
        """
        # Generate job hash if not provided
        if 'job_hash' not in job_data:
            job_data['job_hash'] = generate_job_hash(
                job_data.get('url', ''),
                job_data.get('title', '')
            )
        
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Prepare the INSERT statement
                columns = [
                    'job_hash', 'url', 'title', 'company', 'description',
                    'source', 'department', 'location', 'employment_type',
                    'experience_level', 'workplace_type', 'uid', 
                    'company_name', 'last_updated'
                ]
                
                values = [job_data.get(col) for col in columns]
                placeholders = ','.join(['?' for _ in columns])
                
                cursor.execute(
                    f"""
                    INSERT INTO jobs ({','.join(columns)})
                    VALUES ({placeholders})
                    """,
                    values
                )
                return cursor.lastrowid
                
        except sqlite3.IntegrityError:
            # Duplicate job (hash or URL already exists)
            return None
    
    def update_job_checked(self, job_hash: str) -> bool:
        """
        Update the last_checked timestamp for a job.
        
        Args:
            job_hash: Hash of the job to update
            
        Returns:
            True if job was updated, False otherwise
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET last_checked = CURRENT_TIMESTAMP
                WHERE job_hash = ?
                """,
                (job_hash,)
            )
            return cursor.rowcount > 0
    
    def mark_job_inactive(self, job_hash: str) -> bool:
        """
        Mark a job as inactive (no longer available).
        
        Args:
            job_hash: Hash of the job to mark inactive
            
        Returns:
            True if job was updated, False otherwise
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE jobs
                SET is_active = 0, last_checked = CURRENT_TIMESTAMP
                WHERE job_hash = ?
                """,
                (job_hash,)
            )
            return cursor.rowcount > 0
    
    def get_job_by_hash(self, job_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get a job by its hash.
        
        Args:
            job_hash: Hash of the job
            
        Returns:
            Job record as dictionary or None if not found
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM jobs WHERE job_hash = ?",
                (job_hash,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_job_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get a job by its URL.
        
        Args:
            url: Job URL
            
        Returns:
            Job record as dictionary or None if not found
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM jobs WHERE url = ?",
                (url,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_active_jobs(self, company: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all active jobs, optionally filtered by company.
        
        Args:
            company: Optional company filter
            limit: Optional limit on number of results
            
        Returns:
            List of job records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM jobs WHERE is_active = 1"
            params = []
            
            if company:
                query += " AND company = ?"
                params.append(company)
            
            query += " ORDER BY fetched_at DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_jobs(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all jobs (active and inactive).
        
        Args:
            limit: Optional limit on number of results
            
        Returns:
            List of job records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM jobs ORDER BY fetched_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def count_jobs(self, active_only: bool = True) -> int:
        """
        Count total number of jobs.
        
        Args:
            active_only: If True, count only active jobs
            
        Returns:
            Number of jobs
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT COUNT(*) FROM jobs"
            if active_only:
                query += " WHERE is_active = 1"
            
            cursor.execute(query)
            return cursor.fetchone()[0]
