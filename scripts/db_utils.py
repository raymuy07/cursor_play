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
from scripts.utils import setup_logging

# Setup logger for this module
logger = setup_logging()


# Database paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
SEARCH_QUERIES_DB = os.path.join(DATA_DIR, 'search_queries.db')
COMPANIES_DB = os.path.join(DATA_DIR, 'companies.db')
JOBS_DB = os.path.join(DATA_DIR, 'jobs.db')


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


class JobsDB:
    """Interface for jobs.db operations"""
    
    def __init__(self, db_path: str = JOBS_DB):
        self.db_path = db_path
    
    def get_department_id(self, raw_dept: str) -> Optional[int]:
        """
        Get department ID from raw department text using synonym lookup.
        Logs a warning if department is not found.
        
        Args:
            raw_dept: Raw department text from scraping
            
        Returns:
            Department ID or None if not found or if raw_dept is None/empty
        """
        if not raw_dept or not raw_dept.strip():
            return None
        
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Try exact synonym match first
            cursor.execute(
                """
                SELECT department_id FROM department_synonyms 
                WHERE synonym = ? COLLATE NOCASE
                """,
                (raw_dept.strip(),)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Try partial match on canonical name
            cursor.execute(
                """
                SELECT id FROM departments 
                WHERE canonical_name = ? COLLATE NOCASE
                """,
                (raw_dept.strip(),)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Not found - log warning
            logger.warning(f"Department not found in reference data: '{raw_dept.strip()}'")
            return None
    
    def get_location_id(self, raw_loc: str) -> Optional[int]:
        """
        Get location ID from raw location text using synonym lookup.
        Logs a warning if location is not found.
        
        Args:
            raw_loc: Raw location text from scraping
            
        Returns:
            Location ID or None if not found or if raw_loc is None/empty
        """
        if not raw_loc or not raw_loc.strip():
            return None
        
        # Handle locations with multiple parts (e.g., "City, IL")
        # Extract main location before commas or parentheses
        clean_loc = raw_loc.split(',')[0].strip()
        
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Try exact synonym match first
            cursor.execute(
                """
                SELECT location_id FROM location_synonyms 
                WHERE synonym = ? COLLATE NOCASE
                """,
                (clean_loc,)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Try partial match on canonical name
            cursor.execute(
                """
                SELECT id FROM locations 
                WHERE canonical_name = ? COLLATE NOCASE
                """,
                (clean_loc,)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Not found - log warning
            logger.warning(f"Location not found in reference data: '{clean_loc}' (from '{raw_loc}')")
            return None
    
    def insert_job(self, job_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a new job into the database.
        Handles duplicate prevention via URL uniqueness.
        Automatically normalizes departments and locations.
        
        Args:
            job_data: Dictionary containing job information
                Required: title, url
                Optional: All other job fields
            
        Returns:
            ID of the inserted record, or None if duplicate or error occurred
        """
        try:
            # Validate required fields
            url = job_data.get('url')
            if not url:
                logger.error("Cannot insert job without URL")
                return None
            
            # Normalize department and location
            dept_id = self.get_department_id(job_data.get('department'))
            loc_id = self.get_location_id(job_data.get('location'))
            
            # Handle description - convert dict to text if needed
            description = job_data.get('description', '')
            if isinstance(description, dict):
                description = '\n\n'.join(f"{k}:\n{v}" for k, v in description.items() if v)
            
            # Generate URL hash
            url_hash = generate_job_hash(url, job_data.get('title', ''))
            
            # Parse from_domain from URL
            from_domain = None
            if url:
                from urllib.parse import urlparse
                from_domain = urlparse(url).netloc
            
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    INSERT INTO jobs (
                        title, company_name, department, department_id,
                        location, location_id, workplace_type, experience_level,
                        employment_type, publish_date, description, uid,
                        url, url_hash, from_domain, email, is_ai_inferred
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_data.get('title'),
                        job_data.get('company_name'),
                        job_data.get('department'),
                        dept_id,
                        job_data.get('location'),
                        loc_id,
                        job_data.get('workplace_type'),
                        job_data.get('experience_level'),
                        job_data.get('employment_type', 'Full-time'),
                        job_data.get('last_updated') or job_data.get('publish_date'),
                        description,
                        job_data.get('uid'),
                        url,
                        url_hash,
                        from_domain,
                        job_data.get('email'),
                        job_data.get('is_ai_inferred', False)
                    )
                )
                return cursor.lastrowid
                
        except sqlite3.IntegrityError as e:
            # Check if it's a URL duplicate (most common case)
            error_msg = str(e).lower()
            if 'url' in error_msg or 'unique constraint' in error_msg:
                # Likely a duplicate URL or hash collision
                return None
            else:
                # Other constraint violation
                logger.warning(f"Integrity constraint violation (not URL): {e}")
                return None
        except Exception as e:
            # Catch any other errors (encoding, database issues, etc.)
            logger.error(f"Error inserting job with URL '{job_data.get('url')}': {e}", exc_info=True)
            return None
    


    ##I still dont know if we need this function
    def get_job_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get a job by its URL.
        
        Args:
            url: Job posting URL
            
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
    
    def get_jobs_by_company(self, company_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all jobs for a specific company.
        
        Args:
            company_name: Company name to filter by
            limit: Optional limit on number of results
            
        Returns:
            List of job records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM jobs WHERE company_name = ? ORDER BY scraped_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, (company_name,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_jobs_by_filters(
        self,
        workplace_type: Optional[str] = None,
        experience_level: Optional[str] = None,
        employment_type: Optional[str] = None,
        department_id: Optional[int] = None,
        location_id: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get jobs filtered by various criteria.
        
        Args:
            workplace_type: Filter by workplace type
            experience_level: Filter by experience level
            employment_type: Filter by employment type
            department_id: Filter by department ID
            location_id: Filter by location ID
            limit: Optional limit on number of results
            
        Returns:
            List of job records as dictionaries
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM jobs WHERE 1=1"
            params = []
            
            if workplace_type:
                query += " AND workplace_type = ?"
                params.append(workplace_type)
            
            if experience_level:
                query += " AND experience_level = ?"
                params.append(experience_level)
            
            if employment_type:
                query += " AND employment_type = ?"
                params.append(employment_type)
            
            if department_id:
                query += " AND department_id = ?"
                params.append(department_id)
            
            if location_id:
                query += " AND location_id = ?"
                params.append(location_id)
            
            query += " ORDER BY scraped_at DESC"
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def count_jobs(
        self,
        workplace_type: Optional[str] = None,
        experience_level: Optional[str] = None,
        department_id: Optional[int] = None,
        location_id: Optional[int] = None
    ) -> int:
        """
        Count jobs with optional filters.
        
        Args:
            workplace_type: Filter by workplace type
            experience_level: Filter by experience level
            department_id: Filter by department ID
            location_id: Filter by location ID
            
        Returns:
            Number of jobs matching filters
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT COUNT(*) FROM jobs WHERE 1=1"
            params = []
            
            if workplace_type:
                query += " AND workplace_type = ?"
                params.append(workplace_type)
            
            if experience_level:
                query += " AND experience_level = ?"
                params.append(experience_level)
            
            if department_id:
                query += " AND department_id = ?"
                params.append(department_id)
            
            if location_id:
                query += " AND location_id = ?"
                params.append(location_id)
            
            cursor.execute(query, params)
            return cursor.fetchone()[0]
    

    #I still dont know if we need this function
    def get_all_departments(self) -> List[Dict[str, Any]]:
        """
        Get all departments with their synonyms.
        
        Returns:
            List of department records
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT d.*, GROUP_CONCAT(ds.synonym, ', ') as synonyms
                FROM departments d
                LEFT JOIN department_synonyms ds ON d.id = ds.department_id
                GROUP BY d.id
                ORDER BY d.canonical_name
                """
            )
            return [dict(row) for row in cursor.fetchall()]
    
    
    #!!!I still dont know if we need this function
    def get_all_locations(self) -> List[Dict[str, Any]]:
        """
        Get all locations with their synonyms.
        
        Returns:
            List of location records
        """
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT l.*, GROUP_CONCAT(ls.synonym, ', ') as synonyms
                FROM locations l
                LEFT JOIN location_synonyms ls ON l.id = ls.location_id
                GROUP BY l.id
                ORDER BY l.canonical_name
                """
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def verify_database(self) -> Dict[str, Any]:
        """
        Verify the database structure and return statistics.
        
        Returns:
            Dictionary with verification results including:
            - tables_exist: List of tables found
            - jobs_count: Number of jobs in database
            - departments_count: Number of departments
            - locations_count: Number of locations
            - sample_job: A sample job record (if any exist)
        """
        result = {
            'tables_exist': [],
            'jobs_count': 0,
            'departments_count': 0,
            'locations_count': 0,
            'sample_job': None,
            'errors': []
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            try:
                # Get all tables
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                result['tables_exist'] = [row[0] for row in cursor.fetchall()]
                
                # Count jobs
                try:
                    cursor.execute("SELECT COUNT(*) FROM jobs")
                    result['jobs_count'] = cursor.fetchone()[0]
                except sqlite3.OperationalError as e:
                    result['errors'].append(f"Error counting jobs: {e}")
                
                # Count departments
                try:
                    cursor.execute("SELECT COUNT(*) FROM departments")
                    result['departments_count'] = cursor.fetchone()[0]
                except sqlite3.OperationalError as e:
                    result['errors'].append(f"Error counting departments: {e}")
                
                # Count locations
                try:
                    cursor.execute("SELECT COUNT(*) FROM locations")
                    result['locations_count'] = cursor.fetchone()[0]
                except sqlite3.OperationalError as e:
                    result['errors'].append(f"Error counting locations: {e}")
                
                # Get a sample job
                try:
                    cursor.execute("SELECT * FROM jobs LIMIT 1")
                    row = cursor.fetchone()
                    if row:
                        result['sample_job'] = dict(row)
                except sqlite3.OperationalError as e:
                    result['errors'].append(f"Error fetching sample job: {e}")
            finally:
                conn.close()
                
        except Exception as e:
            result['errors'].append(f"Database connection error: {e}")
        
        return result
