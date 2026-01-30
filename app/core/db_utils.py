#!/usr/bin/env python3
"""
Database Utilities
Provides connection management and app.common operations for SQLite databases.
All database classes support async operations using aiosqlite.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager

import aiosqlite

# Setup logger for this module
logger = logging.getLogger(__name__)


# Database paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
SEARCH_QUERIES_DB = os.path.join(DATA_DIR, "search_queries.db")
COMPANIES_DB = os.path.join(DATA_DIR, "companies.db")
JOBS_DB = os.path.join(DATA_DIR, "jobs.db")
PENDING_EMBEDDED_DB = os.path.join(DATA_DIR, "pending_embedded.db")


def ensure_data_directory():
    """Ensure the data directory exists"""
    os.makedirs(DATA_DIR, exist_ok=True)


def generate_url_hash(url: str) -> str:
    """Generate a hash from a URL for unique identification."""
    if not url:
        return ""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


@contextmanager
def get_db_connection(db_path: str):
    """
    Sync context manager for database connections.
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


@asynccontextmanager
async def get_async_db_connection(db_path: str):
    """
    Async context manager for database connections.
    Automatically handles connection closing and commits.

    Args:
        db_path: Path to the SQLite database file

    Yields:
        aiosqlite.Connection: Async database connection
    """
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    try:
        yield db
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise e
    finally:
        await db.close()


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


"""This will be our first async db, so we will use aiosqlite"""


class PendingEmbeddedDB:
    """Interface for pending_embedded.db operations"""

    def __init__(self, db_path: str = PENDING_EMBEDDED_DB):
        self.db_path = db_path
        self._conn = None
        self.initialize_database()

    ###TODO : remove this when we switch to alembic
    def initialize_database(self):
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT CHECK(status IN ('processing', 'completed', 'failed')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    async def insert_pending_batch_id(self, batch_id: str):
        """Insert a new pending batch ID."""
        try:
            await self._conn.execute(
                """
                    INSERT INTO pending_batches (batch_id, status) VALUES (?, ?)
                """,
                (batch_id, "processing"),
            )
            await self._conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_processing_batches(self):
        """Returns all batches that are currently being processed."""
        try:
            async with self._conn.execute("SELECT * FROM pending_batches WHERE status = 'processing'") as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching processing batches: {e}")
            return []

    async def update_batch_status(self, batch_id: str, status: str) -> bool:
        """Update the status of an existing batch."""
        try:
            await self._conn.execute(
                "UPDATE pending_batches SET status = ? WHERE batch_id = ?",
                (status, batch_id),
            )
            await self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating batch {batch_id}: {e}")
            return False


class CompaniesDB:
    """Interface for companies.db operations - fully sync with per-operation connections."""

    def __init__(self, db_path: str = COMPANIES_DB):
        self.db_path = db_path

    ###TODO : remove this when we switch to alembic
    def initialize_database(self):
        """Initialize the companies database."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT NOT NULL,
                    domain TEXT NOT NULL,
                    company_page_url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_scraped TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    source TEXT DEFAULT 'google_serper'
                )
            """)
            conn.commit()

    def insert_company(self, company_data: dict) -> int | None:
        """Insert a new company into the database."""
        try:
            with get_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO companies (company_name, domain, company_page_url, title, source) VALUES (?, ?, ?, ?, ?)",
                    (
                        company_data.get("company_name"),
                        company_data.get("domain"),
                        company_data.get("company_page_url"),
                        company_data.get("title"),
                        company_data.get("source", "google_serper"),
                    ),
                )
                conn.commit()
                return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def update_last_scraped(self, company_page_url: str) -> bool:
        """Update the last_scraped timestamp for a company."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE companies
                SET last_scraped = CURRENT_TIMESTAMP
                WHERE company_page_url = ?
                """,
                (company_page_url,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_company_inactive(self, company_page_url: str) -> bool:
        """Mark a company as inactive (page no longer available)."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE companies
                SET is_active = 0
                WHERE company_page_url = ?
                """,
                (company_page_url,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def delete_company_by_url(self, company_page_url: str) -> bool:
        """Permanently delete a company record by its job page URL."""
        if not company_page_url:
            return False
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM companies WHERE company_page_url = ?",
                (company_page_url,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def get_company_by_url(self, company_page_url: str) -> dict | None:
        """Get a company by its job page URL."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM companies WHERE company_page_url = ?",
                (company_page_url,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_companies_by_domain(self, domain: str, active_only: bool = True) -> list[dict]:
        """Get all companies for a specific domain."""
        query = "SELECT * FROM companies WHERE domain = ?"
        params = [domain]

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY discovered_at DESC"

        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_stale_companies(self, max_age_hours: int) -> list[dict]:
        """Get companies not scraped within max_age_hours."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM companies
                WHERE is_active = 1
                AND (last_scraped IS NULL OR last_scraped < datetime('now', '-' || ? || ' hours'))
                ORDER BY last_scraped ASC NULLS FIRST
                """,
                (max_age_hours,),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def get_all_companies(self, active_only: bool = True, limit: int | None = None) -> list[dict]:
        """Get all companies."""
        query = "SELECT * FROM companies"
        if active_only:
            query += " WHERE is_active = 1"

        query += " ORDER BY discovered_at DESC"

        if limit:
            query += f" LIMIT {limit}"

        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def count_companies(self, domain: str | None = None, active_only: bool = True) -> int:
        """Count total number of companies."""
        query = "SELECT COUNT(*) FROM companies WHERE 1=1"
        params = []

        if active_only:
            query += " AND is_active = 1"

        if domain:
            query += " AND domain = ?"
            params.append(domain)

        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            return row[0]


class JobsDB:
    """Interface for jobs.db operations - supports both sync and async"""

    def __init__(self, db_path: str = JOBS_DB):
        self.db_path = db_path

    async def connect(self):
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    ###TODO : remove this when we switch to alembic
    def initialize_database(self):
        """Initialize the jobs database with all required tables."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.cursor()
            # Create departments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS departments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL UNIQUE,
                    category TEXT
                )
            """)
            # Create department_synonyms table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS department_synonyms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    synonym TEXT NOT NULL UNIQUE,
                    department_id INTEGER NOT NULL,
                    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE CASCADE
                )
            """)
            # Create locations table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL UNIQUE,
                    country TEXT,
                    region TEXT
                )
            """)
            # Create location_synonyms table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_synonyms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    synonym TEXT NOT NULL UNIQUE,
                    location_id INTEGER NOT NULL,
                    FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE CASCADE
                )
            """)
            # Create jobs table (matching actual schema)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    company_name TEXT,
                    department TEXT,
                    department_id INTEGER,
                    location TEXT,
                    location_id INTEGER,
                    workplace_type TEXT,
                    experience_level TEXT,
                    employment_type TEXT DEFAULT 'Full-time',
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    publish_date TIMESTAMP,
                    description TEXT,
                    uid TEXT,
                    url TEXT UNIQUE NOT NULL,
                    url_hash TEXT UNIQUE,
                    from_domain TEXT,
                    email TEXT,
                    is_ai_inferred BOOLEAN DEFAULT 0,
                    embedding BLOB,
                    original_website_job_url TEXT,
                    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
                    FOREIGN KEY (location_id) REFERENCES locations(id) ON DELETE SET NULL
                )
            """)
            conn.commit()

    async def get_department_id(self, raw_dept: str) -> int | None:
        """
        Get department ID from raw department text using synonym lookup.
        Logs a warning if department is not found.

        """
        if not raw_dept or not raw_dept.strip():
            return None

        # Try exact synonym match first
        async with self._conn.execute(
            """
            SELECT department_id FROM department_synonyms
            WHERE synonym = ? COLLATE NOCASE
            """,
            (raw_dept.strip(),),
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return result[0]

        # Try partial match on canonical name
        async with self._conn.execute(
            """
            SELECT id FROM departments
            WHERE canonical_name = ? COLLATE NOCASE
            """,
            (raw_dept.strip(),),
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return result[0]

        # Not found - log warning
        logger.warning(f"Department not found in reference data: '{raw_dept.strip()}'")
        return None

    async def get_location_id(self, raw_loc: str) -> int | None:
        """
        Get location ID from raw location text using synonym lookup.
        Logs a warning if location is not found.

        Args:
            db: Async database connection
            raw_loc: Raw location text from scraping

        """
        if not raw_loc or not raw_loc.strip():
            return None

        # Handle locations with multiple parts (e.g., "City, IL")
        # Extract main location before commas or parentheses
        clean_loc = raw_loc.split(",")[0].strip()

        # Try exact synonym match first
        async with self._conn.execute(
            """
            SELECT location_id FROM location_synonyms
            WHERE synonym = ? COLLATE NOCASE
            """,
            (clean_loc,),
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return result[0]

        # Try partial match on canonical name
        async with self._conn.execute(
            """
            SELECT id FROM locations
            WHERE canonical_name = ? COLLATE NOCASE
            """,
            (clean_loc,),
        ) as cursor:
            result = await cursor.fetchone()
            if result:
                return result[0]

        # Not found - log warning
        logger.warning(f"Location not found in reference data: '{clean_loc}' (from '{raw_loc}')")
        return None

    async def insert_job(self, job_data: dict) -> int | None:
        """
        Insert a new job into the database.
        Handles duplicate prevention via URL uniqueness.
        Automatically normalizes departments and locations.

        Jobs will be inserted in batches if we want we can write additional sync method for testing
        """
        try:
            # Validate required fields
            url = job_data.get("url")
            if not url:
                logger.error("Cannot insert job without URL")
                return None

            # Normalize department and location
            dept_id = await self.get_department_id(self._conn, job_data.get("department"))
            loc_id = await self.get_location_id(self._conn, job_data.get("location"))

            # Handle description - convert dict to text if needed
            description = job_data.get("description", "")
            if isinstance(description, dict):
                description = "\n\n".join(f"{k}:\n{v}" for k, v in description.items() if v)

            # Generate URL hash
            url_hash = generate_url_hash(url)

            # Parse from_domain from URL
            from_domain = None
            if url:
                from urllib.parse import urlparse

                from_domain = urlparse(url).netloc

            async with self._conn.execute(
                """
                INSERT INTO jobs (
                    title, company_name, department, department_id,
                    location, location_id, workplace_type, experience_level,
                    employment_type, publish_date, description, uid,
                    url, url_hash, from_domain, email, is_ai_inferred,
                    original_website_job_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_data.get("title"),
                    job_data.get("company_name"),
                    job_data.get("department"),
                    dept_id,
                    job_data.get("location"),
                    loc_id,
                    job_data.get("workplace_type"),
                    job_data.get("experience_level"),
                    job_data.get("employment_type", "Full-time"),
                    job_data.get("last_updated") or job_data.get("publish_date"),
                    description,
                    job_data.get("uid"),
                    url,
                    url_hash,
                    from_domain,
                    job_data.get("email"),
                    job_data.get("is_ai_inferred", False),
                    job_data.get("original_website_job_url"),
                ),
            ) as cursor:
                await self._conn.commit()
                return cursor.lastrowid

        except aiosqlite.IntegrityError as e:
            # Check if it's a URL duplicate (most app.common case)
            error_msg = str(e).lower()
            if "url" in error_msg or "unique constraint" in error_msg:
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

    async def update_job_embedding(self, job_id: int, embedding: bytes) -> bool:
        """
        Update the embedding for a specific job.

        Args:
            db: Async database connection
            job_id: ID of the job to update
            embedding: Pickled numpy array as bytes (BLOB)
        """
        try:
            await self._conn.execute(
                """
                UPDATE jobs
                SET embedding = ?
                WHERE id = ?
                """,
                (embedding, job_id),
            )
            await self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating embedding for job ID {job_id}: {e}", exc_info=True)
            return False

    async def filter_existing_jobs(self, jobs: list[dict]) -> list[dict]:
        """Return only jobs that don't exist in DB yet."""
        # Generate hashes for all jobs
        url_hashes = [generate_url_hash(job["url"]) for job in jobs if job.get("url")]

        placeholders = ",".join("?" * len(url_hashes))
        query = f"SELECT url_hash FROM jobs WHERE url_hash IN ({placeholders})"
        cursor = await self._conn.execute(query, url_hashes)
        existing = {row[0] for row in await cursor.fetchall()}

        # Filter out existing
        new_jobs = [j for j in jobs if generate_url_hash(j.get("url")) not in existing]
        return new_jobs

    async def get_jobs_without_embeddings(self, limit: int | None = None) -> list[dict]:
        """
        Get jobs that don't have embeddings yet.

        Args:
            db: Async database connection
            limit: Optional limit on number of results
        """
        # Ensure embedding column exists (for existing databases)
        await self._ensure_embedding_column()

        query = """
            SELECT * FROM jobs
            WHERE embedding IS NULL
            AND description IS NOT NULL
            AND description != ''
            ORDER BY scraped_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute(query) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def _ensure_embedding_column(self):
        """
        Ensure the embedding column exists in the jobs table.
        Adds it if missing (for existing databases that don't have it yet).

        Args:
            db: Async database connection
        """
        try:
            # Check if column exists by trying to select it
            async with self._conn.execute("PRAGMA table_info(jobs)") as cursor:
                rows = await cursor.fetchall()
                columns = [row[1] for row in rows]

            if "embedding" not in columns:
                logger.info("Adding embedding column to jobs table...")
                await self._conn.execute("ALTER TABLE jobs ADD COLUMN embedding BLOB")
                await self._conn.commit()
                logger.info("Embedding column added successfully")
        except Exception as e:
            # Column might already exist or table might not exist yet
            logger.debug(f"Embedding column check: {e}")

    async def get_job_by_url(self, url: str) -> dict | None:
        """Get a job by its URL.

        Args:
            db: Async database connection
            url: Job URL to search for
        """
        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute("SELECT * FROM jobs WHERE url = ?", (url,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def delete_job_by_url(self, url: str) -> bool:
        """Delete a job by URL, returning True when a row was removed.

        Args:
            db: Async database connection
            url: Job URL to delete
        """
        if not url:
            return False

        cursor = await self._conn.execute("DELETE FROM jobs WHERE url = ?", (url,))
        await self._conn.commit()
        return cursor.rowcount > 0

    async def get_jobs_by_company(self, company_name: str, limit: int | None = None) -> list[dict]:
        """Get all jobs for a specific company.

        Args:
            db: Async database connection
            company_name: Company name to filter by
            limit: Optional limit on number of results
        """
        query = "SELECT * FROM jobs WHERE company_name = ? ORDER BY scraped_at DESC"
        if limit:
            query += f" LIMIT {limit}"

        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute(query, (company_name,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_jobs_by_filters(
        self,
        workplace_type: str | None = None,
        experience_level: str | None = None,
        employment_type: str | None = None,
        department_id: int | None = None,
        location_id: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Get jobs filtered by various criteria.

        Args:
            db: Async database connection
            workplace_type: Filter by workplace type
            experience_level: Filter by experience level
            employment_type: Filter by employment type
            department_id: Filter by department ID
            location_id: Filter by location ID
            limit: Optional limit on number of results

        Returns:
            list of job records as dictionaries
        """
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

        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_jobs(
        self,
        workplace_type: str | None = None,
        experience_level: str | None = None,
        department_id: int | None = None,
        location_id: int | None = None,
    ) -> int:
        """
        Count jobs with optional filters.

        Args:
            db: Async database connection
            workplace_type: Filter by workplace type
            experience_level: Filter by experience level
            department_id: Filter by department ID
            location_id: Filter by location ID

        Returns:
            Number of jobs matching filters
        """
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

        async with self._conn.execute(query, params) as cursor:
            row = await cursor.fetchone()
            return row[0]

    async def get_all_departments(self) -> list[dict]:
        """
        Get all departments with their synonyms.
        """
        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute(
            """
            SELECT d.*, GROUP_CONCAT(ds.synonym, ', ') as synonyms
            FROM departments d
            LEFT JOIN department_synonyms ds ON d.id = ds.department_id
            GROUP BY d.id
            ORDER BY d.canonical_name
            """
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # Sync versions for manual/UI tools
    def get_all_departments_sync(self) -> list[dict]:
        """Sync version for UI tools."""
        with get_db_connection(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT d.*, GROUP_CONCAT(ds.synonym, ', ') as synonyms
                FROM departments d
                LEFT JOIN department_synonyms ds ON d.id = ds.department_id
                GROUP BY d.id
                ORDER BY d.canonical_name
            """)
            return [dict(row) for row in cursor.fetchall()]

    async def get_all_locations(self) -> list[dict]:
        """
        Get all locations with their synonyms.

        Args:
            db: Async database connection

        Returns:
            list of location records
        """
        self._conn.row_factory = aiosqlite.Row
        async with self._conn.execute(
            """
            SELECT l.*, GROUP_CONCAT(ls.synonym, ', ') as synonyms
            FROM locations l
            LEFT JOIN location_synonyms ls ON l.id = ls.location_id
            GROUP BY l.id
            ORDER BY l.canonical_name
            """
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def verify_database(self) -> dict:
        """
        Verify the database structure and return statistics.

        Args:
            db: Async database connection

        Returns:
            dictionary with verification results including:
            - tables_exist: list of tables found
            - jobs_count: Number of jobs in database
            - departments_count: Number of departments
            - locations_count: Number of locations
            - sample_job: A sample job record (if any exist)
        """
        result = {
            "tables_exist": [],
            "jobs_count": 0,
            "departments_count": 0,
            "locations_count": 0,
            "sample_job": None,
            "errors": [],
        }

        try:
            # Get all tables
            async with self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") as cursor:
                rows = await cursor.fetchall()
                result["tables_exist"] = [row[0] for row in rows]

            # Count jobs
            try:
                async with self._conn.execute("SELECT COUNT(*) FROM jobs") as cursor:
                    row = await cursor.fetchone()
                    result["jobs_count"] = row[0]
            except Exception as e:
                result["errors"].append(f"Error counting jobs: {e}")

            # Count departments
            try:
                async with self._conn.execute("SELECT COUNT(*) FROM departments") as cursor:
                    row = await cursor.fetchone()
                    result["departments_count"] = row[0]
            except Exception as e:
                result["errors"].append(f"Error counting departments: {e}")

            # Count locations
            try:
                async with self._conn.execute("SELECT COUNT(*) FROM locations") as cursor:
                    row = await cursor.fetchone()
                    result["locations_count"] = row[0]
            except Exception as e:
                result["errors"].append(f"Error counting locations: {e}")

            # Get a sample job
            try:
                self._conn.row_factory = aiosqlite.Row
                async with self._conn.execute("SELECT * FROM jobs LIMIT 1") as cursor:
                    row = await cursor.fetchone()
                    if row:
                        result["sample_job"] = dict(row)
            except Exception as e:
                result["errors"].append(f"Error fetching sample job: {e}")

        except Exception as e:
            result["errors"].append(f"Database error: {e}")

        return result
