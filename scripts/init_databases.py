#!/usr/bin/env python3
"""
Database Initialization Script
Creates and initializes both search_queries.db and companies_pages.db
"""

import os
import sys
from utils import setup_logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_schema import get_search_queries_schema, get_companies_schema, get_jobs_schema
from scripts.db_utils import (
    initialize_database,
    SEARCH_QUERIES_DB,
    COMPANIES_DB,
    JOBS_DB,
    SearchQueriesDB,
    CompaniesDB,
    JobsDB
)


def init_search_queries_db():
    """Initialize search_queries.db"""
    logger = setup_logging()
    
    logger.info(f"Initializing search_queries.db at {SEARCH_QUERIES_DB}")
    
    schema = get_search_queries_schema()
    initialize_database(SEARCH_QUERIES_DB, schema)
    
    logger.info("[OK] search_queries.db initialized successfully")
    
    # Test the database
    db = SearchQueriesDB()
    test_id = db.log_search("comeet", "site:comeet.com jobs", "google_serper", 10)
    logger.info(f"  - Test record inserted with ID: {test_id}")
    
    recent = db.get_recent_searches(1)
    logger.info(f"  - Test record retrieved: {recent[0] if recent else 'None'}")
    
    return True


def init_companies_db():
    """Initialize companies.db"""
    logger = setup_logging()
    
    logger.info(f"Initializing companies.db at {COMPANIES_DB}")
    
    schema = get_companies_schema()
    initialize_database(COMPANIES_DB, schema)
    
    logger.info("[OK] companies.db initialized successfully")
    
    # Test the database
    db = CompaniesDB()
    
    # Try inserting a test company
    test_company = {
        'company_name': 'Test Company',
        'domain': 'comeet',
        'job_page_url': 'https://www.comeet.com/jobs/testcompany/TEST.001',
        'title': 'Jobs at Test Company - Comeet',
        'source': 'google_serper'
    }
    
    test_id = db.insert_company(test_company)
    if test_id:
        logger.info(f"  - Test company inserted with ID: {test_id}")
        
        # Try to insert duplicate (should fail gracefully)
        duplicate_id = db.insert_company(test_company)
        if duplicate_id is None:
            logger.info("  - Duplicate prevention working correctly")
        
        # Retrieve the company
        company = db.get_company_by_url(test_company['job_page_url'])
        logger.info(f"  - Test company retrieved: {company['company_name'] if company else 'None'}")
    else:
        logger.warning("  - Test company insertion failed (may already exist)")
    
    return True


def init_jobs_db():
    """Initialize jobs.db"""
    logger = setup_logging()
    
    logger.info(f"Initializing jobs.db at {JOBS_DB}")
    
    schema = get_jobs_schema()
    initialize_database(JOBS_DB, schema)
    
    logger.info("[OK] jobs.db initialized successfully")
    
    # Test the database
    db = JobsDB()
    
    # Test department and location retrieval
    departments = db.get_all_departments()
    locations = db.get_all_locations()
    
    logger.info(f"  - Loaded {len(departments)} departments with synonyms")
    logger.info(f"  - Loaded {len(locations)} locations with synonyms")
    
    # Try inserting a test job
    test_job = {
        'title': 'Test Software Engineer',
        'company_name': 'Test Company',
        'department': 'Engineering',
        'location': 'Tel Aviv',
        'workplace_type': 'Hybrid',
        'experience_level': 'Senior',
        'employment_type': 'Full-time',
        'description': 'This is a test job posting',
        'url': 'https://example.com/jobs/test-123',
        'uid': 'TEST-123'
    }
    
    test_id = db.insert_job(test_job)
    if test_id:
        logger.info(f"  - Test job inserted with ID: {test_id}")
        
        # Try to insert duplicate (should fail gracefully)
        duplicate_id = db.insert_job(test_job)
        if duplicate_id is None:
            logger.info("  - Duplicate prevention working correctly")
        
        # Retrieve the job
        job = db.get_job_by_url(test_job['url'])
        logger.info(f"  - Test job retrieved: {job['title'] if job else 'None'}")
        logger.info(f"  - Department normalized to ID: {job['department_id'] if job else 'None'}")
        logger.info(f"  - Location normalized to ID: {job['location_id'] if job else 'None'}")
    else:
        logger.warning("  - Test job insertion failed (may already exist)")
    
    return True


def main():
    """Initialize both databases"""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 60)
    
    print("\nInitializing SQLite Databases...\n")
    
    # Initialize search_queries.db
    print("1. Initializing search_queries.db...")
    try:
        init_search_queries_db()
        print("   [OK] search_queries.db ready\n")
    except Exception as e:
        logger.error(f"Failed to initialize search_queries.db: {e}")
        print(f"   [FAILED] {e}\n")
        return False
    
    # Initialize companies.db
    print("2. Initializing companies.db...")
    try:
        init_companies_db()
        print("   [OK] companies.db ready\n")
    except Exception as e:
        logger.error(f"Failed to initialize companies.db: {e}")
        print(f"   [FAILED] {e}\n")
        return False
    
    # Initialize jobs.db
    print("3. Initializing jobs.db...")
    try:
        init_jobs_db()
        print("   [OK] jobs.db ready\n")
    except Exception as e:
        logger.error(f"Failed to initialize jobs.db: {e}")
        print(f"   [FAILED] {e}\n")
        return False
    
    print("=" * 60)
    print("[OK] All databases initialized successfully!")
    print("=" * 60)
    print(f"\nDatabase locations:")
    print(f"  - {SEARCH_QUERIES_DB}")
    print(f"  - {COMPANIES_DB}")
    print(f"  - {JOBS_DB}")
    print()
    
    logger.info("Database initialization complete")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
