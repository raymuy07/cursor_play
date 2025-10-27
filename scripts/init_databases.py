#!/usr/bin/env python3
"""
Database Initialization Script
Creates and initializes both search_queries.db and companies_pages.db
"""

import os
import sys
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_schema import get_search_queries_schema, get_jobs_schema
from scripts.db_utils import (
    initialize_database,
    SEARCH_QUERIES_DB,
    COMPANIES_PAGES_DB,
    SearchQueriesDB,
    JobsDB
)


def setup_simple_logging():
    """Setup basic logging without config.yaml dependency"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger('db_init')


def init_search_queries_db():
    """Initialize search_queries.db"""
    logger = setup_simple_logging()
    
    logger.info(f"Initializing search_queries.db at {SEARCH_QUERIES_DB}")
    
    schema = get_search_queries_schema()
    initialize_database(SEARCH_QUERIES_DB, schema)
    
    logger.info("✓ search_queries.db initialized successfully")
    
    # Test the database
    db = SearchQueriesDB()
    test_id = db.log_search("test_query", "test_source", 0)
    logger.info(f"  - Test record inserted with ID: {test_id}")
    
    recent = db.get_recent_searches(1)
    logger.info(f"  - Test record retrieved: {recent[0] if recent else 'None'}")
    
    return True


def init_companies_pages_db():
    """Initialize companies_pages.db (jobs table)"""
    logger = setup_simple_logging()
    
    logger.info(f"Initializing companies_pages.db at {COMPANIES_PAGES_DB}")
    
    schema = get_jobs_schema()
    initialize_database(COMPANIES_PAGES_DB, schema)
    
    logger.info("✓ companies_pages.db initialized successfully")
    
    # Test the database
    db = JobsDB()
    
    # Try inserting a test job
    test_job = {
        'url': 'https://test.com/job/123',
        'title': 'Test Software Engineer',
        'company': 'Test Company',
        'source': 'test',
        'description': 'Test job description',
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
    else:
        logger.warning("  - Test job insertion failed (may already exist)")
    
    return True


def main():
    """Initialize both databases"""
    logger = setup_simple_logging()
    
    logger.info("=" * 60)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 60)
    
    print("\n🗄️  Initializing SQLite Databases...\n")
    
    # Initialize search_queries.db
    print("1. Initializing search_queries.db...")
    try:
        init_search_queries_db()
        print("   ✓ search_queries.db ready\n")
    except Exception as e:
        logger.error(f"Failed to initialize search_queries.db: {e}")
        print(f"   ✗ Failed: {e}\n")
        return False
    
    # Initialize companies_pages.db
    print("2. Initializing companies_pages.db...")
    try:
        init_companies_pages_db()
        print("   ✓ companies_pages.db ready\n")
    except Exception as e:
        logger.error(f"Failed to initialize companies_pages.db: {e}")
        print(f"   ✗ Failed: {e}\n")
        return False
    
    print("=" * 60)
    print("✓ All databases initialized successfully!")
    print("=" * 60)
    print(f"\nDatabase locations:")
    print(f"  - {SEARCH_QUERIES_DB}")
    print(f"  - {COMPANIES_PAGES_DB}")
    print()
    
    logger.info("Database initialization complete")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
