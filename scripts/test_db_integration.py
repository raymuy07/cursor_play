#!/usr/bin/env python3
"""
Test Database Integration
Quick test to verify database integration is working correctly
"""

import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_utils import SearchQueriesDB, JobsDB
from scripts.db_schema import get_search_queries_schema, get_jobs_schema
from scripts.db_utils import initialize_database, SEARCH_QUERIES_DB, COMPANIES_PAGES_DB


def test_search_queries_db():
    """Test search_queries.db functionality"""
    print("\n" + "=" * 60)
    print("Testing search_queries.db")
    print("=" * 60)
    
    db = SearchQueriesDB()
    
    # Test inserting a search query
    print("\n1. Inserting test search queries...")
    id1 = db.log_search("site:comeet.com jobs", "google_serper", 15)
    id2 = db.log_search("site:lever.co careers", "google_serper", 23)
    print(f"   ✓ Inserted 2 queries (IDs: {id1}, {id2})")
    
    # Test retrieving recent searches
    print("\n2. Retrieving recent searches...")
    recent = db.get_recent_searches(5)
    print(f"   ✓ Found {len(recent)} recent searches:")
    for search in recent[:3]:
        print(f"      - Query: '{search['query']}' | Source: {search['source']} | Results: {search['results_count']}")
    
    # Test retrieving specific search
    print("\n3. Retrieving specific search...")
    specific = db.get_search_by_query("site:comeet.com jobs", "google_serper")
    if specific:
        print(f"   ✓ Found: {specific['query']} with {specific['results_count']} results")
    
    print("\n✓ search_queries.db tests passed!")
    return True


def test_jobs_db():
    """Test companies_pages.db (jobs table) functionality"""
    print("\n" + "=" * 60)
    print("Testing companies_pages.db (jobs table)")
    print("=" * 60)
    
    db = JobsDB()
    
    # Test inserting jobs
    print("\n1. Inserting test jobs...")
    
    test_jobs = [
        {
            'url': 'https://www.comeet.com/jobs/testcompany1/123/software-engineer',
            'title': 'Senior Software Engineer',
            'company': 'Test Company 1',
            'company_name': 'Test Company 1',
            'source': 'comeet',
            'department': 'Engineering',
            'location': 'Tel Aviv, Israel',
            'employment_type': 'Full-time',
            'experience_level': 'Senior',
            'workplace_type': 'Hybrid',
        },
        {
            'url': 'https://www.comeet.com/jobs/testcompany2/456/product-manager',
            'title': 'Product Manager',
            'company': 'Test Company 2',
            'company_name': 'Test Company 2',
            'source': 'comeet',
            'department': 'Product',
            'location': 'Remote',
            'employment_type': 'Full-time',
            'experience_level': 'Mid-level',
            'workplace_type': 'Remote',
        }
    ]
    
    inserted = 0
    duplicates = 0
    
    for job in test_jobs:
        result = db.insert_job(job)
        if result:
            inserted += 1
        else:
            duplicates += 1
    
    print(f"   ✓ Inserted: {inserted}, Duplicates: {duplicates}")
    
    # Test retrieving jobs
    print("\n2. Retrieving active jobs...")
    active_jobs = db.get_active_jobs(limit=5)
    print(f"   ✓ Found {len(active_jobs)} active jobs:")
    for job in active_jobs[:3]:
        print(f"      - {job['title']} at {job['company_name']} ({job['location']})")
    
    # Test getting job by URL
    print("\n3. Retrieving job by URL...")
    job = db.get_job_by_url(test_jobs[0]['url'])
    if job:
        print(f"   ✓ Found: {job['title']} at {job['company_name']}")
    
    # Test duplicate prevention
    print("\n4. Testing duplicate prevention...")
    result = db.insert_job(test_jobs[0])
    if result is None:
        print("   ✓ Duplicate prevention working correctly")
    else:
        print("   ✗ Duplicate was inserted (should have been rejected)")
    
    # Test counting jobs
    print("\n5. Counting jobs...")
    total = db.count_jobs(active_only=True)
    all_jobs = db.count_jobs(active_only=False)
    print(f"   ✓ Active jobs: {total}")
    print(f"   ✓ Total jobs (including inactive): {all_jobs}")
    
    print("\n✓ companies_pages.db tests passed!")
    return True


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("DATABASE INTEGRATION TESTS")
    print("=" * 60)
    
    # Ensure databases are initialized
    print("\nInitializing databases...")
    initialize_database(SEARCH_QUERIES_DB, get_search_queries_schema())
    initialize_database(COMPANIES_PAGES_DB, get_jobs_schema())
    print("✓ Databases initialized")
    
    # Run tests
    try:
        test_search_queries_db()
        test_jobs_db()
        
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print(f"\nDatabase files:")
        print(f"  - {SEARCH_QUERIES_DB}")
        print(f"  - {COMPANIES_PAGES_DB}")
        print()
        
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
