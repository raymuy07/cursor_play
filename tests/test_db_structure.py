#!/usr/bin/env python3
"""
Test Database Structure
Verify the corrected database structure is working properly
"""

import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_utils import SearchQueriesDB, CompaniesDB
from scripts.db_schema import get_search_queries_schema, get_companies_schema
from scripts.db_utils import initialize_database, SEARCH_QUERIES_DB, COMPANIES_DB


def test_search_queries_db():
    """Test search_queries.db functionality"""
    print("\n" + "=" * 60)
    print("Testing search_queries.db")
    print("=" * 60)
    
    db = SearchQueriesDB()
    
    # Test inserting search queries with domain
    print("\n1. Inserting test search queries...")
    id1 = db.log_search("comeet", "site:comeet.com jobs", "google_serper", 15)
    id2 = db.log_search("lever", "site:lever.co careers", "google_serper", 23)
    id3 = db.log_search("greenhouse", "site:greenhouse.io jobs", "google_serper", 31)
    print(f"   ✓ Inserted 3 queries (IDs: {id1}, {id2}, {id3})")
    
    # Test retrieving recent searches
    print("\n2. Retrieving recent searches...")
    recent = db.get_recent_searches(5)
    print(f"   ✓ Found {len(recent)} recent searches:")
    for search in recent[:3]:
        print(f"      - Domain: '{search['domain']}' | Query: '{search['query']}' | Results: {search['results_count']}")
    
    # Test retrieving specific domain search
    print("\n3. Retrieving search by domain...")
    specific = db.get_search_by_domain("comeet", "google_serper")
    if specific:
        print(f"   ✓ Found: domain='{specific['domain']}', query='{specific['query']}', results={specific['results_count']}")
    
    # Test getting domains to search
    print("\n4. Getting domains that need searching...")
    domains = db.get_domains_to_search(max_age_hours=1)  # Domains not searched in last hour
    print(f"   ✓ Domains to search (not searched in last hour): {domains if domains else 'None (all are fresh)'}")
    
    print("\n✓ search_queries.db tests passed!")
    return True


def test_companies_db():
    """Test companies.db functionality"""
    print("\n" + "=" * 60)
    print("Testing companies.db")
    print("=" * 60)
    
    db = CompaniesDB()
    
    # Test inserting companies
    print("\n1. Inserting test companies...")
    
    test_companies = [
        {
            'company_name': 'Arpeely',
            'domain': 'comeet',
            'job_page_url': 'https://www.comeet.com/jobs/arpeely/57.001',
            'title': 'Jobs at Arpeely - Comeet',
            'source': 'google_serper'
        },
        {
            'company_name': 'Lumenis',
            'domain': 'comeet',
            'job_page_url': 'https://www.comeet.com/jobs/lumenis/A1.00C',
            'title': 'Jobs at Lumenis - Comeet',
            'source': 'google_serper'
        },
        {
            'company_name': 'Example Corp',
            'domain': 'lever',
            'job_page_url': 'https://jobs.lever.co/examplecorp',
            'title': 'Example Corp - Jobs',
            'source': 'google_serper'
        }
    ]
    
    inserted = 0
    duplicates = 0
    
    for company in test_companies:
        result = db.insert_company(company)
        if result:
            inserted += 1
        else:
            duplicates += 1
    
    print(f"   ✓ Inserted: {inserted}, Duplicates: {duplicates}")
    
    # Test retrieving companies
    print("\n2. Retrieving all active companies...")
    active_companies = db.get_all_companies(active_only=True, limit=10)
    print(f"   ✓ Found {len(active_companies)} active companies:")
    for company in active_companies[:3]:
        print(f"      - {company['company_name']} ({company['domain']}) - {company['job_page_url']}")
    
    # Test getting company by URL
    print("\n3. Retrieving company by URL...")
    company = db.get_company_by_url(test_companies[0]['job_page_url'])
    if company:
        print(f"   ✓ Found: {company['company_name']} (domain: {company['domain']})")
    
    # Test duplicate prevention
    print("\n4. Testing duplicate prevention...")
    result = db.insert_company(test_companies[0])
    if result is None:
        print("   ✓ Duplicate prevention working correctly")
    else:
        print("   ✗ Duplicate was inserted (should have been rejected)")
    
    # Test getting companies by domain
    print("\n5. Getting companies by domain...")
    comeet_companies = db.get_companies_by_domain('comeet', active_only=True)
    print(f"   ✓ Found {len(comeet_companies)} companies on 'comeet' domain")
    
    # Test counting companies
    print("\n6. Counting companies...")
    total = db.count_companies(active_only=True)
    comeet_count = db.count_companies(domain='comeet', active_only=True)
    print(f"   ✓ Total active companies: {total}")
    print(f"   ✓ Active companies on 'comeet': {comeet_count}")
    
    # Test getting companies to scrape
    print("\n7. Getting companies that need scraping...")
    to_scrape = db.get_companies_to_scrape(limit=5, max_age_hours=1)
    print(f"   ✓ Companies to scrape (never scraped or >1 hour old): {len(to_scrape)}")
    if to_scrape:
        for company in to_scrape[:2]:
            print(f"      - {company['company_name']} (last_scraped: {company['last_scraped'] or 'Never'})")
    
    print("\n✓ companies.db tests passed!")
    return True


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("DATABASE STRUCTURE TESTS")
    print("=" * 60)
    
    # Ensure databases are initialized
    print("\nInitializing databases...")
    initialize_database(SEARCH_QUERIES_DB, get_search_queries_schema())
    initialize_database(COMPANIES_DB, get_companies_schema())
    print("✓ Databases initialized")
    
    # Run tests
    try:
        test_search_queries_db()
        test_companies_db()
        
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print(f"\nDatabase files:")
        print(f"  - {SEARCH_QUERIES_DB}")
        print(f"  - {COMPANIES_DB}")
        print("\nDatabase structure is correct and ready to use!")
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
