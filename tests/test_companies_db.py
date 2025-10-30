#!/usr/bin/env python3
"""
Test script to verify companies database functionality
"""

from scripts.db_utils import CompaniesDB, initialize_database, COMPANIES_DB
from scripts.db_schema import get_companies_schema

def main():
    # Initialize database
    try:
        initialize_database(COMPANIES_DB, get_companies_schema())
        print("✓ Database initialized/verified")
    except Exception as e:
        print(f"✗ Database initialization failed: {e}")
        return
    
    # Test database connection
    db = CompaniesDB()
    
    # Get count
    count = db.count_companies()
    print(f"\n📊 Total companies in database: {count}")
    
    # Get sample companies
    if count > 0:
        print("\n📋 Sample companies (up to 5):")
        companies = db.get_all_companies(limit=5)
        for i, company in enumerate(companies, 1):
            print(f"  {i}. {company['company_name']} ({company['domain']})")
            print(f"     URL: {company['job_page_url']}")
            print(f"     Active: {company['is_active']}")
            print()
    else:
        print("\n⚠️  No companies in database yet. Run discover_companies.py to populate.")
    
    # Test domain-specific query
    if count > 0:
        comeet_count = db.count_companies(domain='comeet.com')
        print(f"📍 Comeet companies: {comeet_count}")

if __name__ == "__main__":
    main()

