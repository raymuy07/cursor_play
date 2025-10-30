#!/usr/bin/env python3
"""
Verify jobs.db database structure and contents
"""

import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.db_utils import JobsDB
from scripts.utils import setup_logging

def main():
    """Verify the jobs database"""
    logger = setup_logging()
    
    logger.info("Verifying jobs.db database...")
    
    jobs_db = JobsDB()
    verification = jobs_db.verify_database()
    
    print("\n" + "="*60)
    print("JOBS DATABASE VERIFICATION REPORT")
    print("="*60)
    
    print(f"\nTables found: {len(verification['tables_exist'])}")
    for table in verification['tables_exist']:
        print(f"  - {table}")
    
    print(f"\nStatistics:")
    print(f"  - Jobs count: {verification['jobs_count']}")
    print(f"  - Departments count: {verification['departments_count']}")
    print(f"  - Locations count: {verification['locations_count']}")
    
    if verification['sample_job']:
        print(f"\nSample job:")
        sample = verification['sample_job']
        print(f"  - ID: {sample.get('id')}")
        print(f"  - Title: {sample.get('title')}")
        print(f"  - Company: {sample.get('company_name')}")
        print(f"  - Department: {sample.get('department')} (ID: {sample.get('department_id')})")
        print(f"  - Location: {sample.get('location')} (ID: {sample.get('location_id')})")
        print(f"  - URL: {sample.get('url')}")
        print(f"  - Scraped at: {sample.get('scraped_at')}")
    else:
        print("\nNo jobs found in database.")
    
    if verification['errors']:
        print(f"\nErrors encountered:")
        for error in verification['errors']:
            print(f"  - {error}")
    else:
        print("\n[OK] No errors found!")
    
    print("\n" + "="*60)
    
    # Additional verification: check if jobs table has proper structure
    if 'jobs' in verification['tables_exist']:
        print("\nVerifying jobs table structure...")
        try:
            from scripts.db_utils import get_db_connection
            with get_db_connection(jobs_db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(jobs)")
                columns = cursor.fetchall()
                print(f"  Jobs table has {len(columns)} columns:")
                for col in columns[:10]:  # Show first 10 columns
                    print(f"    - {col[1]} ({col[2]})")
                if len(columns) > 10:
                    print(f"    ... and {len(columns) - 10} more columns")
        except Exception as e:
            print(f"  Error checking table structure: {e}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()

