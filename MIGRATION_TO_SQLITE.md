# Migration to SQLite - Companies Database

## Overview
Successfully migrated from `companies.json` to SQLite database (`companies.db`) for storing company information.

## Changes Made

### 1. Updated `discover_companies.py`
- **Removed**: JSON file reading/writing logic
- **Removed**: `deduplicate_companies()` function call (handled by DB UNIQUE constraint)
- **Removed**: `load_existing_companies()` function
- **Added**: `CompaniesDB` initialization and usage
- **Modified**: `search_domain_jobs()` now returns count of new companies instead of list
- **Modified**: `process_search_results()` now inserts directly into database and returns count
- **Improved**: Better logging of discovered companies (new vs existing)

### 2. Database Integration
- Companies are now inserted directly into SQLite as they're discovered
- Duplicate detection is automatic via UNIQUE constraint on `job_page_url`
- The `discover_companies()` function now returns all companies from the database
- No data migration needed - old `companies.json` data can be easily re-discovered

### 3. Benefits
- **Data integrity**: UNIQUE constraints prevent duplicates at database level
- **Better tracking**: Timestamps for `discovered_at` and `last_scraped`
- **Status management**: `is_active` flag for companies
- **Query flexibility**: Can easily query by domain, active status, etc.
- **Scalability**: Better performance for large datasets
- **Concurrent access**: SQLite handles multiple readers safely

## Database Schema

```sql
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,
    job_page_url TEXT UNIQUE NOT NULL,
    title TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'google_serper'
);
```

## Usage Examples

### Query Companies
```python
from scripts.db_utils import CompaniesDB

db = CompaniesDB()

# Get all companies
all_companies = db.get_all_companies()

# Get companies by domain
comeet_companies = db.get_companies_by_domain('comeet.com')

# Get companies that need scraping
to_scrape = db.get_companies_to_scrape(limit=10)

# Count companies
total = db.count_companies()
active_comeet = db.count_companies(domain='comeet.com', active_only=True)
```

### Discover Companies
```python
from scripts.discover_companies import discover_companies

# Run discovery - automatically saves to database
companies = discover_companies()
print(f"Total companies: {len(companies)}")
```

## Testing
Run the test script to verify database functionality:
```bash
python scripts/test_companies_db.py
```

## Migration Notes
- ✅ `companies.json` is no longer used by the system
- ✅ No data migration performed (data can be re-discovered easily)
- ✅ All new discoveries automatically go to SQLite
- ⚠️ Old `companies.json` file can be archived or deleted

## Next Steps
Consider similar migrations for:
- Job data (currently using JSON files)
- Search queries (already migrated to SQLite)

