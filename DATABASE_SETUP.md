# SQLite Database Setup - Phase 1

## Overview
Phase 1 establishes the foundation for tracking search queries and company job pages (not individual jobs yet).

## Date
October 27, 2025

## Branch
`feature/sqlite-migration-step-by-step`

---

## Databases Created

### 1. search_queries.db
**Purpose**: Track which domains we've searched and when

**Location**: `/workspace/data/search_queries.db`

**Schema**:
```sql
CREATE TABLE search_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,        -- 'comeet', 'lever', 'greenhouse', etc.
    query TEXT NOT NULL,          -- The actual search query
    source TEXT NOT NULL,         -- 'google_serper', 'indeed', etc.
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    results_count INTEGER DEFAULT 0
);
```

**Why domain field?**: 
- Makes it easy to query: "Which domains haven't been searched lately?"
- More convenient than using long query strings
- Each domain type has its own query pattern

**Example Flow**:
1. Check which domain hasn't been searched recently
2. Use that domain to construct the appropriate search query
3. Run Serper API with the query
4. Log the search with domain, query, source, and result count

### 2. companies.db
**Purpose**: Store company job pages discovered from searches

**Location**: `/workspace/data/companies.db`

**Schema**:
```sql
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,          -- 'comeet', 'lever', etc.
    job_page_url TEXT UNIQUE NOT NULL,  -- The company's main job listings page
    title TEXT,                    -- Page title from search results
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP,        -- When we last scraped jobs from this page
    is_active BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'google_serper'
);
```

**Important**: 
- Each company has ONE job page URL
- That URL contains MANY individual jobs
- We're NOT storing individual jobs yet - just the company pages!

**Example Data**:
- "https://www.comeet.com/jobs/arpeely/57.001" → Arpeely's job page (contains ~10 jobs)
- "https://www.comeet.com/jobs/lumenis/A1.00C" → Lumenis' job page (contains ~15 jobs)

---

## New Files Created

### Core Database Modules
1. **`scripts/db_schema.py`** - Schema definitions for both databases
2. **`scripts/db_utils.py`** - Database utilities and connection management
3. **`scripts/init_databases.py`** - Database initialization script
4. **`scripts/test_db_structure.py`** - Structure and integration tests

### Key Classes

#### SearchQueriesDB
Interface for search_queries.db operations:
```python
db = SearchQueriesDB()

# Log a search
db.log_search("comeet", "site:comeet.com jobs", "google_serper", 15)

# Get recent searches
recent = db.get_recent_searches(limit=10)

# Get last search for a domain
search = db.get_search_by_domain("comeet")

# Get domains that need searching
domains = db.get_domains_to_search(max_age_hours=24)
```

#### CompaniesDB  
Interface for companies.db operations:
```python
db = CompaniesDB()

# Insert a company
company_data = {
    'company_name': 'Arpeely',
    'domain': 'comeet',
    'job_page_url': 'https://www.comeet.com/jobs/arpeely/57.001',
    'title': 'Jobs at Arpeely - Comeet',
    'source': 'google_serper'
}
company_id = db.insert_company(company_data)

# Get companies by domain
comeet_companies = db.get_companies_by_domain('comeet')

# Get companies that need scraping
to_scrape = db.get_companies_to_scrape(limit=10, max_age_hours=168)  # 7 days

# Update when scraped
db.update_last_scraped('https://www.comeet.com/jobs/arpeely/57.001')

# Count companies
total = db.count_companies(domain='comeet', active_only=True)
```

---

## Setup and Testing

### Initialize Databases
```bash
python3 scripts/init_databases.py
```

Output:
```
🗄️  Initializing SQLite Databases...

1. Initializing search_queries.db...
   ✓ search_queries.db ready

2. Initializing companies.db...
   ✓ companies.db ready

============================================================
✓ All databases initialized successfully!
============================================================
```

### Run Tests
```bash
python3 scripts/test_db_structure.py
```

All tests should pass, verifying:
- Database creation
- Insert operations
- Duplicate prevention
- Query operations
- Domain filtering
- Timestamp tracking

---

## Flow Example

### Step 1: Check Which Domain to Search
```python
from scripts.db_utils import SearchQueriesDB

search_db = SearchQueriesDB()

# Get domains not searched in last 24 hours
domains_to_search = search_db.get_domains_to_search(max_age_hours=24)
# Returns: ['comeet', 'lever', 'greenhouse'] (if they're due)

# Or check specific domain
last_search = search_db.get_search_by_domain('comeet')
if last_search:
    print(f"Comeet last searched at: {last_search['searched_at']}")
```

### Step 2: Run Search via Serper API
```python
# Construct query for domain
domain = 'comeet'
query = f"site:{domain}.com jobs"

# Call Serper API (your existing code)
results = serper_api_search(query)

# Log the search
search_db.log_search(
    domain='comeet',
    query=query,
    source='google_serper',
    results_count=len(results)
)
```

### Step 3: Store Discovered Companies
```python
from scripts.db_utils import CompaniesDB

companies_db = CompaniesDB()

# For each result from Serper
for result in results:
    company_data = {
        'company_name': extract_company_name(result['title']),
        'domain': 'comeet',
        'job_page_url': result['link'],
        'title': result['title'],
        'source': 'google_serper'
    }
    
    company_id = companies_db.insert_company(company_data)
    if company_id:
        print(f"New company added: {company_data['company_name']}")
    else:
        print(f"Company already exists: {company_data['company_name']}")
```

### Step 4: Get Companies to Scrape Jobs From
```python
# Get companies that haven't been scraped in 7 days
companies_to_scrape = companies_db.get_companies_to_scrape(
    limit=20,
    max_age_hours=168  # 7 days
)

for company in companies_to_scrape:
    print(f"Scrape: {company['company_name']} at {company['job_page_url']}")
    
    # After scraping the jobs from this company page
    companies_db.update_last_scraped(company['job_page_url'])
```

---

## Benefits

1. **Smart Search Scheduling**: Track which domains need searching
2. **No Duplicate Companies**: UNIQUE constraint on job_page_url
3. **Scraping Management**: Know which companies need job extraction
4. **Audit Trail**: Every search is logged with timestamp
5. **Performance**: Indexes on all key fields
6. **Flexibility**: Easy to add new domains

---

## SQL Query Examples

### View Recent Searches
```sql
sqlite3 data/search_queries.db \
  "SELECT domain, query, results_count, searched_at 
   FROM search_queries 
   ORDER BY searched_at DESC 
   LIMIT 10"
```

### Find Domains Not Searched Recently
```sql
sqlite3 data/search_queries.db \
  "SELECT DISTINCT domain 
   FROM search_queries 
   WHERE searched_at < datetime('now', '-24 hours')"
```

### View All Companies
```sql
sqlite3 data/companies.db \
  "SELECT company_name, domain, discovered_at, last_scraped 
   FROM companies 
   WHERE is_active=1 
   ORDER BY discovered_at DESC"
```

### Count Companies by Domain
```sql
sqlite3 data/companies.db \
  "SELECT domain, COUNT(*) as count 
   FROM companies 
   WHERE is_active=1 
   GROUP BY domain"
```

### Companies That Need Scraping
```sql
sqlite3 data/companies.db \
  "SELECT company_name, domain, job_page_url, last_scraped
   FROM companies 
   WHERE is_active=1 
   AND (last_scraped IS NULL OR last_scraped < datetime('now', '-7 days'))
   ORDER BY last_scraped ASC NULLS FIRST
   LIMIT 20"
```

---

## What's NOT Implemented Yet

This is Phase 1 only. We are NOT yet:
- ❌ Integrating with `discover_companies.py` automatically
- ❌ Integrating with `scrape_jobs.py` automatically  
- ❌ Storing individual job listings (that's Phase 2)
- ❌ Removing JSON file dependencies

## What IS Working

- ✅ Database schemas defined
- ✅ Database utility classes created
- ✅ Initialization scripts ready
- ✅ All tests passing
- ✅ Ready to integrate into workflow (next phase)

---

## Next Steps (Future Phases)

**Phase 2**: Integrate search_queries.db into discover_companies.py
- Automatically log all searches
- Use domain tracking to schedule searches

**Phase 3**: Integrate companies.db into discover_companies.py
- Store discovered companies in DB
- Update discover flow to use DB instead of JSON

**Phase 4**: Create jobs.db for individual job listings
- Store individual jobs scraped from company pages
- Link jobs to companies

**Phase 5**: Full migration away from JSON files

---

## Files Layout
```
/workspace/
├── data/
│   ├── search_queries.db      # NEW: Search tracking
│   ├── companies.db           # NEW: Company job pages
│   ├── companies.json         # Existing: Still in use
│   ├── jobs_raw.json         # Existing: Still in use
│   └── jobs_filtered.json    # Existing: Still in use
├── scripts/
│   ├── db_schema.py          # NEW: Database schemas
│   ├── db_utils.py           # NEW: Database utilities (397 lines)
│   ├── init_databases.py     # NEW: Initialization script
│   ├── test_db_structure.py  # NEW: Tests
│   ├── discover_companies.py # Unchanged (for now)
│   ├── scrape_jobs.py        # Unchanged (for now)
│   ├── filter_jobs.py        # Unchanged
│   └── utils.py              # Unchanged
```

---

## Summary

✅ **Phase 1 Complete**: Database structure established and tested
- `search_queries.db` tracks domain searches with timestamps
- `companies.db` stores company job pages (not individual jobs)
- Duplicate prevention via UNIQUE constraints
- Comprehensive utility classes for database operations
- Full test coverage

Ready for Phase 2: Integration into existing scripts! 🚀
