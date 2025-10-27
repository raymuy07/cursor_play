# Database Quick Start Guide

## Setup

### Initialize Databases (First Time Only)
```bash
python3 scripts/init_databases.py
```

This creates:
- `/workspace/data/search_queries.db` - Search tracking
- `/workspace/data/companies_pages.db` - Jobs storage

## Using the Databases

### In Python Scripts

```python
from scripts.db_utils import SearchQueriesDB, JobsDB

# Search Queries Database
search_db = SearchQueriesDB()

# Log a search
search_db.log_search("site:comeet.com jobs", "google_serper", 15)

# Get recent searches
recent = search_db.get_recent_searches(limit=10)
for search in recent:
    print(f"{search['query']} - {search['results_count']} results")

# Jobs Database  
jobs_db = JobsDB()

# Insert a job
job_data = {
    'url': 'https://example.com/job/123',
    'title': 'Software Engineer',
    'company': 'Example Corp',
    'source': 'comeet',
    'department': 'Engineering',
    'location': 'Tel Aviv, Israel',
}
job_id = jobs_db.insert_job(job_data)  # Returns None if duplicate

# Get active jobs
active_jobs = jobs_db.get_active_jobs(limit=50)

# Get job by URL
job = jobs_db.get_job_by_url('https://example.com/job/123')

# Count jobs
total = jobs_db.count_jobs(active_only=True)
```

## Direct SQL Queries

### View Search History
```bash
sqlite3 data/search_queries.db "SELECT * FROM search_queries ORDER BY searched_at DESC LIMIT 10"
```

### View Recent Jobs
```bash
sqlite3 data/companies_pages.db "SELECT title, company_name, location FROM jobs WHERE is_active=1 ORDER BY fetched_at DESC LIMIT 10"
```

### Count Jobs by Company
```bash
sqlite3 data/companies_pages.db "SELECT company_name, COUNT(*) as job_count FROM jobs WHERE is_active=1 GROUP BY company_name ORDER BY job_count DESC"
```

### Find Jobs by Department
```bash
sqlite3 data/companies_pages.db "SELECT title, company_name FROM jobs WHERE department LIKE '%Engineering%' AND is_active=1"
```

## Testing

Run integration tests:
```bash
python3 scripts/test_db_integration.py
```

## Current Workflow

1. **Discover Companies** (logs searches to DB)
   ```bash
   python3 scripts/discover_companies.py
   ```

2. **Scrape Jobs** (saves to DB + JSON backup)
   ```bash
   python3 scripts/scrape_jobs.py
   ```

3. **Filter Jobs** (still uses JSON)
   ```bash
   python3 scripts/filter_jobs.py
   ```

## Database Schema

### search_queries table
- `id` - Auto-incrementing primary key
- `query` - Search query string
- `source` - Search source (e.g., 'google_serper')
- `searched_at` - Timestamp
- `results_count` - Number of results

### jobs table
- `id` - Auto-incrementing primary key
- `job_hash` - MD5 hash (UNIQUE)
- `url` - Job URL (UNIQUE)
- `title` - Job title
- `company` / `company_name` - Company name
- `description` - Job description
- `fetched_at` - When first discovered
- `last_checked` - When last verified
- `is_active` - Boolean flag
- `source` - Data source
- `department` - Job department
- `location` - Job location
- `employment_type` - Full-time, Part-time, etc.
- `experience_level` - Junior, Senior, etc.
- `workplace_type` - Remote, Hybrid, On-site
- `uid` - External ID
- `last_updated` - Last update timestamp

## Tips

1. **Duplicate Prevention**: Jobs are automatically deduplicated by URL and job_hash
2. **Backup**: JSON files are still created as backup during migration period
3. **Performance**: All critical fields have indexes for fast queries
4. **Error Handling**: Database operations fail gracefully with warnings

## Useful Queries

### Jobs Added Today
```sql
SELECT COUNT(*) FROM jobs 
WHERE DATE(fetched_at) = DATE('now');
```

### Companies with Most Jobs
```sql
SELECT company_name, COUNT(*) as jobs 
FROM jobs 
WHERE is_active=1 
GROUP BY company_name 
ORDER BY jobs DESC 
LIMIT 10;
```

### Search Activity by Source
```sql
SELECT source, COUNT(*) as searches, SUM(results_count) as total_results 
FROM search_queries 
GROUP BY source;
```

### Remote Jobs
```sql
SELECT title, company_name, location 
FROM jobs 
WHERE (workplace_type LIKE '%Remote%' OR location LIKE '%Remote%') 
AND is_active=1;
```
