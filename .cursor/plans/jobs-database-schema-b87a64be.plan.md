<!-- b87a64be-3fca-4feb-86f8-bffd02feb210 182b7b52-be65-4396-ae34-7e469fdbfdd9 -->
# Jobs Database Schema Plan

## Overview

Create a well-structured jobs.db with proper normalization, canonical reference tables, and optimized indexing for common filter operations.

## Database Structure

### Core Jobs Table

- **id**: PRIMARY KEY (needed for future user table foreign keys)
- **title**: TEXT, NOT NULL
- **department**: TEXT (raw text from scraping)
- **department_id**: INTEGER, FOREIGN KEY to departments table
- **location**: TEXT (raw text from scraping)
- **location_id**: INTEGER, FOREIGN KEY to locations table
- **scraped_at**: TIMESTAMP (when we scraped this job) - **INDEXED**
- **publish_date**: TIMESTAMP (job posting date/last_updated) - **INDEXED**
- **uid**: TEXT (source's unique identifier)
- **company_id**: INTEGER, FOREIGN KEY to companies table - **INDEXED**
- **company_name_raw**: TEXT (raw text from scraping, backup for debugging)
- **workplace_type**: TEXT (On-site/Hybrid/Remote) - **INDEXED**
- **experience_level**: TEXT (Intern/Junior/Senior/etc) - **INDEXED**
- **employment_type**: TEXT (Full-time/Part-time/Contract) - **INDEXED**, default 'NULL'
- **description**: TEXT (joined dict values for embedding)
- **url**: TEXT, UNIQUE, NOT NULL
- **url_hash**: TEXT, UNIQUE - **INDEXED**
- **email**: TEXT (for future use)
- **is_ai_inferred**: BOOLEAN (flag if AI filled missing fields)
- from_domain: TEXT

### Companies Reference Tables

**companies**: Canonical company names

- id, canonical_name

**company_synonyms**: Maps variations to canonical

- id, synonym, company_id (FK)
- Examples: "Microsoft Corp" → Microsoft, "MSFT" → Microsoft

### Departments Reference Tables

**departments**: Canonical department names

- id, canonical_name

**department_synonyms**: Maps variations to canonical

- id, synonym, department_id (FK)
- Examples: "R&D" → Engineering, "Engineering" → Engineering

### Locations Reference Tables

**locations**: Canonical location names

- id, canonical_name, country, region

**location_synonyms**: Maps variations to canonical

- id, synonym, location_id (FK)
- Examples: "TLV", "Tel Aviv", "Tel Aviv-Yafo" → "Tel Aviv"

## Implementation Steps

1. **Create schema file** at `.cursor/schemas/jobs_schema.sql`

- Define all tables with proper constraints
- Add all necessary indexes (including idx_company, idx_publish_date, idx_url_hash, idx_scraped_at)
- Include sample data for common companies, departments and locations

2. **Update `scripts/db_schema.py`**

- Add JOBS_SCHEMA constant
- Add helper functions: `get_jobs_schema()`, `normalize_company()`, `normalize_department()`, `normalize_location()`

3. **Create utility functions** in `scripts/db_utils.py`

- `normalize_company(raw_company)`: Maps raw text → company_id
- `normalize_department(raw_dept)`: Maps raw text → department_id
- `normalize_location(raw_loc)`: Maps raw text → location_id
- `get_or_create_company(synonym)`: Auto-add new companies
- `get_or_create_department(synonym)`: Auto-add new departments
- `get_or_create_location(synonym)`: Auto-add new locations

4. **Update `scripts/init_databases.py`**

- Add jobs.db initialization
- Populate initial company, department and location reference data

## Key Design Decisions

- Keep both raw text and normalized IDs for flexibility
- URL remains unique identifier (url_hash indexed for optimized lookups)
- Description stored as plain TEXT (join dict values with newlines)
- All filter fields indexed: workplace_type, employment_type, experience_level, department_id, location_id, company_id
- Date fields indexed: publish_date (DESC), scraped_at (DESC) for sorting and freshness queries
- Hybrid normalization: function-based mapping rather than triggers
- Companies normalized like departments/locations for consistent filtering and autocomplete

### To-dos

- [ ] Create .cursor/schemas/ folder and jobs_schema.sql file with complete DDL
- [ ] Add JOBS_SCHEMA constant and getter functions to scripts/db_schema.py
- [ ] Add department/location normalization functions to scripts/db_utils.py
- [x] Create scripts/migrate_jobs_to_db.py to populate from jobs_raw.json
- [ ] Update scripts/init_databases.py to initialize jobs.db with reference data