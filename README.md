# Job Hunter

A small, personal project to discover, scrape and score job listings for small companies hosted on Comeet (and similar providers). The pipeline is implemented in **Python** with **SQLite** for data storage and orchestrated with **cron**. This README documents goals, architecture, installation, usage, data formats, and operational notes.

**Current Status**: Phase 1 complete - Company discovery migrated to SQLite. Job storage (Phase 2) coming next.

---

## Project goals

- Discover company job pages using a Google dork query (example: `site:comeet.com intitle:"jobs at" intext:"tel aviv"`).
- Maintain a deduplicated list of company job-page URLs.
- Scrape job postings from those pages and store structured job metadata.
- Score jobs automatically against your skills and preferences using an AI model.
- Deliver a daily digest of matched jobs.
- Be polite to target sites (rate-limiting, caching, scheduling).

---

## High-level architecture

The project is split into three modules:

1. **Source discovery** (`discover_companies.py`)
   - Run Google searches using Serper API to gather company job page links.
   - Normalize and deduplicate company job page URLs.
   - **Store results in SQLite** (`data/companies.db`) with automatic duplicate prevention.
   - Track search queries in `data/search_queries.db` to avoid redundant searches.

2. **Job extraction** (`scrape_jobs.py`)
   - For each company page, fetch the page and extract job metadata.
   - Use lightweight scraping (requests + BeautifulSoup) for parsing.
   - Currently saves to JSON (`data/jobs_raw.json` and `data/jobs_filtered.json`).
   - **Phase 2**: Will migrate to SQLite jobs database.

3. **Scoring & delivery** (`score_jobs.py`, `send_digest.py`) [Planned]
   - Apply a scoring function (AI + rules) that ranks jobs according to your skills and preferences.
   - Keep a job history / seen-set to avoid duplicates in daily digests.
   - Send daily digest via Telegram, email, or another channel.

Scheduling and orchestration are handled by `cron` jobs.

### Data Storage Architecture

**Current (Hybrid)**:
- **SQLite databases** (Phase 1 ✅):
  - `data/companies.db` - Company job pages and metadata
  - `data/search_queries.db` - Search history and tracking
- **JSON files** (Phase 2 migration pending):
  - `data/jobs_raw.json` - Raw job listings
  - `data/jobs_filtered.json` - Filtered jobs

---

## Directory structure

```
job-hunter/
├── README.md
├── data/
│   ├── companies.db            # SQLite: company job pages (Phase 1 ✅)
│   ├── search_queries.db       # SQLite: search history (Phase 1 ✅)
│   ├── jobs_raw.json           # raw scrape outputs (Phase 2: migrate to SQLite)
│   └── jobs_filtered.json      # normalized & filtered jobs
├── scripts/
│   ├── discover_companies.py   # Company discovery (SQLite integrated ✅)
│   ├── scrape_jobs.py          # Job scraping (currently JSON)
│   ├── filter_jobs.py          # Job filtering
│   ├── db_schema.py            # Database schemas
│   ├── db_utils.py             # Database utilities (CompaniesDB, SearchQueriesDB)
│   ├── init_databases.py       # Database initialization
│   ├── utils.py                # General helpers
│   └── test_*.py               # Test scripts
├── tests/
│   └── fixtures/               # Test HTML files
├── requirements.txt
├── config.yaml                 # API keys, scraping rules (not in repo)
└── logs/
    └── jobhunter.log
```

---

## Prerequisites

- Python 3.10+ (recommended)
- SQLite3 (included with Python)
- pip packages from `requirements.txt`

### Installation

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd job-hunter
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file with your API keys:
   ```bash
   SERPER_API_KEY=your_serper_key_here
   OPENAI_API_KEY=your_openai_key_here
   TELEGRAM_BOT_TOKEN=your_telegram_token_here
   TELEGRAM_CHAT_ID=your_chat_id_here
   ```

4. **Initialize databases**
   ```bash
   python scripts/init_databases.py
   ```
   
   This creates:
   - `data/companies.db` - Company pages database
   - `data/search_queries.db` - Search tracking database

5. **Verify setup**
   ```bash
   python scripts/test_companies_db.py
   ```

---

## Configuration

Use `config.yaml` or environment variables for secrets and operational parameters. Example `config.yaml`:

```yaml
google_dork: 'site:comeet.com intitle:"jobs at" intext:"tel aviv"'
data_dir: './data'
user_agent: 'Mozilla/5.0 (compatible; JobHunter/1.0; +https://your.site)'
playwright_headless: true
max_concurrent_requests: 3
request_delay_seconds: 2
ai:
  provider: 'openai'
  api_key_env: 'OPENAI_API_KEY'
scoring:
  min_score: 0.6
delivery:
  method: 'telegram'
  telegram_token_env: 'TG_BOT_TOKEN'
  telegram_chat_id: '12345678'
```

Keep API keys in environment variables or a `.env` file and never commit them.

---

## Data formats

### SQLite Databases (Phase 1 ✅)

#### `companies.db`
Stores company job pages discovered from searches.

**Schema**:
```sql
CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    domain TEXT NOT NULL,              -- 'comeet', 'lever', etc.
    job_page_url TEXT UNIQUE NOT NULL, -- Main job listings page
    title TEXT,                        -- Page title from search
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped TIMESTAMP,            -- When jobs were last scraped
    is_active BOOLEAN DEFAULT 1,
    source TEXT DEFAULT 'google_serper'
);
```

**Usage**:
```python
from scripts.db_utils import CompaniesDB

db = CompaniesDB()
companies = db.get_all_companies()
to_scrape = db.get_companies_to_scrape(limit=10)
```

#### `search_queries.db`
Tracks search queries to avoid redundant API calls.

**Schema**:
```sql
CREATE TABLE search_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,              -- 'comeet', 'lever', etc.
    query TEXT NOT NULL,               -- The search query
    source TEXT NOT NULL,              -- 'google_serper'
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    results_count INTEGER DEFAULT 0
);
```

**Usage**:
```python
from scripts.db_utils import SearchQueriesDB

db = SearchQueriesDB()
db.log_search("comeet", "site:comeet.com jobs", "google_serper", 15)
domains_to_search = db.get_domains_to_search(max_age_hours=24)
```

### JSON Files (Phase 2 - Will migrate to SQLite)

#### `jobs_raw.json`
Raw job entries as extracted from company pages:

```json
{
  "title": "Backend Engineer",
  "company_name": "Company Name",
  "department": "Engineering",
  "location": "Tel Aviv, IL",
  "url": "https://...",
  "url_hash": "abc123..."
}
```

#### `jobs_filtered.json`
Filtered and normalized job entries:

```json
{
  "job_id": "comeet-12345",
  "company": "Company 1",
  "title": "Software Engineer, R&D",
  "location": "Tel Aviv",
  "department": "R&D",
  "description": "...",
  "apply_url": "https://...",
  "tags": ["python","backend"],
  "score": 0.0
}
```

Use a stable `job_id` field (if the source provides one) or compute a deterministic hash from (company + title + apply_url) to identify duplicates.

---

## Scraping best practices (be polite)

- Respect robots.txt and target servers. If the provider blocks scraping, stop.
- Rate-limit requests (`request_delay_seconds`), and limit concurrency (`max_concurrent_requests`).
- Use caching / conditional GET (ETag / If-Modified-Since) to avoid re-downloading unchanged pages.
- Use a descriptive User-Agent that includes contact info (or project name).
- Rotate request intervals (randomized jitter) so your traffic looks less uniform.
- Keep scraping frequency low (weekly discovery + daily scoring is plenty).

---

## Scheduling with cron (examples)

Edit your crontab with `crontab -e` and add entries like:

```cron
# Weekly discovery & scrape (run Sundays at 03:00)
0 3 * * 0 /usr/bin/python3 /path/to/job-hunter/scripts/discover_companies.py >> /path/to/job-hunter/logs/discover.log 2>&1
0 4 * * 0 /usr/bin/python3 /path/to/job-hunter/scripts/scrape_jobs.py >> /path/to/job-hunter/logs/scrape.log 2>&1

# Daily scoring & digest (run every weekday at 19:00)
0 19 * * 1-5 /usr/bin/python3 /path/to/job-hunter/scripts/score_jobs.py >> /path/to/job-hunter/logs/score.log 2>&1
0 19 * * 1-5 /usr/bin/python3 /path/to/job-hunter/scripts/send_digest.py >> /path/to/job-hunter/logs/digest.log 2>&1
```

If you prefer more control with Python, consider APScheduler inside a long-running service.

---

## Scoring approach (AI + rules)

Design the scoring as a combination of deterministic filters and an AI-based relevance score.

1. **Hard filters** (fast, deterministic):
   - Location must match (e.g., Tel Aviv).
   - Department must be in a whitelist (R&D, Engineering, SW).
   - Exclude internships, non-R&D categories, remote-only if you want on-site.

2. **AI-assisted relevance**:
   - Build a compact prompt that describes your skills, seniority, and preferences.
   - Ask the model to score the job from 0–1 and return a short justification and key matched terms.

**Example prompt (shortened):**
```
You are an assistant that scores job ads for a candidate. Candidate skills: Python, backend, distributed systems, CI/CD. Candidate prefers: R&D roles in Tel Aviv, senior-level. Rate the match from 0.0 to 1.0 and return JSON: {"score": float, "highlights": [str]}

Job title: ...
Description: ...
```

Store the numeric score and the highlights. Use a `min_score` threshold to decide which jobs make the digest.

Notes:
- Keep prompts deterministic and short. Cache model calls (don't score the same job repeatedly if unchanged).
- If using OpenAI or similar, watch usage costs and batch candidates/jobs where possible.

---

## Deliveries & notifications

Options for delivery:
- Telegram bot (simple, quick)
- Email (SMTP)
- Slack webhook

Make the delivery template compact. Example digest structure:

```
Daily matched jobs (3)
1) [Company] Title — score 0.82
   Location: Tel Aviv
   URL: https://...
   Highlights: Python, distributed systems

(See ./data/jobs_filtered.json for full details)
```

---

## Resilience, logging & state

- Keep a `seen_jobs` data store (SQLite or JSON) containing job_id and first-seen timestamp. Use it to avoid repeating notifications.
- Log important events/errors to `logs/` with rotating file handler.
- Add retry/backoff for transient HTTP failures.

---

## Development tips

- Start with a small subset of company URLs to iterate quickly.
- Write unit tests for normalization (URL cleaning, job_id hashing) and scoring glue code.
- Use `python -m http.server` to develop parsers with saved HTML fixtures.

---

## Roadmap

### ✅ Phase 1 Complete: Company Discovery
- SQLite storage for companies
- Search query tracking
- Automatic duplicate prevention
- Company discovery via Serper API

### 🚧 Phase 2 Next: Jobs Database
- Design jobs.db schema
- Create JobsDB utility class
- Migrate jobs from JSON to SQLite
- Link jobs to companies
- Job status tracking (new, seen, applied)

### 📋 Future improvements
- Add a simple web UI to browse matched jobs
- Implement AI-powered job scoring
- Set up automated daily digests
- Export n8n workflows to orchestrate and monitor pipelines visually
- Add credentials manager (Vault) for secrets

---

## License

Use a permissive license such as MIT. Example `LICENSE` header:

```
MIT License

Copyright (c) 2025 Your Name

Permission is hereby granted, free of charge, to any person obtaining a copy
...
```

---

## Contact / notes

This project is a personal hobby. Use responsibly and obey terms of service for the sites you scrape. If a site requests you stop scraping, comply immediately.


---

*End of README*

