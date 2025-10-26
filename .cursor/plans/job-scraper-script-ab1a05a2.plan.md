<!-- ab1a05a2-cf47-4a13-a190-7b62d079f716 320cf635-440d-4916-b7a5-d337292c21cd -->
# Job Scraper Script Plan

## Overview

Create `scripts/scrape_jobs.py` to extract engineering-relevant job links from Comeet career pages listed in `data/companies.json`.

## Implementation Details

### 1. Core Scraping Logic

- Read company URLs from `data/companies.json`
- For each company, fetch the job page HTML using `requests`
- Parse HTML with BeautifulSoup to extract job links and titles
- Handle the Comeet page structure (departments as headers, job links below)

### 2. Keyword Filtering

Create a keyword-based filter to identify engineering-relevant jobs:

- **Include keywords**: engineer, developer, software, backend, frontend, fullstack, devops, data, R&D, research, architect, programmer, technical, QA, automation, ML, AI, cloud, infrastructure, security, cyber
- **Exclude keywords**: marketing, HR, human resources, sales, finance, accounting, legal, admin, office manager, recruiter, talent acquisition

Apply case-insensitive matching on job titles and department names.

### 3. Rate Limiting & Configuration

Add to `config.yaml`:

```yaml
job_scraping:
  rate_limit_delay: 3  # seconds between company page requests
  max_retries: 3
  timeout: 30
  schedule_interval: 180  # seconds (3 minutes) between full scraping runs
```

Use `time.sleep()` between requests to avoid server stress.

### 4. Data Structure

Save to `data/jobs_raw.json` with hierarchical structure organized by company URL:

```json
{
  "https://www.comeet.com/jobs/amp/6A.005": {
    "company_name": "AMP",
    "company_url": "https://www.comeet.com/jobs/amp/6A.005",
    "last_scraped": 1234567890.123,
    "jobs": [
      {
        "job_title": "Control Engineer - Servo Motors",
        "job_url": "https://www.comeet.com/jobs/amp/6A.005/control-engineer---servo-motors/84.851",
        "department": "R&D",
        "scraped_at": 1234567890.123
      }
    ]
  }
}
```

**Key features:**

- Jobs indexed by parent company URL for easy grouping and sorting
- Deduplication based on unique `job_url` (similar to companies.json approach)
- Structure supports future web presentation with company grouping
- Preserves company context for each job listing

### 5. Scheduling & Automation

- **Continuous Operation**: Script runs every 3 minutes to fetch fresh job data
- **Incremental Updates**: Only process companies that haven't been scraped recently
- **Background Service**: Can be run as a daemon/service for continuous operation

### 6. Key Files to Modify/Create

- **Create**: `scripts/scrape_jobs.py` - main scraping script with scheduling loop
- **Update**: `config.yaml` - add job_scraping configuration section
- **Update**: `data/companies.json` - update `last_scraped` timestamp after processing
- **Output**: `data/jobs_raw.json` - filtered job listings with hierarchical structure

### 7. Error Handling

- Handle network errors gracefully (retry logic)
- Skip companies with no open positions
- Log errors without stopping the entire process
- Update `last_scraped` only on successful scrapes

### 8. Logging

Use existing logging setup from `utils.py` to track:

- Companies processed
- Jobs found per company
- Jobs filtered (relevant vs total)
- Errors and retries

### To-dos

- [ ] Create scripts/scrape_jobs.py with HTML parsing and job extraction logic
- [ ] Implement keyword-based filtering function for engineering jobs
- [ ] Add configurable rate limiting and retry logic
- [ ] Add job_scraping section to config.yaml
- [ ] Implement 3-minute scheduling loop for continuous operation
- [ ] Add deduplication logic based on job_url
- [ ] Test the scraper on a few companies and verify output format