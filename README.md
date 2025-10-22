# Job Hunter

A small, personal project to discover, scrape and score job listings for small companies hosted on Comeet (and similar providers). The pipeline is implemented in **Python** and orchestrated with **cron**. This README documents goals, architecture, installation, usage, data formats, and operational notes.

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
   - Run the Google dork using Playwright to gather search result links.
   - Normalize and deduplicate company job page URLs.
   - Save results into `data/companies.json`.

2. **Job extraction** (`scrape_jobs.py`)
   - For each company page, fetch the page and extract job metadata.
   - Use lightweight scraping (requests + BeautifulSoup) where possible; fall back to Playwright only if JS rendering is required.
   - Save raw results into `data/jobs_raw.json` and a filtered/normalized form in `data/jobs_filtered.json`.

3. **Scoring & delivery** (`score_jobs.py`, `send_digest.py`)
   - Apply a scoring function (AI + rules) that ranks jobs according to your skills and preferences.
   - Keep a job history / seen-set to avoid duplicates in daily digests.
   - Send daily digest via Telegram, email, or another channel.

Scheduling and orchestration are handled by `cron` jobs.

---

## Directory structure (suggested)

```
job-hunter/
├── README.md
├── data/
│   ├── companies.json          # deduplicated list of company job pages
│   ├── jobs_raw.json           # raw scrape outputs
│   ├── jobs_filtered.json      # normalized & filtered jobs
│   └── seen_jobs.db            # optional SQLite or JSON of seen job IDs
├── scripts/
│   ├── discover_companies.py
│   ├── scrape_jobs.py
│   ├── score_jobs.py
│   ├── send_digest.py
│   └── utils.py                # helpers (dedup, http client wrapper, logging)
├── workflows/                  # optional exports (n8n later if desired)
├── requirements.txt
├── config.yaml                 # scheduler, thresholds, API keys, scraping rules
└── logs/
    └── jobhunter.log
```

---

## Prerequisites

- Python 3.10+ (recommended)
- Node & Playwright (only for `discover_companies.py` and any JS-rendered pages)
- pip packages from `requirements.txt` (requests, beautifulsoup4, playwright, pydantic, sqlite3 or tinydb, openai or other AI client, python-telegram-bot or requests for webhook delivery)

Example `requirements.txt` minimal:

```
playwright
requests
beautifulsoup4
pydantic
python-dotenv
openai
python-telegram-bot
APScheduler
```

(Install Playwright browsers after installing the package: `playwright install`)

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

### `companies.json`

An array of unique company job-page URLs, e.g.:

```json
[
  "https://company1.com/jobs",
  "https://company2.com/careers"
]
```

### `jobs_raw.json`

Raw entries as extracted from pages. Example entry:

```json
{
  "company_url": "https://company1.com/jobs",
  "fetched_at": "2025-10-21T12:00:00+03:00",
  "html_snippet": "<div class=\"job-listing\">...</div>",
  "job_id": "comeet-12345"
}
```

### `jobs_filtered.json`

Normalized job entries used for scoring:

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

## Future improvements

- Add a simple web UI to browse matched jobs.
- Replace JSON files with a small SQLite DB for more robust queries.
- Export n8n workflows to orchestrate and monitor pipelines visually.
- Add credentials manager (Vault) for secrets.

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

