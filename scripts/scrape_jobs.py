import json
import re
import hashlib
import os
import sys
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import random

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database access for companies and scrape scheduling
from scripts.db_utils import CompaniesDB, generate_job_hash
from scripts.utils import load_config, setup_logging


class JobExtractor:
    """Extract job information from HTML using multiple parsing strategies."""
    
    def __init__(self, html_content: str, logger: Optional[logging.Logger] = None):
        self.html_content = html_content
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.logger = logger or setup_logging()
    
    def extract_jobs(self) -> List[Dict]:
        """
        Try multiple extraction methods and return the first successful result.
        
        Returns:
            List of job dictionaries
        """
        # Method 1: Extract from JavaScript variable (Comeet pattern)
        jobs = self._extract_from_js_variable()
        if jobs:
            return jobs
        
        # Method 2: Extract from HTML elements (placeholder for alternative pattern)
        jobs = self._extract_from_html_elements()
        if jobs:
            return jobs
        
        # Method 3: Extract from JSON-LD schema (placeholder for future)
        
        return []
    
    def _extract_from_js_variable(self) -> List[Dict]:
        """
        Extract job data from JavaScript variable (Comeet pattern).
        Pattern: COMPANY_POSITIONS_DATA = [...];
        """
        try:
            # Find the COMPANY_POSITIONS_DATA variable
            pattern = r'COMPANY_POSITIONS_DATA\s*=\s*(\[.*?\]);'
            match = re.search(pattern, self.html_content, re.DOTALL)
            
            if match:
                json_str = match.group(1)
                jobs_data = json.loads(json_str)
                
                # Parse and structure the job information
                jobs = []
                for job in jobs_data:
                    job_info = {
                        'title': job.get('name'),
                        'department': job.get('department'),
                        'location': self._parse_location(job.get('location', {})),
                        'employment_type': job.get('employment_type'),
                        'experience_level': job.get('experience_level'),
                        'workplace_type': job.get('workplace_type'),
                        'uid': job.get('uid'),
                        'url': job.get('url_comeet_hosted_page'),
                        'company_name': job.get('company_name'),
                        'last_updated': job.get('time_updated'),
                        'description': self._parse_custom_fields(job.get('custom_fields', {}))
                    }
                    jobs.append(job_info)
                
                return jobs
        except (json.JSONDecodeError, AttributeError) as e:
            self.logger.warning(f"Error parsing JS variable: {e}")
        
        return []
    
    def _extract_from_html_elements(self) -> List[Dict]:
        """
        Extract job data from HTML elements (alternative pattern).
        Handles multiple common patterns including Angular-based job listings.
        """
        jobs = []
        
        # Pattern 1: Angular/Comeet positionItem links
        job_links = self.soup.find_all('a', class_='positionItem')
        
        for link in job_links:
            # Extract title
            title_elem = link.find('span', class_='positionLink')
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract URL
            url = link.get('href') or link.get('ng-href')
            
            # Extract details from the list
            details_list = link.find('ul', class_='positionDetails')
            location = None
            experience_level = None
            employment_type = None
            
            if details_list:
                items = details_list.find_all('li')
                for item in items:
                    text = item.get_text(strip=True)
                    
                    # Check if it contains location icon
                    if item.find('i', class_='fa-map-marker'):
                        location = text
                    # Check for common employment type keywords
                    elif any(keyword in text.lower() for keyword in ['full-time', 'part-time', 'contract', 'temporary', 'freelance']):
                        employment_type = text
                    # Check for experience level keywords
                    elif any(keyword in text.lower() for keyword in ['senior', 'junior', 'mid-level', 'entry', 'lead', 'principal', 'intern']):
                        experience_level = text
                    # If none of the above, try to infer
                    else:
                        # If it's a short text without special chars, might be experience or type
                        if len(text.split()) <= 2:
                            if not experience_level:
                                experience_level = text
                            elif not employment_type:
                                employment_type = text
            
            job_info = {
                'title': title,
                'location': location,
                'employment_type': employment_type,
                'experience_level': experience_level,
                'url': url
            }
            
            # Only add if we found at least a title
            if job_info['title']:
                jobs.append(job_info)
        
        # If no jobs found with Pattern 1, try Pattern 2: Generic job cards
        if not jobs:
            job_cards = self.soup.find_all('div', class_=['job-card', 'job-listing', 'job-item', 'position-card'])
            
            for card in job_cards:
                job_info = {
                    'title': self._safe_extract(card, ['h2', 'h3', '.job-title', '.position-title']),
                    'department': self._safe_extract(card, ['.department', '.team', '.category']),
                    'location': self._safe_extract(card, ['.location', '.job-location']),
                    'employment_type': self._safe_extract(card, ['.employment-type', '.job-type']),
                    'url': self._extract_link(card)
                }
                
                # Only add if we found at least a title
                if job_info['title']:
                    jobs.append(job_info)
        
        return jobs
    
    
    def _parse_location(self, location_dict: Dict) -> str:
        """Parse location dictionary into readable string."""
        if not location_dict:
            return "Not specified"
        
        parts = []
        if location_dict.get('city'):
            parts.append(location_dict['city'])
        if location_dict.get('country'):
            parts.append(location_dict['country'])
        
        if location_dict.get('is_remote'):
            parts.append("(Remote)")
        
        return ", ".join(parts) if parts else location_dict.get('name', 'Not specified')
    
    def _parse_custom_fields(self, custom_fields: Dict) -> Dict:
        """Extract description and requirements from custom fields."""
        result = {}
        
        if 'details' in custom_fields:
            for detail in custom_fields['details']:
                name = detail.get('name', '').lower()
                value = detail.get('value', '')
                
                # Skip if value is None or not a string
                if value and isinstance(value, str):
                    # Remove HTML tags for cleaner text
                    clean_value = BeautifulSoup(value, 'html.parser').get_text(separator='\n').strip()
                    result[name] = clean_value
        
        return result
    
    def _safe_extract(self, element, selectors: List[str]) -> Optional[str]:
        """Safely extract text from element using multiple selector attempts."""
        for selector in selectors:
            try:
                if selector.startswith('.'):
                    found = element.find(class_=selector[1:])
                else:
                    found = element.find(selector)
                
                if found:
                    return found.get_text(strip=True)
            except:
                continue
        return None
    
    def _extract_link(self, element) -> Optional[str]:
        """Extract job URL from element."""
        link = element.find('a', href=True)
        return link['href'] if link else None


def generate_url_hash(url: str) -> str:
    """Generate a hash from a URL for unique identification."""
    if not url:
        return ""
    return hashlib.md5(url.encode('utf-8')).hexdigest()


def add_hash_to_jobs(jobs: List[Dict]) -> List[Dict]:
    """Add a hash field to each job based on its URL."""
    for job in jobs:
        job['url_hash'] = generate_url_hash(job.get('url', ''))
    return jobs


def merge_jobs(new_jobs: List[Dict], existing_jobs: List[Dict]) -> List[Dict]:
    """
    Merge new jobs with existing jobs, avoiding duplicates based on URL.
    
    Args:
        new_jobs: List of newly extracted jobs
        existing_jobs: List of existing jobs
        
    Returns:
        Merged list of unique jobs
    """
    # Create a set of existing URL hashes for quick lookup
    existing_hashes = {job.get('url_hash', '') for job in existing_jobs}
    
    # Filter out jobs that already exist
    unique_new_jobs = [
        job for job in new_jobs 
        if job.get('url_hash', '') not in existing_hashes
    ]
    
    # Combine existing and new unique jobs
    merged_jobs = existing_jobs + unique_new_jobs
    
    return merged_jobs


def save_jobs_to_db(jobs: List[Dict], source: str = 'comeet') -> tuple[int, int]:
    """
    Placeholder for Phase 2 JobsDB integration.
    Currently returns (0, 0) and does nothing.
    """
    return 0, 0


def load_existing_jobs(filepath: str, logger: Optional[logging.Logger] = None) -> List[Dict]:
    """Load existing jobs from file if it exists."""
    logger = logger or setup_logging()
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                jobs = json.load(f)
                # Ensure all jobs have hashes
                for job in jobs:
                    if 'url_hash' not in job or not job.get('url_hash'):
                        job['url_hash'] = generate_url_hash(job.get('url', ''))
                return jobs
        except json.JSONDecodeError:
            logger.warning(f"Could not parse {filepath}, starting with empty list")
            return []
    return []


def get_scraping_config() -> Dict:
    """
    Load scraping configuration from config.yaml job_scraping section.
    Raises exception if config is missing or invalid.
    """
    cfg = load_config()
    job_scraping = cfg.get('job_scraping', {})
    
    if not job_scraping:
        raise ValueError("Missing 'job_scraping' section in config.yaml")
    
    return {
        'max_age_hours': job_scraping['max_age_hours'],
        'max_companies_per_run': job_scraping['max_companies_per_run'],
        'rate_limit_delay': job_scraping['rate_limit_delay'],
        'request_timeout': job_scraping['timeout'],
        'user_agents': job_scraping['user_agents'],
    }


def fetch_html(url: str, session: Optional[requests.Session], timeout: int, user_agent: str) -> Optional[str]:
    """Fetch HTML content for a given URL using requests."""
    
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'close',
    }
    try:
        sess = session or requests.Session()
        resp = sess.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        return None
    except requests.RequestException:
        return None


def enrich_jobs_with_company(jobs: List[Dict], company: Dict) -> List[Dict]:
    """Ensure jobs include company_name and source from the company record when missing."""
    for job in jobs:
        if not job.get('company_name'):
            job['company_name'] = company.get('company_name')
        if not job.get('source') and company.get('domain'):
            job['source'] = company.get('domain')
    return jobs


def parse_timestamp(ts: Optional[str]) -> Optional[float]:
    """Parse timestamp string to unix timestamp float. Returns None if parsing fails."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace(' ', 'T')).timestamp()
    except Exception:
        return None


def run_scrape() -> None:
    """Main scraping workflow orchestrating DB scheduling and rate limiting."""
    logger = setup_logging()
    config = get_scraping_config()
    
    logger.info(f"Starting scrape with config: max_companies={config['max_companies_per_run']}, "
                f"rate_limit={config['rate_limit_delay']}s, timeout={config['request_timeout']}s")

    jobs_json_path = os.path.join('data', 'jobs_raw.json')
    existing_jobs = load_existing_jobs(jobs_json_path, logger)
    logger.info(f"Loaded {len(existing_jobs)} existing jobs from {jobs_json_path}")

    db = CompaniesDB()
    
    # Attempt DB-side selection; if not supported, fall back to Python filtering
    try:
        to_scrape = db.get_companies_to_scrape(
            limit=config['max_companies_per_run'],
            max_age_hours=config['max_age_hours'],
        )
    except Exception as exc:
        logger.warning(f"DB-side scheduling query failed ({exc}); falling back to client-side filtering")
        all_companies = db.get_all_companies(active_only=True)
        cutoff = datetime.utcnow().timestamp() - (config['max_age_hours'] * 3600)

        def needs_scrape(company: Dict) -> bool:
            ts = parse_timestamp(company.get('last_scraped'))
            return ts is None or ts < cutoff

        candidates = [c for c in all_companies if needs_scrape(c)]
        # Sort: never-scraped first (None), then oldest scraped
        candidates.sort(key=lambda c: parse_timestamp(c.get('last_scraped')) or 0)
        to_scrape = candidates[:config['max_companies_per_run']]

    if not to_scrape:
        logger.info("No companies require scraping at this time.")
        return

    logger.info(f"Preparing to scrape {len(to_scrape)} company pages")

    session = requests.Session()
    new_jobs_total: List[Dict] = []

    for idx, company in enumerate(to_scrape, start=1):
        url = company.get('job_page_url')
        if not url:
            continue

        # Rotate user agents for better stealth
        user_agent = random.choice(config['user_agents'])
        
        logger.info(f"[{idx}/{len(to_scrape)}] Fetching {url}")
        html = fetch_html(url, session, config['request_timeout'], user_agent)
        if not html:
            logger.warning(f"Failed to fetch HTML from {url}")
            # Respect rate limit even on failure
            time.sleep(config['rate_limit_delay'])
            continue

        extractor = JobExtractor(html, logger=logger)
        extracted = extractor.extract_jobs() or []
        extracted = enrich_jobs_with_company(extracted, company)
        extracted = add_hash_to_jobs(extracted)

        if extracted:
            logger.info(f"Extracted {len(extracted)} jobs from {url}")
        else:
            logger.info(f"No jobs found for {url}")

        # Merge into in-memory accumulator
        new_jobs_total.extend(extracted)

        # Update last_scraped only after a successful HTTP fetch (regardless of jobs found)
        try:
            db.update_last_scraped(url)
        except Exception as e:
            logger.warning(f"Could not update last_scraped for {url}: {e}")

        # Rate limit between requests
        time.sleep(config['rate_limit_delay'])

    # Merge and save once for the entire run
    if new_jobs_total:
        merged_jobs = merge_jobs(new_jobs_total, existing_jobs)
        jobs_added = len(merged_jobs) - len(existing_jobs)
        with open(jobs_json_path, 'w', encoding='utf-8') as f:
            json.dump(merged_jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(merged_jobs)} jobs to {jobs_json_path} (+{jobs_added} new)")
    else:
        logger.info("No new jobs to save.")


if __name__ == "__main__":
    run_scrape()
