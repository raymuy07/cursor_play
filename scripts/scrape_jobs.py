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

from functools import partial
import asyncio
import aio_pika
from scripts.queue import RabbitMQConnection


# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database access for companies and scrape scheduling
from scripts.db_utils import CompaniesDB, JobsDB
from common.utils import load_config
from queue import CompanyQueue, JobQueue

logger = logging.getLogger(__name__)

class JobScraper:
    """In charge of the whole job scraping process
    it will recieve an url of a company job page and will return a list of jobs as json"""

    def __init__(self):
        self.html_content = html_content
        self.soup = BeautifulSoup(html_content, 'html.parser')

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
                        'email': job.get('email'),
                        'last_updated': job.get('time_updated'),
                        'original_website_job_url': self._get_original_website_url(job),
                        'description': self._parse_custom_fields(job.get('custom_fields', {}))
                    }
                    jobs.append(job_info)

                return jobs
        except (json.JSONDecodeError, AttributeError) as e:
            logger.warning(f"Error parsing JS variable: {e}")

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


    def _get_original_website_url(self, job: Dict) -> Optional[str]:
        """
        Get the original website job URL, but only if it's different from the main URL.
        """
        main_url = job.get('url_comeet_hosted_page')
        url_active = job.get('url_active_page')
        url_detected = job.get('url_detected_page')

        # Try url_active_page first, then url_detected_page
        original_url = url_active or url_detected

        # Only return if it's different from the main URL
        if original_url and original_url != main_url:
            return original_url

        return None

    def _parse_location(self, location_dict: Dict) -> str:
        """Parse location dictionary into readable string."""
        if not location_dict:
            return "Not specified"

        parts = []
        if location_dict.get('city'):
            parts.append(location_dict['city'])
        if location_dict.get('country'):
            if location_dict['country'] == 'IL':
                parts.append("ISRAEL")
            else:
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
            except Exception as e:
                logger.warning(f"Error extracting text from element: {e}")
                continue
        return None

    def _extract_link(self, element) -> Optional[str]:
        """Extract job URL from element."""
        link = element.find('a', href=True)
        return link['href'] if link else None


    ##probably deprecated cause i want to post the clean jobs json to a rabbitmq queue
    def save_scraped_jobs_to_temp(jobs: List[Dict], temp_file_path: Optional[str] = None) -> str:
        """
        Save scraped jobs to a temporary JSON file.
        This allows recovery if database insertion fails.
        """
        if temp_file_path is None:
            # Generate timestamped temp file in data directory
            tmp_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'temp')
            os.makedirs(tmp_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_file_path = os.path.join(tmp_dir, f'temp_scraped_jobs_{timestamp}.json')

        try:
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False, default=str)
            return temp_file_path
        except Exception as e:
            logger.error(f"Failed to save scraped jobs to temp file '{temp_file_path}': {e}")
            raise





def job_is_hebrew_filter(job: Dict) -> bool:
    """
    Check if a job contains Hebrew text.

    Args:
        job: Job dictionary with title, description, location, etc.

    Returns:
        True if job appears to be in Hebrew, False otherwise
    """
    # Check multiple fields for Hebrew characters
    # Handle description which might be a dict
    description = job.get('description', '')
    if isinstance(description, dict):
        description = ' '.join(str(v) for v in description.values() if v)

    fields_to_check = [
        job.get('title', ''),
        description,
        job.get('location', ''),
        job.get('company_name', '')
    ]

    # Combine all text fields
    combined_text = ' '.join(str(field) for field in fields_to_check if field)

    if not combined_text:
        return False

    # Count Hebrew characters
    hebrew_chars = sum(1 for c in combined_text if '\u0590' <= c <= '\u05FF')
    total_alpha_chars = sum(1 for c in combined_text if c.isalpha())

    # If more than 10% of alphabetic characters are Hebrew, consider it a Hebrew job
    if total_alpha_chars > 0:
        hebrew_ratio = hebrew_chars / total_alpha_chars
        return hebrew_ratio > 0.1

    return False

def is_in_israel_filter(job: Dict) -> bool:

    location = job.get('location', '')
    return "ISRAEL" in location


def filter_valid_jobs(jobs: List[Dict]) -> tuple[List[Dict], Dict[str, int]]:
    """
    Filter jobs based on validation criteria.
    Returns valid jobs and a breakdown of filtered counts.

    Returns:
    Tuple of (valid_jobs, filter_counts) where:
    - valid_jobs: List of jobs that passed all filters
    - filter_counts: Dictionary with counts of filtered jobs by reason
                    e.g., {'hebrew': 5, 'general_department': 2}
    """
    valid_jobs = []
    filter_counts = {
        'hebrew': 0,
        'general_department': 0,
        'job_not_in_israel': 0
    }

    for job in jobs:

        # Filter: Jobs not in Israel
        if not is_in_israel_filter(job):
            filter_counts['job_not_in_israel'] += 1
            continue

        # Filter: Hebrew jobs
        if job_is_hebrew_filter(job):
            filter_counts['hebrew'] += 1
            continue

        # Filter: General department jobs
        try:
            department = str(job.get('department')).strip().lower()
            if department == 'general':
                filter_counts['general_department'] += 1
                continue
        except AttributeError:
            pass

        valid_jobs.append(job)

    return valid_jobs, filter_counts


class JobPersister:


    def save_jobs_to_db(jobs: List[Dict], jobs_db: JobsDB) -> tuple[bool, int, int]:
        """
        Save jobs to the jobs database using JobsDB.
        Automatically handles duplicate detection (via URL uniqueness).
        Filters out invalid jobs using filter_valid_jobs().

        Args:
            jobs: List of job dictionaries to save
            jobs_db: JobsDB instance for database operations
            logger: Optional logger instance

        Returns:
            Tuple of (success, jobs_inserted, jobs_skipped) where:
            - success: True if save operation completed without critical errors
            - jobs_inserted: Number of new jobs successfully inserted
            - jobs_skipped: Number of jobs skipped (duplicates or errors)
        """
        inserted = 0
        skipped = 0
        errors = 0

        # Filter out invalid jobs using external filtering function
        valid_jobs, filter_counts = filter_valid_jobs(jobs)
        logger.info(f"Filter counts: {filter_counts}")
        # Process only valid jobs
        for job in valid_jobs:
            try:
                job_id = jobs_db.insert_job(job)
                if job_id:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Unexpected error processing job with URL '{job.get('url')}': {e}", exc_info=True)
                skipped += 1
                errors += 1

        # Success if we processed jobs without too many errors (allow some failures)
        success = errors == 0 or (errors < len(valid_jobs) * 0.5)  # Fail if >50% errors

        return success, inserted, skipped


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


    def enrich_jobs_with_company(jobs: List[Dict], company: Dict) -> List[Dict]:
        """Ensure jobs include company_name and source from the company record when missing."""
        for job in jobs:
            if not job.get('company_name'):
                job['company_name'] = company.get('company_name')
            if not job.get('source') and company.get('domain'):
                job['source'] = company.get('domain')
        return jobs

    # def get_scraping_config() -> Dict:
    #     """
    #     Load scraping configuration from config.yaml job_scraping section.
    #     Raises exception if config is missing or invalid.
    #     """
    #     cfg = load_config()
    #     job_scraping = cfg.get('job_scraping', {})

    #     if not job_scraping:
    #         raise ValueError("Missing 'job_scraping' section in config.yaml")

    #     return {
    #         'max_age_hours': job_scraping['max_age_hours'],
    #         'max_companies_per_run': job_scraping['max_companies_per_run'],
    #         'rate_limit_delay': job_scraping['rate_limit_delay'],
    #         'request_timeout': job_scraping['timeout'],
    #         'user_agents': job_scraping['user_agents'],
    #     }









def recover_temp_files(jobs_db: JobsDB) -> None:
    """Recover jobs from temp files and insert them into the database"""

    try:
        tmp_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'temp')
        os.makedirs(tmp_path, exist_ok=True)
        for tmp_file in os.listdir(tmp_path):
            tmp_file_path = os.path.join(tmp_path, tmp_file)
            logger.info(f"Temp file exists: {tmp_file}, Retrying insertion")
            with open(tmp_file_path, 'r', encoding='utf-8') as f:
                jobs = json.load(f)
                logger.info(f"Loaded {len(jobs)} jobs from temp file")
                success, inserted, skipped= save_jobs_to_db(jobs, jobs_db)
                if success:
                    logger.info(f"Successfully inserted {inserted} jobs from temp file ({skipped} skipped)")
                    os.remove(tmp_file_path)
                    logger.info(f"Temp file deleted: {tmp_file_path}")
                else:
                    logger.error(f"Failed to insert jobs from temp file: {tmp_file_path}")
    except Exception as e:
        logger.error(f"Failed to load scraped jobs from temp file: {e}")
        raise







async def fetch_html_from_url(url: str, session: Optional[requests.Session], timeout: int, user_agent: str) -> Optional[str]:
    """Fetch HTML content for a given URL using requests."""
    """I think this is a key function cause we may encounter problems with fetching html on a proxy"""

    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'close',
    }
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        return None
    except requests.RequestException:
        return None



async def process_company(message: aio_pika.IncomingMessage, job_queue:JobQueue):
    """ this function in charge of consuming the company_queue urls and to scrpae them using the JobScraper"""
    async with message.process():
        company = json.loads(message.body)
        html =  fetch_html_from_url(company['job_page_url'])


        scraper = JobScraper()
        clean_jobs = scraper.extract_jobs(html)

        ## I need to think how to seperate the jobs. by comma?
        if clean_jobs:
            job_queue.publish_batch()



async def consume():

    connection = await aio_pika.connect_robust("amqp://localhost/")
    rabbitmq = RabbitMQConnection()
    job_queue = JobQueue(rabbitmq)

    channel = await connection.channel()
    queue = await channel.declare_queue("companies_to_scrape", durable=True)


    ###???
    scraper = JobScraper()

    callback = partial(process_company,job_queue=job_queue)

    await queue.consume(callback)

    # that makes a promise that is never resolved.
    await asyncio.Future()








def scrape_jobs_from_companies_deprecated(companies: List[Dict], config: Dict, companies_db: CompaniesDB) -> List[Dict]:

    new_jobs_total: List[Dict] = []
    temp_file_path = None

    with requests.Session() as session:

        for idx, company in enumerate(companies, start=1):
            url = company.get('job_page_url')
            if not url:
                logger.warning(f"Company {company.get('company_name')} has no job page URL")
                continue

            # Rotate user agents for better stealth
            user_agent = random.choice(config['user_agents'])

            logger.info(f"[{idx}/{len(companies)}] Fetching {url}")
            html = fetch_html(url, session, config['request_timeout'], user_agent)
            if not html:
                logger.warning(f"Failed to fetch HTML from {url}")
                # Respect rate limit even on failure
                time.sleep(config['rate_limit_delay'])
                continue

            extractor = JobScraper(html)
            extracted = extractor.extract_jobs() or []
            extracted = enrich_jobs_with_company(extracted, company)
            extracted = add_hash_to_jobs(extracted)

            if extracted:
                logger.info(f"Extracted {len(extracted)} jobs from {url}")
            else:
                logger.info(f"No jobs found for {url}")

            # Accumulate jobs for batch insertion
            new_jobs_total.extend(extracted)

            # Update last_scraped only after a successful HTTP fetch (regardless of jobs found)
            try:
                companies_db.update_last_scraped(url)
            except Exception as e:
                logger.warning(f"Could not update last_scraped for {url}: {e}")

            # Rate limit between requests
            time.sleep(config['rate_limit_delay'])

        # Save all extracted jobs to temporary file first (to preserve if DB insertion fails)
        if not new_jobs_total:
            logger.info("No new jobs to save.")
            return
        try:
            temp_file_path = save_scraped_jobs_to_temp(new_jobs_total)
            logger.info(f"Saved {len(new_jobs_total)} scraped jobs to temporary file: {temp_file_path}")
        except Exception as e:
            logger.error(f"Failed to save scraped jobs to temp file: {e}")
            logger.error("Scraped jobs may be lost if database insertion fails!")

        return new_jobs_total, temp_file_path
# def persist_jobs(jobs: List[Dict], jobs_db: JobsDB, logger) -> None:


def run_scrape() -> None:
    """Main scraping workflow orchestrating DB scheduling and rate limiting."""
    config = get_scraping_config()

    logger.info(f"Starting scrape with config: max_companies={config['max_companies_per_run']}, "
                f"rate_limit={config['rate_limit_delay']}s, timeout={config['request_timeout']}s")

    # Initialize database connections
    companies_db = CompaniesDB()
    jobs_db = JobsDB()

    """We want to check if there are any jobs in the temp file and if so, try to insert them into the database
    the existence of a temp file is already a red flag so we should not retry this operation all the time"""

    recover_temp_files(jobs_db)

    to_scrape = select_companies_to_scrape(companies_db, config)
    new_jobs_total, temp_file_path = scrape_jobs_from_companies_deprecated(to_scrape, config, companies_db)




    try:
        success, inserted, skipped= save_jobs_to_db(new_jobs_total, jobs_db)
        if success:
            logger.info(f"Successfully saved {inserted} new jobs to database ({skipped} duplicates skipped)")
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Temp file deleted after successful insertion: {temp_file_path}")
                except OSError as remove_err:
                    logger.warning(f"Failed to delete temp file '{temp_file_path}': {remove_err}")
        else:
            logger.error(f"Database insertion had too many errors: {inserted} inserted, {skipped} skipped")
            if temp_file_path:
                logger.error(f"Scraped jobs preserved in temp file: {temp_file_path}")
    except Exception as e:
        logger.error(f"CRITICAL: Database insertion failed: {e}", exc_info=True)
        if temp_file_path:
            logger.error(f"Scraped jobs are preserved in temp file: {temp_file_path}")
            logger.error("You can retry insertion by loading from the temp file later")
        else:
            logger.error("Scraped jobs may have been lost - no temp file available")
        raise


if __name__ == "__main__":
    asyncio.run(consume())
