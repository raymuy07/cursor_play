import json
import re
import os
import sys
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
import random
import httpx

from functools import partial
import asyncio
import aio_pika



from scripts.message_queue import RabbitMQConnection, CompanyQueue, JobQueue
from scripts.db_utils import CompaniesDB, JobsDB
from common.utils import setup_logging, load_config

logger = logging.getLogger(__name__)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]

class JobScraper:
    """In charge of the whole job scraping process
    it will recieve an url of a company job page and will return a list of jobs as json"""

    def __init__(self, html_content: str):
        self._html_content = html_content
        self._soup = BeautifulSoup(html_content, 'html.parser')

    def extract_jobs(self) -> List[Dict]:
        """
        Try multiple extraction methods and return the first successful result.

        """
        logger.debug(f"Starting job extraction, HTML length: {len(self._html_content)} chars")

        # Method 1: Extract from JavaScript variable (Comeet pattern)
        logger.debug("Trying extraction method 1: JS variable (Comeet pattern)")
        jobs = self._extract_from_js_variable()
        if jobs:
            logger.debug(f"Method 1 succeeded: found {len(jobs)} jobs")
            return jobs

        # Method 2: Extract from HTML elements (placeholder for alternative pattern)
        logger.debug("Trying extraction method 2: HTML elements")
        jobs = self._extract_from_html_elements()
        if jobs:
            logger.debug(f"Method 2 succeeded: found {len(jobs)} jobs")
            return jobs

        # Method 3: Extract from JSON-LD schema (placeholder for future)
        logger.debug("No jobs found with any extraction method")
        return []

    def _extract_from_js_variable(self) -> List[Dict]:
        """
        Extract job data from JavaScript variable (Comeet pattern).
        Pattern: COMPANY_POSITIONS_DATA = [...];
        """
        try:
            # Find the COMPANY_POSITIONS_DATA variable
            pattern = r'COMPANY_POSITIONS_DATA\s*=\s*(\[.*?\]);'
            match = re.search(pattern, self._html_content, re.DOTALL)

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
        job_links = self._soup.find_all('a', class_='positionItem')

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
            job_cards = self._soup.find_all('div', class_=['job-card', 'job-listing', 'job-item', 'position-card'])
            logger.debug(f"Pattern 2 (generic job cards): found {len(job_cards)} elements")

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





async def fetch_html_from_url(url: str, client: httpx.AsyncClient) -> Optional[str]:
    """Fetch HTML content for a given URL using requests."""
    """I think this is a key function cause we may encounter problems with fetching html on a proxy"""

    user_agent = random.choice(USER_AGENTS)
    headers = {
        'User-Agent': user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'close',
    }

    logger.debug(f"Fetching URL: {url}")
    try:
        resp = await client.get(url, headers=headers, timeout = 10)
        resp.raise_for_status() # This raises an error for 500s, 403s, etc.
        logger.debug(f"Fetched {url} - status: {resp.status_code}, length: {len(resp.text)} chars")
        return resp.text

    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        raise # Let the queue handle the retry



async def process_company(company: Dict, job_queue:JobQueue, client: httpx.AsyncClient):
    """this function lives inside a queue consumer and is in charge of scraping a company and publishing the jobs to the jobs queue"""

    company_name = company.get('company_name', 'Unknown')
    url = company.get('job_page_url')
    logger.debug(f"Processing company: {company_name} | URL: {url}")

    html = await fetch_html_from_url(url, client=client)

    if html:
        scraper = JobScraper(html)
        jobs = scraper.extract_jobs()
        if jobs:
            logger.info(f"Found {len(jobs)} jobs for {company_name}")
            await job_queue.publish_batch(jobs, url)
        else:
            logger.warning(f"No jobs found for {company_name} at {url}")
    else:
        logger.warning(f"No HTML content retrieved for {company_name}")

async def start_worker():
    """Start the worker and connect to RabbitMQ"""
    logger.info("Initializing job scraper worker...")

    rabbitmq = RabbitMQConnection()
    logger.debug(f"Connecting to RabbitMQ at {rabbitmq.host}:{rabbitmq.port}")
    await rabbitmq.connect()
    logger.info("Connected to RabbitMQ successfully")

    job_queue = JobQueue(rabbitmq)
    company_queue = CompanyQueue(rabbitmq)
    logger.debug("Queues initialized: companies_to_scrape, jobs_to_persist")

    async with httpx.AsyncClient() as client:
        # Use partial to 'pre-fill' the arguments for the queue
        callback = partial(process_company, job_queue=job_queue, client=client)

        logger.info("Company scraper worker started, waiting for messages...")
        await company_queue.consume(callback, prefetch=10)




if __name__ == "__main__":

    ##This is a place holder for testing. it should run only once by the main script.
    setup_logging()

    asyncio.run(start_worker())
