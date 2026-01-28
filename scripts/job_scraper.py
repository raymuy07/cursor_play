from __future__ import annotations

import asyncio
import json
import logging
import random
import re

import httpx
from bs4 import BeautifulSoup

from common.utils import setup_logging
from scripts.message_queue import CompanyQueue, JobQueue, QueueItem, RabbitMQConnection

logger = logging.getLogger(__name__)


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


class ScraperCoordinator:
    """
    Coordinates concurrent scraping of companies from the RabbitMQ queue.

    Uses the worker pattern:
    - A feeder task pulls messages from RabbitMQ into an internal asyncio.Queue
    - Multiple workers concurrently process items from the internal queue
    - Messages are acked/nacked based on processing success
    """

    def __init__(
        self,
        rabbitmq: RabbitMQConnection,
        num_workers: int = 5,
        prefetch: int = 10,
    ):
        self.rabbitmq = rabbitmq
        self.num_workers = num_workers
        self.prefetch = prefetch

        # Internal queue for concurrent processing
        self.todo: asyncio.Queue[QueueItem] = asyncio.Queue()
        self.total_processed = 0
        self.total_failed = 0

    async def run(self):
        """Main entry point - start workers and process companies."""
        logger.info(f"Starting ScraperCoordinator with {self.num_workers} workers")

        await self.rabbitmq.connect()
        logger.info("Connected to RabbitMQ")

        job_queue = JobQueue(self.rabbitmq)
        company_queue = CompanyQueue(self.rabbitmq)

        async with httpx.AsyncClient() as client:
            # Start worker tasks
            workers = [asyncio.create_task(self.worker(job_queue, client)) for _ in range(self.num_workers)]
            logger.info(f"Started {len(workers)} worker tasks")

            # Start feeder task - pulls from RabbitMQ into internal queue
            feeder = asyncio.create_task(company_queue.feed_queue(self.todo, prefetch=self.prefetch))
            logger.info("Started feeder task, waiting for messages...")

            # Wait for feeder to complete (runs until cancelled or queue empty)
            try:
                await feeder
            except asyncio.CancelledError:
                logger.info("Feeder cancelled")

            # Wait for all queued items to be processed
            await self.todo.join()

            # Cancel workers
            for worker in workers:
                worker.cancel()

            logger.info(f"ScraperCoordinator finished. Processed: {self.total_processed}, Failed: {self.total_failed}")

    async def worker(self, job_queue: JobQueue, client: httpx.AsyncClient):
        """Worker task that processes companies from the internal queue."""
        while True:
            try:
                await self.process_one(job_queue, client)
            except asyncio.CancelledError:
                return

    async def process_one(self, job_queue: JobQueue, client: httpx.AsyncClient):
        """Process a single company from the queue."""
        item = await self.todo.get()
        try:
            success = await self.scrape_company(item.data, job_queue, client)
            if success:
                self.total_processed += 1
            else:
                self.total_failed += 1
        finally:
            # Always ack - we either succeeded or gave up after retries
            await item.message.ack()
            self.todo.task_done()

    async def scrape_company(
        self, company: dict, job_queue: JobQueue, client: httpx.AsyncClient, max_retries: int = 3
    ) -> bool:
        """
        Scrape a company and publish jobs to the job queue.
        Returns True on success, False if all retries failed.
        """
        company_name = company.get("company_name", "Unknown")
        url = company.get("company_page_url")
        logger.debug(f"Processing company: {company_name} | URL: {url}")

        # Retry loop for fetching HTML
        html = None
        for attempt in range(1, max_retries + 1):
            try:
                html = await fetch_html_from_url(url, client=client)
                break  # Success, exit retry loop
            except Exception as e:
                if attempt < max_retries:
                    wait_time = random.randint(1, 3)
                    logger.warning(
                        f"Attempt {attempt}/{max_retries} failed for {company_name}: {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"GIVING UP on {company_name} after {max_retries} failed attempts: {e}")
                    return False

        # Rate limit between successful requests too
        await asyncio.sleep(random.randint(1, 3))

        if html:
            scraper = JobScraper(html)
            jobs = scraper.extract_jobs()
            if jobs:
                logger.info(f"Found {len(jobs)} jobs for {company_name}")
                await job_queue.publish_jobs_from_url(jobs, url)
            else:
                logger.warning(f"No jobs found for {company_name} at {url}")
            return True
        else:
            logger.warning(f"No HTML content retrieved for {company_name}")
            return False


class JobScraper:
    """
    Parses HTML content to extract job listings.
    This class is focused on HTML parsing only - no async I/O.
    """

    def __init__(self, html_content: str):
        self._html_content = html_content
        self._soup = BeautifulSoup(html_content, "html.parser")

    def extract_jobs(self) -> list[dict]:
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

    def _extract_from_js_variable(self) -> list[dict]:
        """
        Extract job data from JavaScript variable (Comeet pattern).
        Pattern: COMPANY_POSITIONS_DATA = [...];
        """
        try:
            # Find the COMPANY_POSITIONS_DATA variable
            pattern = r"COMPANY_POSITIONS_DATA\s*=\s*(\[.*?\]);"
            match = re.search(pattern, self._html_content, re.DOTALL)

            if match:
                json_str = match.group(1)
                jobs_data = json.loads(json_str)

                # Parse and structure the job information
                jobs = []
                for job in jobs_data:
                    job_info = {
                        "title": job.get("name"),
                        "department": job.get("department"),
                        "location": self._parse_location(job.get("location", {})),
                        "employment_type": job.get("employment_type"),
                        "experience_level": job.get("experience_level"),
                        "workplace_type": job.get("workplace_type"),
                        "uid": job.get("uid"),
                        "url": job.get("url_comeet_hosted_page"),
                        "company_name": job.get("company_name"),
                        "email": job.get("email"),
                        "last_updated": job.get("time_updated"),
                        "original_website_job_url": self._get_original_website_url(job),
                        "description": self._parse_custom_fields(job.get("custom_fields", {})),
                    }
                    jobs.append(job_info)

                return jobs
        except (json.JSONDecodeError, AttributeError) as e:
            logger.warning(f"Error parsing JS variable: {e}")

        return []

    def _extract_from_html_elements(self) -> list[dict]:
        """
        Extract job data from HTML elements (alternative pattern).
        Handles multiple common patterns including Angular-based job listings.
        """
        jobs = []

        # Pattern 1: Angular/Comeet positionItem links
        job_links = self._soup.find_all("a", class_="positionItem")

        for link in job_links:
            # Extract title
            title_elem = link.find("span", class_="positionLink")
            title = title_elem.get_text(strip=True) if title_elem else None

            # Extract URL
            url = link.get("href") or link.get("ng-href")

            # Extract details from the list
            details_list = link.find("ul", class_="positionDetails")
            location = None
            experience_level = None
            employment_type = None

            if details_list:
                items = details_list.find_all("li")
                for item in items:
                    text = item.get_text(strip=True)

                    # Check if it contains location icon
                    if item.find("i", class_="fa-map-marker"):
                        location = text
                    # Check for common employment type keywords
                    elif any(
                        keyword in text.lower()
                        for keyword in ["full-time", "part-time", "contract", "temporary", "freelance"]
                    ):
                        employment_type = text
                    # Check for experience level keywords
                    elif any(
                        keyword in text.lower()
                        for keyword in ["senior", "junior", "mid-level", "entry", "lead", "principal", "intern"]
                    ):
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
                "title": title,
                "location": location,
                "employment_type": employment_type,
                "experience_level": experience_level,
                "url": url,
            }

            # Only add if we found at least a title
            if job_info["title"]:
                jobs.append(job_info)

        # If no jobs found with Pattern 1, try Pattern 2: Generic job cards
        if not jobs:
            job_cards = self._soup.find_all("div", class_=["job-card", "job-listing", "job-item", "position-card"])
            logger.debug(f"Pattern 2 (generic job cards): found {len(job_cards)} elements")

            for card in job_cards:
                job_info = {
                    "title": self._safe_extract(card, ["h2", "h3", ".job-title", ".position-title"]),
                    "department": self._safe_extract(card, [".department", ".team", ".category"]),
                    "location": self._safe_extract(card, [".location", ".job-location"]),
                    "employment_type": self._safe_extract(card, [".employment-type", ".job-type"]),
                    "url": self._extract_link(card),
                }

                # Only add if we found at least a title
                if job_info["title"]:
                    jobs.append(job_info)

        return jobs

    def _get_original_website_url(self, job: dict) -> str | None:
        """
        Get the original website job URL, but only if it's different from the main URL.
        """
        main_url = job.get("url_comeet_hosted_page")
        url_active = job.get("url_active_page")
        url_detected = job.get("url_detected_page")

        # Try url_active_page first, then url_detected_page
        original_url = url_active or url_detected

        # Only return if it's different from the main URL
        if original_url and original_url != main_url:
            return original_url

        return None

    def _parse_location(self, location_dict: dict) -> str:
        """Parse location dictionary into readable string."""
        if not location_dict:
            return "Not specified"

        parts = []
        if location_dict.get("city"):
            parts.append(location_dict["city"])
        if location_dict.get("country"):
            if location_dict["country"] == "IL":
                parts.append("ISRAEL")
            else:
                parts.append(location_dict["country"])

        if location_dict.get("is_remote"):
            parts.append("(Remote)")

        return ", ".join(parts) if parts else location_dict.get("name", "Not specified")

    def _parse_custom_fields(self, custom_fields: dict) -> dict:
        """Extract description and requirements from custom fields."""
        result = {}

        if "details" in custom_fields:
            for detail in custom_fields["details"]:
                name = detail.get("name", "").lower()
                value = detail.get("value", "")

                # Skip if value is None or not a string
                if value and isinstance(value, str):
                    # Remove HTML tags for cleaner text
                    clean_value = BeautifulSoup(value, "html.parser").get_text(separator="\n").strip()
                    result[name] = clean_value

        return result

    def _safe_extract(self, element, selectors: list[str]) -> str | None:
        """Safely extract text from element using multiple selector attempts."""
        for selector in selectors:
            try:
                if selector.startswith("."):
                    found = element.find(class_=selector[1:])
                else:
                    found = element.find(selector)

                if found:
                    return found.get_text(strip=True)
            except Exception as e:
                logger.warning(f"Error extracting text from element: {e}")
                continue
        return None

    def _extract_link(self, element) -> str | None:
        """Extract job URL from element."""
        link = element.find("a", href=True)
        return link["href"] if link else None


async def fetch_html_from_url(url: str, client: httpx.AsyncClient) -> str | None:
    """Fetch HTML content for a given URL using requests."""
    """I think this is a key function cause we may encounter problems with fetching html on a proxy"""

    user_agent = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "close",
    }

    logger.debug(f"Fetching URL: {url}")
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        resp.raise_for_status()  # This raises an error for 500s, 403s, etc.
        logger.debug(f"Fetched {url} - status: {resp.status_code}, length: {len(resp.text)} chars")
        return resp.text

    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        raise  # Let the queue handle the retry


async def main():
    """Entry point for the job scraper service."""

    rabbitmq = RabbitMQConnection()
    coordinator = ScraperCoordinator(
        rabbitmq=rabbitmq,
        num_workers=5,
        prefetch=10,
    )

    try:
        await coordinator.run()
    finally:
        await rabbitmq.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main(), debug=True)
