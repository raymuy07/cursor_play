from __future__ import annotations

import asyncio
import logging
import random

import aiosqlite
import httpx

from app.common.models import validate_jobs
from app.core.scraper import JobScraper, fetch_html_from_url
from app.services.db_utils import JobsDB
from app.services.message_queue import CompanyQueue, JobQueue, QueueItem, RabbitMQConnection

logger = logging.getLogger("app.core")


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
        jobs_db: JobsDB | None = None,
    ):
        self.rabbitmq = rabbitmq
        self.jobs_db = jobs_db or JobsDB()
        self.num_workers = num_workers
        self.prefetch = prefetch

        # Internal queue for concurrent processing
        self.todo: asyncio.Queue[QueueItem] = asyncio.Queue()
        self.total_processed = 0
        self.total_failed = 0

        # Database connection (opened in run())
        self._db_connection: aiosqlite.Connection | None = None

    async def run(self):
        """Main entry point - start workers and process companies."""
        logger.info(f"Starting ScraperCoordinator with {self.num_workers} workers")

        await self.rabbitmq.connect()
        logger.info("Connected to RabbitMQ")

        job_queue = JobQueue(self.rabbitmq)
        company_queue = CompanyQueue(self.rabbitmq)

        # Open database connection for the lifetime of the coordinator
        async with aiosqlite.connect(self.jobs_db.db_path) as db:
            self._db_connection = db

            async with httpx.AsyncClient() as client:
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

                logger.info(
                    f"ScraperCoordinator finished. Processed: {self.total_processed}, Failed: {self.total_failed}"
                )

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
                    logger.warning(f"Attempt {attempt}/{max_retries} failed for {company_name}: {e}. Retrying ...")
                    await asyncio.sleep(random.randint(1, 2))
                else:
                    logger.error(f"GIVING UP on {company_name} after {max_retries} failed attempts: {e}")
                    return False

        # Rate limit between successful requests too
        await asyncio.sleep(random.randint(1, 3))

        if html:
            scraper = JobScraper(html)
            raw_jobs = scraper.extract_jobs()

            # Step 1: Validate jobs with Pydantic
            valid_jobs, invalid_jobs = validate_jobs(raw_jobs)
            if invalid_jobs:
                logger.debug(f"Filtered out {len(invalid_jobs)} invalid jobs for {company_name}")

            if not valid_jobs:
                logger.warning(f"No valid jobs extracted for {company_name} at {url}")
                return True

            # Step 2: Filter out jobs that already exist in DB
            new_jobs = await self.jobs_db.filter_existing_jobs(valid_jobs, self._db_connection)

            if new_jobs:
                logger.info(
                    f"Found {len(new_jobs)} new jobs for {company_name} (filtered {len(valid_jobs) - len(new_jobs)} existing)"
                )
                await job_queue.publish_jobs_from_url(new_jobs, url)
            else:
                logger.info(f"No new jobs for {company_name} - all {len(valid_jobs)} jobs already exist")
            return True
        else:
            logger.warning(f"No HTML content retrieved for {company_name}")
            return False
