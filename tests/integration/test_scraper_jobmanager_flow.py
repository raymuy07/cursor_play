"""
Integration test for Scraper Worker → Job Manager flow with REAL scraping.

Tests the complete flow:
1. Publish companies to company queue
2. Scraper worker consumes companies, scrapes REAL websites, publishes jobs to jobs queue
3. Job manager drains jobs queue, filters, and inserts to mock DB (without embeddings)

Run with: pytest -m integration tests/test_scraper_jobmanager_flow.py -v -s --log-cli-level=INFO
Requires RabbitMQ running on localhost:5672 (use Docker: docker run -d -p 5672:5672 rabbitmq)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.core.db_utils import generate_url_hash
from app.core.message_queue import CompanyQueue, JobQueue, QueueItem, RabbitMQConnection
from app.models.job import validate_jobs
from app.services.job_filter import JobFilter
from app.services.job_persister import JobPersister
from app.services.scraper import JobScraper, fetch_html_from_url

logger = logging.getLogger(__name__)


# ============================================================================
# REAL COMPANY URLs - From your companies.db database
# ============================================================================
REAL_COMPANIES = [
    {
        "company_name": "Insycle",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/Insycle/D3.002",
    },
    {
        "company_name": "Kaltura",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/kaltura/E2.00D",
    },
    {
        "company_name": "NextTA",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/nextta/5A.006",
    },
    {
        "company_name": "Netafim",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/netafim/B7.002",
    },
    {
        "company_name": "Quicklizard",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/quicklizard/C9.004",
    },
    {
        "company_name": "Firefly",
        "domain": "comeet.com",
        "company_page_url": "https://www.comeet.com/jobs/firefly/39.000",
    },
]


def get_random_companies(count: int = 2) -> list[dict]:
    """Get N random companies from the list (only those with URLs filled in)."""
    valid_companies = [c for c in REAL_COMPANIES if c["company_page_url"]]
    if len(valid_companies) < count:
        raise ValueError(f"Need at least {count} companies with URLs filled in. Found: {len(valid_companies)}")
    return random.sample(valid_companies, count)


class RealScraperWorker:
    """
    Real scraper worker that actually scrapes HTTP.
    Consumes from company queue, scrapes pages, publishes jobs to job queue.
    """

    def __init__(self, rabbitmq: RabbitMQConnection):
        self.rabbitmq = rabbitmq
        self.companies_processed: list[dict] = []
        self.jobs_published: list[dict] = []
        self.scrape_errors: list[dict] = []

    async def process_company(self, company: dict, job_queue: JobQueue, client: httpx.AsyncClient) -> bool:
        """Process a single company - REAL scraping."""
        url = company.get("company_page_url")
        company_name = company.get("company_name", "Unknown")

        logger.info(f"🌐 [RealScraper] Scraping company: {company_name} | URL: {url}")
        self.companies_processed.append(company)

        try:
            # REAL HTTP fetch
            html = await fetch_html_from_url(url, client)

            if not html:
                logger.warning(f"No HTML returned for {company_name}")
                return False

            logger.info(f"📄 [RealScraper] Got {len(html)} chars of HTML from {company_name}")

            # REAL parsing
            scraper = JobScraper(html)
            raw_jobs = scraper.extract_jobs()

            if not raw_jobs:
                logger.warning(f"No jobs extracted from {company_name}")
                return False

            logger.info(f"📋 [RealScraper] Extracted {len(raw_jobs)} raw jobs from {company_name}")

            # Validate jobs
            valid_jobs, invalid_jobs = validate_jobs(raw_jobs)

            if invalid_jobs:
                logger.debug(f"Filtered out {len(invalid_jobs)} invalid jobs for {company_name}")

            if valid_jobs:
                logger.info(f"📤 [RealScraper] Publishing {len(valid_jobs)} valid jobs for {company_name}")
                await job_queue.publish_jobs_from_url(valid_jobs, url)
                self.jobs_published.extend(valid_jobs)
                return True
            else:
                logger.warning(f"No valid jobs for {company_name} after validation")
                return False

        except Exception as e:
            logger.error(f"❌ [RealScraper] Error scraping {company_name}: {e}")
            self.scrape_errors.append({"company": company_name, "error": str(e)})
            return False

    async def run(self, timeout_seconds: float = 30.0):
        """
        Run the real scraper: consume from company queue, scrape, publish to jobs queue.
        """
        await self.rabbitmq.connect()

        company_queue = CompanyQueue(self.rabbitmq)
        job_queue = JobQueue(self.rabbitmq)

        # Internal queue for processing
        todo: asyncio.Queue[QueueItem] = asyncio.Queue()

        async with httpx.AsyncClient() as client:

            async def process_worker():
                """Worker that processes companies from internal queue."""
                while True:
                    try:
                        item = await asyncio.wait_for(todo.get(), timeout=3.0)
                        await self.process_company(item.data, job_queue, client)
                        await item.message.ack()
                        todo.task_done()
                    except asyncio.TimeoutError:
                        # No more items, exit worker
                        break
                    except asyncio.CancelledError:
                        break

            # Feed from RabbitMQ queue to internal queue
            async def feed_internal_queue():
                with contextlib.suppress(asyncio.CancelledError):
                    await company_queue.feed_queue(todo, prefetch=10)

            # Start feeder and worker
            feeder_task = asyncio.create_task(feed_internal_queue())
            worker_task = asyncio.create_task(process_worker())

            # Wait for either timeout or completion
            try:
                await asyncio.wait_for(worker_task, timeout=timeout_seconds)
            except asyncio.TimeoutError:
                logger.info("Scraper timeout reached")
            finally:
                feeder_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await feeder_task


class MockJobsDB:
    """
    Mock JobsDB for testing - stores jobs in memory without embeddings.
    """

    def __init__(self):
        self.jobs: dict[str, dict] = {}  # url_hash -> job
        self._conn = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass

    async def insert_job(self, job_data: dict) -> str | None:
        """Insert job and return url_hash (without embedding)."""
        url = job_data.get("url")
        if not url:
            return None

        url_hash = generate_url_hash(url)

        # Check for duplicate
        if url_hash in self.jobs:
            logger.debug(f"Job already exists: {url}")
            return None

        # Store job (without embedding)
        self.jobs[url_hash] = {**job_data, "url_hash": url_hash, "embedding": None}
        logger.info(f"📥 [MockDB] Inserted job: {job_data.get('title')} ({url_hash[:8]}...)")
        return url_hash

    async def filter_existing_jobs(self, jobs: list[dict]) -> list[dict]:
        """Return only jobs that don't exist yet."""
        return [j for j in jobs if generate_url_hash(j.get("url", "")) not in self.jobs]


class MockPendingDB:
    """Mock PendingEmbeddedDB for testing."""

    def __init__(self):
        self.batches: dict[str, str] = {}  # batch_id -> status
        self._conn = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass

    async def insert_pending_batch_id(self, batch_id: str):
        self.batches[batch_id] = "processing"
        return True


@pytest.mark.integration
@pytest.mark.asyncio
class TestScraperJobManagerFlow:
    """
    Integration tests for the Scraper → Job Manager flow with REAL scraping.

    Tests that:
    1. Companies are published to company queue
    2. Scraper worker consumes, REALLY scrapes, and publishes to jobs queue
    3. Job manager drains, filters, and inserts to DB
    """

    @pytest.fixture
    async def rabbitmq(self):
        """Create a RabbitMQ connection for tests."""
        conn = RabbitMQConnection(host="localhost", port=5672)
        await conn.connect()

        # Purge queues to start fresh
        try:
            companies_q = await conn.channel.get_queue("companies_to_scrape")
            await companies_q.purge()
            jobs_q = await conn.channel.get_queue("jobs_to_persist")
            await jobs_q.purge()
            logger.info("🧹 Purged test queues")
        except Exception as e:
            logger.debug(f"Queue purge error (may not exist yet): {e}")

        yield conn
        await conn.close()

    @pytest.fixture
    def mock_jobs_db(self):
        """Create a mock jobs database."""
        return MockJobsDB()

    @pytest.fixture
    def mock_pending_db(self):
        """Create a mock pending database."""
        return MockPendingDB()

    async def test_full_scraper_to_jobmanager_flow(self, rabbitmq, mock_jobs_db, mock_pending_db):
        """
        Full E2E test: Company Queue → REAL Scraper → Jobs Queue → Job Manager → DB

        Flow:
        1. Pick 2 random companies from REAL_COMPANIES
        2. Publish them to company queue
        3. Real scraper scrapes actual websites, publishes jobs to jobs queue
        4. Job manager drains jobs queue, filters, inserts to mock DB
        5. Verify jobs are in DB

        RabbitMQ is REAL, HTTP scraping is REAL, DB is MOCKED (in-memory).
        """
        # Pick 2 random companies
        test_companies = get_random_companies(2)

        metrics = {
            "companies_published": 0,
            "companies_processed": 0,
            "jobs_scraped": 0,
            "jobs_filtered_out": {},
            "jobs_valid": 0,
            "jobs_inserted": 0,
        }

        # ========================================
        # Step 1: Publish test companies to queue
        # ========================================
        company_queue = CompanyQueue(rabbitmq)

        for company in test_companies:
            await company_queue.publish(company)
            metrics["companies_published"] += 1
            logger.info(f"📤 [Step 1] Published company: {company['company_name']}")

        assert metrics["companies_published"] == 2

        # ========================================
        # Step 2: Real Scraper Worker processes companies
        # ========================================
        real_scraper = RealScraperWorker(
            rabbitmq=RabbitMQConnection(host="localhost", port=5672),
        )

        await real_scraper.run(timeout_seconds=30.0)

        metrics["companies_processed"] = len(real_scraper.companies_processed)
        metrics["jobs_scraped"] = len(real_scraper.jobs_published)

        logger.info(
            f"✅ [Step 2] Scraper processed {metrics['companies_processed']} companies, "
            f"scraped {metrics['jobs_scraped']} jobs"
        )

        assert metrics["companies_processed"] == 2, "Both companies should be processed"
        assert metrics["jobs_scraped"] > 0, "Should have scraped some jobs"

        # ========================================
        # Step 3: Job Manager drains queue and filters
        # ========================================
        job_queue = JobQueue(rabbitmq)

        # Drain all jobs from queue
        logger.info("📥 [Step 3] Draining jobs from queue...")
        all_jobs = await job_queue.drain_all(timeout_seconds=5.0)

        logger.info(f"📥 [Step 3] Drained {len(all_jobs)} jobs from queue")
        assert len(all_jobs) > 0, "Should have jobs in the queue"

        # Filter jobs (same as JobManager)
        valid_jobs, filter_counts = JobFilter.filter_valid_jobs(all_jobs)
        metrics["jobs_filtered_out"] = filter_counts
        metrics["jobs_valid"] = len(valid_jobs)

        logger.info(f"✅ [Step 3] Filtered: {len(valid_jobs)} valid, removed: {filter_counts}")

        # ========================================
        # Step 4: Insert valid jobs to mock DB
        # ========================================
        if valid_jobs:
            logger.info("📥 [Step 4] Inserting valid jobs to mock DB...")

            # Create mock job persister with mock DBs
            mock_embedder = MagicMock()
            mock_embedder.create_embedding_batch = AsyncMock(return_value="mock-batch-123")

            job_persister = JobPersister(
                jobs_db=mock_jobs_db,
                pending_db=mock_pending_db,
                embedder=mock_embedder,
            )

            # Persist jobs (without embeddings - that's a different test)
            jobs_for_embedding = await job_persister.persist_jobs(valid_jobs)
            metrics["jobs_inserted"] = len(jobs_for_embedding)

            logger.info(f"✅ [Step 4] Inserted {metrics['jobs_inserted']} jobs to mock DB")
        else:
            logger.info("⚠️ [Step 4] No valid jobs to insert (all filtered out)")

        # ========================================
        # Print detailed metrics report
        # ========================================
        print("\n" + "=" * 70)
        print("🎉 REAL SCRAPER → JOB MANAGER FLOW TEST COMPLETE")
        print("=" * 70)
        print("🏢 COMPANIES TESTED:")
        for c in test_companies:
            print(f"   - {c['company_name']}: {c['company_page_url']}")
        print("-" * 70)
        print("📊 METRICS:")
        print(f"   Companies published:.............. {metrics['companies_published']}")
        print(f"   Companies processed by scraper:... {metrics['companies_processed']}")
        print(f"   Jobs scraped (total):............. {metrics['jobs_scraped']}")
        print(f"   Jobs filtered out:................ {sum(metrics['jobs_filtered_out'].values())}")
        print(f"     - Hebrew:...................... {metrics['jobs_filtered_out'].get('hebrew', 0)}")
        print(f"     - Not in Israel:............... {metrics['jobs_filtered_out'].get('job_not_in_israel', 0)}")
        print(f"     - General department:.......... {metrics['jobs_filtered_out'].get('general_department', 0)}")
        print(f"   Jobs valid after filter:.......... {metrics['jobs_valid']}")
        print(f"   Jobs inserted to DB:.............. {metrics['jobs_inserted']}")
        print("-" * 70)
        print("🔌 INTEGRATIONS:")
        print("   RabbitMQ:......................... ✅ REAL")
        print("   HTTP Scraping:.................... ✅ REAL")
        print("   Database:......................... 🔸 MOCKED (in-memory)")
        print("   Embeddings:....................... ⏭️  SKIPPED (not part of this test)")
        print("-" * 70)
        if mock_jobs_db.jobs:
            print("✅ SAMPLE JOBS IN MOCK DB:")
            for i, (_, job) in enumerate(list(mock_jobs_db.jobs.items())[:5]):
                print(f"   {i + 1}. {job.get('title', 'N/A')} @ {job.get('company_name', 'N/A')}")
            if len(mock_jobs_db.jobs) > 5:
                print(f"   ... and {len(mock_jobs_db.jobs) - 5} more")
        print("=" * 70 + "\n")

        # Errors report
        if real_scraper.scrape_errors:
            print("⚠️ SCRAPE ERRORS:")
            for err in real_scraper.scrape_errors:
                print(f"   - {err['company']}: {err['error']}")
            print()

    async def test_single_company_scrape_flow(self, rabbitmq):
        """
        Test scraping a single random company and verifying jobs reach the queue.

        This tests the first half of the flow in isolation:
        Company Queue → Real Scraper → Jobs Queue
        """
        # Pick 1 random company
        test_companies = get_random_companies(1)
        test_company = test_companies[0]

        company_queue = CompanyQueue(rabbitmq)
        job_queue = JobQueue(rabbitmq)

        # Publish company
        await company_queue.publish(test_company)
        logger.info(f"📤 Published company: {test_company['company_name']}")

        # Run real scraper
        real_scraper = RealScraperWorker(
            rabbitmq=RabbitMQConnection(host="localhost", port=5672),
        )
        await real_scraper.run(timeout_seconds=20.0)

        # Verify jobs were published to jobs queue
        jobs = await job_queue.drain_all(timeout_seconds=5.0)

        print(f"\n✅ Scraped {test_company['company_name']}: {len(jobs)} jobs published to queue")
        for i, job in enumerate(jobs[:5]):
            print(f"   {i + 1}. {job.get('title', 'N/A')} - {job.get('location', 'N/A')}")
        if len(jobs) > 5:
            print(f"   ... and {len(jobs) - 5} more")

        assert len(real_scraper.companies_processed) == 1, "Should process 1 company"
        # Don't assert on job count - company might have 0 jobs

    async def test_job_filter_logic(self, rabbitmq):
        """
        Test that the job filter correctly removes invalid jobs.

        Tests filtering logic in isolation (no scraping, just filter logic):
        - Hebrew jobs
        - Jobs not in Israel
        - General department jobs
        """
        # Create a mix of valid and invalid jobs
        test_jobs = [
            # Valid job
            {
                "title": "Valid Engineer",
                "url": "https://example.com/valid",
                "location": "Tel Aviv, ISRAEL",
                "department": "Engineering",
            },
            # Hebrew job (should be filtered)
            {
                "title": "מפתח תוכנה",
                "url": "https://example.com/hebrew",
                "location": "Tel Aviv, ISRAEL",
                "department": "Engineering",
            },
            # Not in Israel (should be filtered)
            {
                "title": "US Engineer",
                "url": "https://example.com/us",
                "location": "San Francisco, USA",
                "department": "Engineering",
            },
            # General department (should be filtered)
            {
                "title": "General Role",
                "url": "https://example.com/general",
                "location": "Haifa, ISRAEL",
                "department": "General",
            },
        ]

        valid_jobs, filter_counts = JobFilter.filter_valid_jobs(test_jobs)

        assert len(valid_jobs) == 1, "Only 1 job should pass filters"
        assert valid_jobs[0]["title"] == "Valid Engineer"
        assert filter_counts["hebrew"] == 1
        assert filter_counts["job_not_in_israel"] == 1
        assert filter_counts["general_department"] == 1

        print(f"\n✅ Filter test passed: 1 valid, 3 filtered ({filter_counts})")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rabbitmq_available():
    """Quick test to verify RabbitMQ is running and accessible."""
    rabbitmq = RabbitMQConnection(host="localhost", port=5672)

    try:
        await rabbitmq.connect()
        assert rabbitmq.connection is not None
        assert rabbitmq.channel is not None
        print("\n✅ RabbitMQ is available at localhost:5672")
    finally:
        await rabbitmq.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_direct_scrape_no_queue():
    """
    Test direct scraping without queues - useful for debugging scraper issues.

    Picks 1 random company and scrapes it directly, showing the results.
    """
    test_companies = get_random_companies(1)
    company = test_companies[0]

    print(f"\n🌐 Directly scraping: {company['company_name']}")
    print(f"   URL: {company['company_page_url']}")

    async with httpx.AsyncClient() as client:
        try:
            html = await fetch_html_from_url(company["company_page_url"], client)
            print(f"   HTML size: {len(html)} chars")

            scraper = JobScraper(html)
            raw_jobs = scraper.extract_jobs()
            print(f"   Raw jobs extracted: {len(raw_jobs)}")

            if raw_jobs:
                valid_jobs, invalid_jobs = validate_jobs(raw_jobs)
                print(f"   Valid jobs: {len(valid_jobs)}, Invalid: {len(invalid_jobs)}")

                if valid_jobs:
                    print("\n   📋 First 3 valid jobs:")
                    for i, job in enumerate(valid_jobs[:3]):
                        print(f"      {i + 1}. {job.get('title')} - {job.get('location')}")

                if invalid_jobs:
                    print("\n   ❌ Invalid job errors:")
                    for inv in invalid_jobs[:3]:
                        print(f"      - {inv['error'][:80]}...")

        except Exception as e:
            print(f"   ❌ Error: {e}")
            raise
