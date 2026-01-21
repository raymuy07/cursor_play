"""
End-to-end pipeline integration tests.
Tests the full flow: CompanyManager → JobScraper → JobFilterEmbedder

Run with: pytest -m integration tests/test_pipeline_integration.py -v -s --log-cli-level=INFO
Requires RabbitMQ running on localhost:5672
"""

import asyncio
import contextlib
import logging
import time

import httpx
import pytest

from scripts.job_filter_embedder import JobFilter
from scripts.job_scraper import JobScraper, fetch_html_from_url
from scripts.message_queue import CompanyQueue, JobQueue, RabbitMQConnection

logger = logging.getLogger(__name__)

# Real companies to test against (known working Comeet pages)
TEST_COMPANIES = [
    {"company_name": "Flare", "domain": "comeet.com", "job_page_url": "https://www.comeet.com/jobs/flare/36.00F"},
    # Add more as backup in case one goes offline
]


@pytest.mark.integration
@pytest.mark.asyncio
class TestPipelineIntegration:
    """End-to-end integration tests for the job scraping pipeline."""

    @pytest.fixture
    async def rabbitmq(self):
        """Create a RabbitMQ connection for tests."""
        conn = RabbitMQConnection(host="localhost", port=5672)
        await conn.connect()
        yield conn
        await conn.close()

    @pytest.fixture
    async def http_client(self):
        """Create an HTTP client for scraping."""
        async with httpx.AsyncClient() as client:
            yield client

    async def test_scrape_real_company_and_extract_jobs(self, http_client):
        """Test that we can scrape a real company page and extract jobs."""
        company = TEST_COMPANIES[0]

        html = await fetch_html_from_url(company["job_page_url"], http_client)

        assert html is not None, f"Failed to fetch HTML from {company['job_page_url']}"
        assert len(html) > 1000, "HTML content seems too short"

        scraper = JobScraper(html)
        jobs = scraper.extract_jobs()

        # We expect at least some jobs (or 0 if company has no openings)
        assert isinstance(jobs, list), "Jobs should be a list"

        if jobs:
            # Validate job structure
            job = jobs[0]
            assert "title" in job, "Job should have a title"
            assert "url" in job, "Job should have a URL"
            print(f"✅ Extracted {len(jobs)} jobs from {company['company_name']}")

    async def test_job_filter_filters_correctly(self, http_client):
        """Test that JobFilter correctly filters jobs."""
        company = TEST_COMPANIES[0]

        html = await fetch_html_from_url(company["job_page_url"], http_client)
        scraper = JobScraper(html)
        jobs = scraper.extract_jobs()

        if not jobs:
            pytest.skip("No jobs to filter from test company")

        valid_jobs, filter_counts = JobFilter.filter_valid_jobs(jobs)

        # Validate filter output
        assert isinstance(valid_jobs, list)
        assert isinstance(filter_counts, dict)
        assert "hebrew" in filter_counts
        assert "job_not_in_israel" in filter_counts

        total_filtered = sum(filter_counts.values())
        assert len(valid_jobs) + total_filtered == len(jobs), "Jobs should be either valid or filtered"

        print(f"✅ Filtered: {len(valid_jobs)} valid, {filter_counts}")

    async def test_full_pipeline_company_to_queue_to_filter(self, rabbitmq, http_client):
        """
        Full E2E test: Publish company → Scrape → Publish jobs → Consume & Filter

        This tests the actual queue message flow between components.
        """
        metrics = {
            "timings": {},
            "counts": {},
        }
        pipeline_start = time.perf_counter()

        company_queue = CompanyQueue(rabbitmq)
        job_queue = JobQueue(rabbitmq)

        test_company = TEST_COMPANIES[0]

        # Step 1: Simulate CompanyManager publishing a company
        step_start = time.perf_counter()
        await company_queue.publish(test_company)
        metrics["timings"]["publish_company"] = time.perf_counter() - step_start
        logger.info(
            f"📤 Published company: {test_company['company_name']} ({metrics['timings']['publish_company']:.3f}s)"
        )

        # Step 2: Simulate JobScraper consuming and processing
        scraped_jobs = []
        html_size = 0

        async def scraper_callback(company: dict):
            nonlocal html_size
            """Simulates job_scraper.process_company"""
            fetch_start = time.perf_counter()
            html = await fetch_html_from_url(company["job_page_url"], http_client)
            metrics["timings"]["fetch_html"] = time.perf_counter() - fetch_start

            if html:
                html_size = len(html)
                parse_start = time.perf_counter()
                scraper = JobScraper(html)
                jobs = scraper.extract_jobs()
                metrics["timings"]["parse_jobs"] = time.perf_counter() - parse_start

                if jobs:
                    scraped_jobs.extend(jobs)
                    # Publish to job queue (like the real scraper does)
                    publish_start = time.perf_counter()
                    await job_queue.publish_batch(jobs, company["job_page_url"])
                    metrics["timings"]["publish_jobs"] = time.perf_counter() - publish_start
                    logger.info(f"📤 Published {len(jobs)} jobs to job queue")
            raise asyncio.CancelledError()  # Exit after one message

        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(company_queue.consume(scraper_callback, prefetch=1), timeout=30.0)

        assert len(scraped_jobs) > 0, "Should have scraped some jobs"
        metrics["counts"]["scraped_jobs"] = len(scraped_jobs)
        metrics["counts"]["html_size_kb"] = html_size / 1024

        # Step 3: Simulate FilterEmbedder consuming job batches
        filtered_results = {"valid": [], "counts": {}}

        async def filter_callback(jobs_data: dict):
            """Simulates job_filter_embedder.filter_embedder_batch_call"""
            jobs = jobs_data.get("jobs", [])
            source = jobs_data.get("source_url", "")

            filter_start = time.perf_counter()
            valid_jobs, filter_counts = JobFilter.filter_valid_jobs(jobs)
            metrics["timings"]["filter_jobs"] = time.perf_counter() - filter_start

            filtered_results["valid"] = valid_jobs
            filtered_results["counts"] = filter_counts

            logger.info(f"✅ Filtered batch from {source}: {len(valid_jobs)} valid")
            raise asyncio.CancelledError()

        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(job_queue.consume(filter_callback, prefetch=1), timeout=10.0)

        # Verify end-to-end success
        total_processed = len(filtered_results["valid"]) + sum(filtered_results["counts"].values())
        assert total_processed == len(scraped_jobs), "All scraped jobs should be processed by filter"

        metrics["timings"]["total_pipeline"] = time.perf_counter() - pipeline_start
        metrics["counts"]["valid_jobs"] = len(filtered_results["valid"])
        metrics["counts"]["filter_breakdown"] = filtered_results["counts"]

        # Print detailed metrics report
        print("\n" + "=" * 60)
        print("🎉 PIPELINE TEST METRICS")
        print("=" * 60)
        print(f"Company: {test_company['company_name']}")
        print(f"URL: {test_company['job_page_url']}")
        print("-" * 60)
        print("⏱️  TIMING BREAKDOWN:")
        for step, duration in metrics["timings"].items():
            print(f"   {step:.<30} {duration:.3f}s")
        print("-" * 60)
        print("📊 COUNTS:")
        print(f"   HTML size:................ {metrics['counts']['html_size_kb']:.1f} KB")
        print(f"   Jobs scraped:............. {metrics['counts']['scraped_jobs']}")
        print(f"   Jobs valid after filter:.. {metrics['counts']['valid_jobs']}")
        print(f"   Filter breakdown:......... {metrics['counts']['filter_breakdown']}")
        print("=" * 60 + "\n")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rabbitmq_connection():
    """Basic test to verify RabbitMQ is accessible."""
    rabbitmq = RabbitMQConnection(host="localhost", port=5672)

    try:
        await rabbitmq.connect()
        assert rabbitmq.connection is not None
        assert rabbitmq.channel is not None
        print("✅ RabbitMQ connection successful")
    finally:
        await rabbitmq.close()
