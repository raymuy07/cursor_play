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
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.services.job_filter_embedder import JobFilter
from app.services.job_scraper import JobScraper, fetch_html_from_url
from app.services.message_queue import CompanyQueue, JobQueue, RabbitMQConnection

logger = logging.getLogger(__name__)

# Real companies to test against (known working Comeet pages)
TEST_COMPANIES = [
    {"company_name": "Flare", "domain": "comeet.com", "company_page_url": "https://www.comeet.com/jobs/flare/36.00F"},
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

        html = await fetch_html_from_url(company["company_page_url"], http_client)

        assert html is not None, f"Failed to fetch HTML from {company['company_page_url']}"
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

        html = await fetch_html_from_url(company["company_page_url"], http_client)
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

    async def test_full_pipeline_company_to_queue_to_persister(self, rabbitmq, http_client):
        """
        Full E2E test: Company → Scrape → Filter → Embed (mocked) → Persist (placeholder)

        Pipeline flow:
        1. CompanyManager publishes company to companies_queue
        2. JobScraper consumes, scrapes HTML, publishes jobs to jobs_queue
        3. FilterEmbedder consumes, filters jobs, calls TextEmbedder (MOCKED)
        4. Persister saves to DB (PLACEHOLDER)

        RabbitMQ is REAL, OpenAI API is MOCKED.
        """
        from app.common.txt_embedder import TextEmbedder

        metrics = {
            "timings": {},
            "counts": {},
        }
        pipeline_start = time.perf_counter()

        company_queue = CompanyQueue(rabbitmq)
        job_queue = JobQueue(rabbitmq)

        test_company = TEST_COMPANIES[0]

        # ========================================
        # Step 1: CompanyManager publishes company
        # ========================================
        step_start = time.perf_counter()
        await company_queue.publish(test_company)
        metrics["timings"]["1_publish_company"] = time.perf_counter() - step_start
        logger.info(f"📤 [Step 1] Published company: {test_company['company_name']}")

        # ========================================
        # Step 2: JobScraper consumes and scrapes
        # ========================================
        scraped_jobs = []
        html_size = 0

        async def scraper_callback(company: dict):
            nonlocal html_size
            """Simulates job_scraper.process_company"""
            fetch_start = time.perf_counter()
            html = await fetch_html_from_url(company["company_page_url"], http_client)
            metrics["timings"]["2_fetch_html"] = time.perf_counter() - fetch_start

            if html:
                html_size = len(html)
                parse_start = time.perf_counter()
                scraper = JobScraper(html)
                jobs = scraper.extract_jobs()
                metrics["timings"]["2_parse_jobs"] = time.perf_counter() - parse_start

                if jobs:
                    scraped_jobs.extend(jobs)
                    publish_start = time.perf_counter()
                    await job_queue.publish_jobs_from_url(jobs, company["company_page_url"])
                    metrics["timings"]["2_publish_jobs"] = time.perf_counter() - publish_start
                    logger.info(f"📤 [Step 2] Scraped {len(jobs)} jobs, published to jobs_queue")
            raise asyncio.CancelledError()

        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(company_queue.consume(scraper_callback, prefetch=1), timeout=30.0)

        assert len(scraped_jobs) > 0, "Should have scraped some jobs"
        metrics["counts"]["scraped_jobs"] = len(scraped_jobs)
        metrics["counts"]["html_size_kb"] = html_size / 1024

        # ========================================
        # Step 3: FilterEmbedder consumes, filters, and embeds (MOCKED)
        # ========================================
        filtered_results = {"valid": [], "counts": {}, "batch_id": None}

        # Create embedder and mock the OpenAI API calls
        embedder = TextEmbedder()

        with (
            patch.object(embedder.client.files, "create", new_callable=AsyncMock) as mock_file_create,
            patch.object(embedder.client.batches, "create", new_callable=AsyncMock) as mock_batch_create,
        ):
            mock_file_create.return_value = MagicMock(id="file-test123")
            mock_batch_create.return_value = MagicMock(id="batch-test456")

            async def filter_embed_callback(jobs_data: dict):
                """Simulates job_filter_embedder.filter_embedder_batch_call with mocked embedder"""
                jobs = jobs_data.get("jobs", [])
                source = jobs_data.get("source_url", "")

                # Filter jobs
                filter_start = time.perf_counter()
                valid_jobs, filter_counts = JobFilter.filter_valid_jobs(jobs)
                metrics["timings"]["3_filter_jobs"] = time.perf_counter() - filter_start

                filtered_results["valid"] = valid_jobs
                filtered_results["counts"] = filter_counts

                logger.info(f"✅ [Step 3] Filtered: {len(valid_jobs)} valid from {source}")

                # Embed valid jobs (API is mocked - $0 cost)
                if valid_jobs:
                    # Prepare job descriptions for embedding
                    job_texts = []
                    for job in valid_jobs:
                        desc = job.get("description", {})
                        if isinstance(desc, dict):
                            desc = " ".join(str(v) for v in desc.values() if v)
                        job_texts.append(f"{job.get('title', '')} - {desc[:500]}")

                    embed_start = time.perf_counter()
                    batch_id = await embedder.create_embedding_batch(job_texts)
                    metrics["timings"]["3_create_embedding_batch"] = time.perf_counter() - embed_start
                    filtered_results["batch_id"] = batch_id
                    logger.info(f"📤 [Step 3] Created embedding batch: {batch_id} (MOCKED)")

                raise asyncio.CancelledError()

            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(job_queue.consume(filter_embed_callback, prefetch=1), timeout=10.0)

            # Verify embedder was called correctly
            if filtered_results["valid"]:
                mock_file_create.assert_called_once()
                mock_batch_create.assert_called_once()
                assert filtered_results["batch_id"] == "batch-test456"

        # ========================================
        # Step 4: Persister (PLACEHOLDER)
        # ========================================
        # TODO: Implement when JobPersister is ready
        # This would:
        # 1. Poll for batch completion (or mock immediate completion)
        # 2. Retrieve embeddings from batch results
        # 3. Save jobs + embeddings to jobs.db
        persister_start = time.perf_counter()
        jobs_to_persist = filtered_results["valid"]
        batch_id = filtered_results["batch_id"]

        # Placeholder: In real implementation, this would call JobPersister
        logger.info(f"📥 [Step 4] PLACEHOLDER: Would persist {len(jobs_to_persist)} jobs with batch_id={batch_id}")
        metrics["timings"]["4_persist_placeholder"] = time.perf_counter() - persister_start
        metrics["counts"]["jobs_to_persist"] = len(jobs_to_persist)

        # ========================================
        # Verify end-to-end success
        # ========================================
        total_processed = len(filtered_results["valid"]) + sum(filtered_results["counts"].values())
        assert total_processed == len(scraped_jobs), "All scraped jobs should be processed by filter"

        metrics["timings"]["total_pipeline"] = time.perf_counter() - pipeline_start
        metrics["counts"]["valid_jobs"] = len(filtered_results["valid"])
        metrics["counts"]["filter_breakdown"] = filtered_results["counts"]

        # Print detailed metrics report
        print("\n" + "=" * 70)
        print("🎉 FULL PIPELINE TEST METRICS")
        print("=" * 70)
        print(f"Company: {test_company['company_name']}")
        print(f"URL: {test_company['company_page_url']}")
        print("-" * 70)
        print("⏱️  TIMING BREAKDOWN:")
        for step, duration in sorted(metrics["timings"].items()):
            print(f"   {step:.<40} {duration:.3f}s")
        print("-" * 70)
        print("📊 COUNTS:")
        print(f"   HTML size:........................ {metrics['counts']['html_size_kb']:.1f} KB")
        print(f"   Jobs scraped:..................... {metrics['counts']['scraped_jobs']}")
        print(f"   Jobs valid after filter:.......... {metrics['counts']['valid_jobs']}")
        print(f"   Jobs to persist:.................. {metrics['counts']['jobs_to_persist']}")
        print(f"   Filter breakdown:................. {metrics['counts']['filter_breakdown']}")
        print("-" * 70)
        print("🔌 INTEGRATIONS:")
        print("   RabbitMQ:......................... ✅ REAL")
        print("   HTTP Scraping:.................... ✅ REAL")
        print(f"   OpenAI Embeddings:................ 🔸 MOCKED (batch_id={filtered_results['batch_id']})")
        print("   Database Persist:................. ⏳ PLACEHOLDER")
        print("=" * 70 + "\n")


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_embedder_api_request_structure():
    """
    Test that TextEmbedder builds correct API requests WITHOUT calling OpenAI.

    This validates the entire code path through the embedder:
    - JSONL file creation
    - File upload to OpenAI
    - Batch job creation

    All OpenAI API calls are mocked, so this costs $0.
    """
    from app.common.txt_embedder import TextEmbedder

    embedder = TextEmbedder()

    test_texts = [
        "Software Engineer at TechCorp - Building scalable systems",
        "Product Manager role - Leading cross-functional teams",
        "Data Scientist position - Machine learning expertise required",
    ]

    # Mock the OpenAI client methods
    with (
        patch.object(embedder.client.files, "create", new_callable=AsyncMock) as mock_file_create,
        patch.object(embedder.client.batches, "create", new_callable=AsyncMock) as mock_batch_create,
    ):
        # Setup mock returns
        mock_file_create.return_value = MagicMock(id="file-abc123")
        mock_batch_create.return_value = MagicMock(id="batch-xyz789")

        # This will build the JSONL and call the mocked API
        batch_id = await embedder.create_embedding_batch(test_texts)

        # Verify batch ID returned correctly
        assert batch_id == "batch-xyz789"

        # Verify file upload was called
        mock_file_create.assert_called_once()
        call_kwargs = mock_file_create.call_args.kwargs
        assert call_kwargs["purpose"] == "batch"

        # Verify batch creation was called with correct parameters
        mock_batch_create.assert_called_once_with(
            input_file_id="file-abc123", endpoint="/v1/embeddings", completion_window="24h"
        )

        print("✅ TextEmbedder API request structure verified")
        print(f"   Texts submitted: {len(test_texts)}")
        print(f"   Mock batch ID: {batch_id}")
        print("   No actual API calls made - $0 cost")
