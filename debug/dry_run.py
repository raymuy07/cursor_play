#!/usr/bin/env python3
"""
Dry run scraper - tests job scraping on 10 fixed URLs without RabbitMQ.
Run with F5 using "Dry Run Scraper" launch config, or: python debug/dry_run.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.common.utils import setup_logging
from app.services.job_scraper import JobScraper, fetch_html_from_url

logger = logging.getLogger(__name__)

# 10 fixed test URLs - replace with your own
TEST_URLS = [
    {"name": "Aidoc", "url": "https://www.comeet.com/jobs/Aidoc/B4.007"},
    {"name": "Mend", "url": "https://www.comeet.com/jobs/mend/83.000"},
    {"name": "Feedvisor", "url": "https://www.comeet.com/jobs/feedvisor/11.00A"},
    {"name": "SysAid", "url": "https://www.comeet.com/jobs/sysaid/43.00A"},
    {"name": "Justt", "url": "https://www.comeet.com/jobs/justt/36.001"},
    {"name": "BioCatch", "url": "https://www.comeet.com/jobs/biocatch/03.00E"},
    {"name": "IRONSCALES", "url": "https://www.comeet.com/jobs/ironscales/1A.007"},
    {"name": "Penlink", "url": "https://www.comeet.com/jobs/penlink/E5.002"},
    {"name": "Accelerated Digital Media", "url": "https://www.comeet.com/jobs/accelerateddigitalmedia/49.00D"},
    {"name": "Test Company", "url": "https://www.comeet.com/jobs/testcompany/TEST.001"},
]


async def scrape_url(name: str, url: str, client: httpx.AsyncClient) -> dict:
    """Scrape a single URL and return results."""
    result = {"name": name, "url": url, "success": False, "jobs": 0, "error": None}

    try:
        logger.info(f"Fetching: {name}")
        html = await fetch_html_from_url(url, client=client)

        if not html:
            result["error"] = "No HTML"
            return result

        scraper = JobScraper(html)
        jobs = scraper.extract_jobs()
        result["jobs"] = len(jobs)
        result["success"] = True

        if jobs:
            logger.info(f"  ✓ {len(jobs)} jobs found")
            for job in jobs[:3]:
                logger.info(f"    - {job.get('title', 'N/A')}")
            if len(jobs) > 3:
                logger.info(f"    ... +{len(jobs) - 3} more")
        else:
            logger.warning("  ✗ No jobs found")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"  ✗ Error: {e}")

    return result


async def main():
    if not TEST_URLS:
        logger.error("No URLs configured! Add URLs to TEST_URLS list in debug/dry_run.py")
        return

    logger.info(f"Dry run: testing {len(TEST_URLS)} URLs\n")

    results = []
    async with httpx.AsyncClient() as client:
        for item in TEST_URLS:
            result = await scrape_url(item["name"], item["url"], client)
            results.append(result)
            await asyncio.sleep(1)  # Be polite

    # Summary
    success = sum(1 for r in results if r["success"])
    total_jobs = sum(r["jobs"] for r in results)

    logger.info(f"\n{'=' * 40}")
    logger.info(f"Done: {success}/{len(TEST_URLS)} succeeded, {total_jobs} total jobs")

    if any(r["error"] for r in results):
        logger.info("\nErrors:")
        for r in results:
            if r["error"]:
                logger.info(f"  {r['name']}: {r['error']}")


if __name__ == "__main__":
    setup_logging(level=logging.INFO)
    asyncio.run(main())
