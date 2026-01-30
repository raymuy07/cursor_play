from __future__ import annotations

import asyncio
import logging

from app.common.utils import setup_logging
from app.core.db_utils import JobsDB
from app.services.message_queue import RabbitMQConnection
from app.services.scrape_coordinator import ScraperCoordinator

logger = logging.getLogger("app.workers")


"""
Scraper Worker - consumes company queue, scrapes jobs, publishes to job queue.
Run with: python -m app.workers.scraper_worker
"""


async def main():
    """Entry point for the job scraper service."""

    rabbitmq = RabbitMQConnection()
    jobs_db = JobsDB()

    await jobs_db.connect()
    await rabbitmq.connect()
    logger.info("Connected to RabbitMQ and JobsDB")

    coordinator = ScraperCoordinator(rabbitmq=rabbitmq, jobs_db=jobs_db, num_workers=5, prefetch=10)

    try:
        await coordinator.run()
    finally:
        await rabbitmq.close()
        await jobs_db.close()


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main(), debug=True)
