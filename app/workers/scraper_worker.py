from __future__ import annotations

import asyncio
import logging

from app.common.utils import setup_logging
from app.core.scrape_coordinator import ScraperCoordinator
from app.services.message_queue import RabbitMQConnection

logger = logging.getLogger("app.workers")


"""
Scraper Worker - consumes company queue, scrapes jobs, publishes to job queue.
Run with: python -m app.workers.scraper_worker
"""


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
