import asyncio
import logging
from functools import partial

from app.common.txt_embedder import TextEmbedder
from app.common.utils import setup_logging
from app.core.db_utils import PendingEmbeddedDB
from app.services.job_embedder import JobEmbedder
from app.services.message_queue import JobQueue, RabbitMQConnection

logger = logging.getLogger("app.workers")


async def main():
    """Entry point for the embedder worker service."""
    rabbitmq = RabbitMQConnection()
    pending_db = PendingEmbeddedDB()
    text_embedder = TextEmbedder()

    # Connect to all resources
    await rabbitmq.connect()
    await pending_db.connect()
    logger.info("Connected to RabbitMQ and PendingEmbeddedDB")

    job_queue = JobQueue(rabbitmq)
    embedder = JobEmbedder(pending_db=pending_db, text_embedder=text_embedder)

    # Create callback with pending_db bound
    callback = partial(embedder.process_batch)

    try:
        logger.info("Embedder worker started, waiting for jobs...")
        await job_queue.consume(callback, prefetch=10)
    finally:
        await pending_db.close()
        await rabbitmq.close()


if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
