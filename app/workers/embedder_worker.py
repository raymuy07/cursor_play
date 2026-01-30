import asyncio
from app.common.utils import setup_logging
from app.services.message_queue import RabbitMQConnection
from app.core.db_utils import JobsDB, PendingEmbeddedDB

import logging


logger = logging.getLogger("app.workers")

    ##and here we want to persist the batch_id into the pending embedded db (which is not yet exist)


async def filter_consumer():
    rabbitmq = RabbitMQConnection()
    logger.debug(f"Connecting to RabbitMQ at {rabbitmq.host}:{rabbitmq.port}")
    await rabbitmq.connect()
    logger.info("Connected to RabbitMQ successfully")

    job_queue = JobQueue(rabbitmq)
    pending_db_lib = PendingEmbeddedDB()

    # Open persistent connection for the life of the worker
    async with aiosqlite.connect(pending_db_lib.db_path) as db:
        # Pass the open connection and the library instance via partial
        callback = partial(filter_embedder_batch_call, pending_db_lib=pending_db_lib, db=db)

        logger.info("Filter consumer worker started, waiting for raw jobs...")
        await job_queue.consume(callback, prefetch=10)


async def main():
    rabbitmq = RabbitMQConnection()
    jobs_db = JobsDB()
    pending_db_lib = PendingEmbeddedDB()

    consumer = FilterConsumer(rabbitmq=rabbitmq, jobs_db=jobs_db, pending_db_lib=pending_db_lib)



if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main(), debug=True)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")

