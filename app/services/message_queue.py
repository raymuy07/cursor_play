from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

import aio_pika

logger = logging.getLogger(__name__)

# Queue names
COMPANIES_QUEUE = "companies_to_scrape"
JOBS_QUEUE = "jobs_to_persist"


@dataclass
class QueueItem:
    """Wraps a message with its data and ack/nack callbacks."""

    data: dict
    message: aio_pika.IncomingMessage


class RabbitMQConnection:
    """Manages RabbitMQ connection and channel lifecycle using aio-pika."""

    def __init__(self, host: str = "localhost", port: int = 5672):
        self.host = host
        self.port = port
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.RobustChannel | None = None

    async def connect(self):
        """Establish a robust connection and channel."""
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(host=self.host, port=self.port)

        if not self.channel or self.channel.is_closed:
            self.channel = await self.connection.channel()
            # Default prefetch; can be overridden in consumers
            await self.channel.set_qos(prefetch_count=10)

        # Declare queues to ensure they exist
        await self.channel.declare_queue(COMPANIES_QUEUE, durable=True)
        await self.channel.declare_queue(JOBS_QUEUE, durable=True)

    async def close(self):
        """Close connection and channel."""
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()


class BaseQueue:
    """Base class for RabbitMQ queues."""

    def __init__(self, rabbitmq: RabbitMQConnection, queue_name: str):
        self.rabbitmq = rabbitmq
        self.queue_name = queue_name

    async def _ensure_connected(self):
        if not self.rabbitmq.channel or self.rabbitmq.channel.is_closed:
            await self.rabbitmq.connect()


class CompanyQueue(BaseQueue):
    """Producer/Consumer for company scraping queue."""

    def __init__(self, rabbitmq: RabbitMQConnection):
        super().__init__(rabbitmq, COMPANIES_QUEUE)

    async def publish(self, company: dict):
        """Push a company to the scrape queue."""
        await self._ensure_connected()
        message = aio_pika.Message(body=json.dumps(company).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.rabbitmq.channel.default_exchange.publish(message, routing_key=self.queue_name)
        logger.debug(f"Queued company: {company.get('company_name')}")

    async def feed_queue(
        self,
        internal_queue: asyncio.Queue[QueueItem],
        prefetch: int = 10,
    ):
        """
        Feed messages from RabbitMQ into an internal asyncio.Queue.
        This allows workers to process messages concurrently.
        Messages are NOT auto-acked - workers must call message.ack() after processing.
        """
        await self._ensure_connected()
        await self.rabbitmq.channel.set_qos(prefetch_count=prefetch)

        queue = await self.rabbitmq.channel.get_queue(self.queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                company = json.loads(message.body.decode())
                item = QueueItem(data=company, message=message)
                await internal_queue.put(item)

    async def consume(self, callback: Callable[[dict], Coroutine[Any, Any, None]], prefetch: int = 10):
        """
        Consume companies from queue sequentially (legacy method).
        For concurrent processing, use ScraperCoordinator instead.
        """
        await self._ensure_connected()
        await self.rabbitmq.channel.set_qos(prefetch_count=prefetch)

        queue = await self.rabbitmq.channel.get_queue(self.queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    company = json.loads(message.body.decode())
                    try:
                        await callback(company)
                    except Exception as e:
                        logger.error(f"Error processing company: {e}")
                        raise


class JobQueue(BaseQueue):
    """Producer/Consumer for job persistence queue."""

    def __init__(self, rabbitmq: RabbitMQConnection):
        super().__init__(rabbitmq, JOBS_QUEUE)

    async def publish_jobs_from_url(self, jobs: list, source_url: str):
        """Push a batch of jobs to the persist queue."""
        await self._ensure_connected()
        payload = {"jobs": jobs, "source_url": source_url}
        message = aio_pika.Message(
            body=json.dumps(payload, default=str).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await self.rabbitmq.channel.default_exchange.publish(message, routing_key=self.queue_name)
        logger.info(f"Queued {len(jobs)} jobs from {source_url}")

    async def consume(self, callback: callable[[dict], None], prefetch: int = 1):
        """Consume job batches from queue."""
        await self._ensure_connected()
        await self.rabbitmq.channel.set_qos(prefetch_count=prefetch)

        queue = await self.rabbitmq.channel.get_queue(self.queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    jobs_data = json.loads(message.body.decode())
                    try:
                        await callback(jobs_data)
                    except Exception as e:
                        logger.error(f"Error persisting jobs: {e}")
                        raise
