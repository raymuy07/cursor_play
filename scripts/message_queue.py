import json
import logging
from typing import Dict, Optional, Callable, Any
import aio_pika

logger = logging.getLogger(__name__)

# Queue names
COMPANIES_QUEUE = "companies_to_scrape"
JOBS_QUEUE = "jobs_to_persist"

class RabbitMQConnection:
    """Manages RabbitMQ connection and channel lifecycle using aio-pika."""

    def __init__(self, host: str = "localhost", port: int = 5672):
        self.host = host
        self.port = port
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None

    async def connect(self):
        """Establish a robust connection and channel."""
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(
                host=self.host,
                port=self.port
            )

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

    async def publish(self, company: Dict):
        """Push a company to the scrape queue."""
        await self._ensure_connected()
        message = aio_pika.Message(
            body=json.dumps(company).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await self.rabbitmq.channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        logger.debug(f"Queued company: {company.get('company_name')}")

    async def consume(self, callback: Callable[[Dict], Any], prefetch: int = 10):
        """
        Consume companies from queue.
        Callback should be an async function.
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
                        # message.process() context manager will handle nack if exception is raised
                        # but we might want to log it specifically.
                        raise

class JobQueue(BaseQueue):
    """Producer/Consumer for job persistence queue."""

    def __init__(self, rabbitmq: RabbitMQConnection):
        super().__init__(rabbitmq, JOBS_QUEUE)

    async def publish_batch(self, jobs: list, source_url: str):
        """Push a batch of jobs to the persist queue."""
        await self._ensure_connected()
        payload = {"jobs": jobs, "source_url": source_url}
        message = aio_pika.Message(
            body=json.dumps(payload, default=str).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await self.rabbitmq.channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        logger.info(f"Queued {len(jobs)} jobs from {source_url}")

    async def consume(self, callback: Callable[[list, str], Any], prefetch: int = 1):
        """Consume job batches from queue."""
        await self._ensure_connected()
        await self.rabbitmq.channel.set_qos(prefetch_count=prefetch)

        queue = await self.rabbitmq.channel.get_queue(self.queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    try:
                        await callback(data["jobs"], data["source_url"])
                    except Exception as e:
                        logger.error(f"Error persisting jobs: {e}")
                        raise
