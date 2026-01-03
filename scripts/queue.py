import pika
import json
import logging
from typing import Dict, Optional, Callable
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Queue names
COMPANIES_QUEUE = "companies_to_scrape"
JOBS_QUEUE = "jobs_to_persist"


class RabbitMQConnection:
    """Manages RabbitMQ connection lifecycle."""

    def __init__(self, host: str = "localhost", port: int = 5672):
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None

    def connect(self):
        """Establish connection and declare queues."""
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host, port=self.port)
        )
        self._channel = self._connection.channel()

        # Declare durable queues (survive broker restart)
        self._channel.queue_declare(queue=COMPANIES_QUEUE, durable=True)
        self._channel.queue_declare(queue=JOBS_QUEUE, durable=True)

        return self._channel

    def close(self):
        if self._connection:
            self._connection.close()

    @property
    def channel(self):
        if not self._channel:
            self.connect()
        return self._channel


class CompanyQueue:
    """Producer/Consumer for company scraping queue."""

    def __init__(self, connection: RabbitMQConnection):
        self.connection = connection
        self.queue_name = COMPANIES_QUEUE

    def publish(self, company: Dict):
        """Push a company to the scrape queue."""
        self.connection.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=json.dumps(company),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )
        logger.debug(f"Queued company: {company.get('company_name')}")

    def consume(self, callback: Callable[[Dict], bool], prefetch: int = 1):
        """
        Consume companies from queue.
        Callback should return True on success (ack) or False (nack/requeue).
        """
        channel = self.connection.channel
        channel.basic_qos(prefetch_count=prefetch)

        def on_message(ch, method, properties, body):
            company = json.loads(body)
            try:
                success = callback(company)
                if success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                logger.error(f"Error processing company: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        channel.basic_consume(queue=self.queue_name, on_message_callback=on_message)
        logger.info(f"Waiting for companies on {self.queue_name}...")
        channel.start_consuming()


class JobQueue:
    """Producer/Consumer for job persistence queue."""

    def __init__(self, connection: RabbitMQConnection):
        self.connection = connection
        self.queue_name = JOBS_QUEUE

    def publish_batch(self, jobs: list, source_url: str):
        """Push a batch of jobs to the persist queue."""
        message = {"jobs": jobs, "source_url": source_url}
        self.connection.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=json.dumps(message, default=str),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        logger.info(f"Queued {len(jobs)} jobs from {source_url}")

    def consume(self, callback: Callable[[list, str], bool], prefetch: int = 1):
        """Consume job batches from queue."""
        channel = self.connection.channel
        channel.basic_qos(prefetch_count=prefetch)

        def on_message(ch, method, properties, body):
            data = json.loads(body)
            try:
                success = callback(data["jobs"], data["source_url"])
                if success:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                logger.error(f"Error persisting jobs: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        channel.basic_consume(queue=self.queue_name, on_message_callback=on_message)
        logger.info(f"Waiting for jobs on {self.queue_name}...")
        channel.start_consuming()
