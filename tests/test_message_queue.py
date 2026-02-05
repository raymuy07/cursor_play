"""Tests for RabbitMQ message queue functionality."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.message_queue import (
    COMPANIES_QUEUE,
    JOBS_QUEUE,
    BaseQueue,
    CompanyQueue,
    JobQueue,
    QueueItem,
    RabbitMQConnection,
)

# -----------------------------------------------------------------------------
# Unit Tests (mocked, no real RabbitMQ required)
# -----------------------------------------------------------------------------


class TestRabbitMQConnection:
    """Unit tests for RabbitMQConnection class."""

    @pytest.mark.asyncio
    async def test_connect_creates_connection_and_channel(self):
        """Test that connect() establishes connection and channel."""
        rabbitmq = RabbitMQConnection(host="localhost", port=5672)

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False

        with patch("app.core.message_queue.aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_connection
            mock_connection.channel.return_value = mock_channel

            await rabbitmq.connect()

            mock_connect.assert_called_once_with(host="localhost", port=5672)
            mock_connection.channel.assert_called_once()
            mock_channel.set_qos.assert_called_once_with(prefetch_count=10)
            assert mock_channel.declare_queue.call_count == 2

    @pytest.mark.asyncio
    async def test_connect_reuses_existing_connection(self):
        """Test that connect() reuses open connection."""
        rabbitmq = RabbitMQConnection()

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False

        rabbitmq.connection = mock_connection
        rabbitmq.channel = mock_channel

        with patch("app.core.message_queue.aio_pika.connect_robust", new_callable=AsyncMock) as mock_connect:
            await rabbitmq.connect()
            mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_closes_channel_and_connection(self):
        """Test that close() properly closes resources."""
        rabbitmq = RabbitMQConnection()
        rabbitmq.channel = AsyncMock()
        rabbitmq.connection = AsyncMock()

        await rabbitmq.close()

        rabbitmq.channel.close.assert_called_once()
        rabbitmq.connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_none_resources(self):
        """Test that close() handles None channel/connection gracefully."""
        rabbitmq = RabbitMQConnection()
        rabbitmq.channel = None
        rabbitmq.connection = None

        # Should not raise
        await rabbitmq.close()


class TestCompanyQueue:
    """Unit tests for CompanyQueue class."""

    @pytest.fixture
    def mock_rabbitmq(self):
        """Create a mocked RabbitMQ connection."""
        rabbitmq = MagicMock(spec=RabbitMQConnection)
        rabbitmq.channel = AsyncMock()
        rabbitmq.channel.is_closed = False
        rabbitmq.channel.default_exchange = AsyncMock()
        return rabbitmq

    @pytest.mark.asyncio
    async def test_publish_sends_company_to_queue(self, mock_rabbitmq):
        """Test that publish() sends company data to the queue."""
        queue = CompanyQueue(mock_rabbitmq)

        company = {
            "company_name": "Test Corp",
            "domain": "lever",
            "company_page_url": "https://jobs.lever.co/testcorp",
        }

        with patch("app.core.message_queue.aio_pika.Message") as mock_message_class:
            mock_message = MagicMock()
            mock_message_class.return_value = mock_message

            await queue.publish(company)

            # Verify message was created with correct body
            call_args = mock_message_class.call_args
            body = call_args.kwargs["body"]
            assert json.loads(body.decode()) == company

            # Verify message was published
            mock_rabbitmq.channel.default_exchange.publish.assert_called_once()
            publish_call = mock_rabbitmq.channel.default_exchange.publish.call_args
            assert publish_call.kwargs["routing_key"] == COMPANIES_QUEUE

    @pytest.mark.asyncio
    async def test_feed_queue_puts_items_into_internal_queue(self, mock_rabbitmq):
        """Test that feed_queue() puts messages into an asyncio.Queue."""
        queue = CompanyQueue(mock_rabbitmq)

        company_data = {"company_name": "Test Corp", "domain": "lever"}

        # Create mock message
        mock_message = AsyncMock()
        mock_message.body = json.dumps(company_data).encode()

        # Create mock queue iterator that yields one message then stops
        mock_rmq_queue = MagicMock()

        async def mock_iterator():
            yield mock_message
            # After yielding, raise to break the loop for testing
            raise asyncio.CancelledError()

        # queue.iterator() is called synchronously (not awaited) and used as
        # `async with queue.iterator() as queue_iter:`, so iterator must be a
        # regular MagicMock returning an async context manager.
        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_iterator())
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_rmq_queue.iterator.return_value = mock_ctx

        mock_rabbitmq.channel.get_queue = AsyncMock(return_value=mock_rmq_queue)
        mock_rabbitmq.channel.set_qos = AsyncMock()

        # Create internal queue to receive items
        internal_queue: asyncio.Queue[QueueItem] = asyncio.Queue()

        # Run feed_queue (will be cancelled after processing one message)
        try:
            await queue.feed_queue(internal_queue, prefetch=1)
        except asyncio.CancelledError:
            pass

        # Verify item was put into internal queue
        assert not internal_queue.empty()
        item = await internal_queue.get()
        assert item.data == company_data
        assert item.message == mock_message


class TestJobQueue:
    """Unit tests for JobQueue class."""

    @pytest.fixture
    def mock_rabbitmq(self):
        """Create a mocked RabbitMQ connection."""
        rabbitmq = MagicMock(spec=RabbitMQConnection)
        rabbitmq.channel = AsyncMock()
        rabbitmq.channel.is_closed = False
        rabbitmq.channel.default_exchange = AsyncMock()
        return rabbitmq

    @pytest.mark.asyncio
    async def test_publish_batch_sends_jobs_to_queue(self, mock_rabbitmq):
        """Test that publish_batch() sends job batch to the queue."""
        queue = JobQueue(mock_rabbitmq)

        jobs = [
            {"title": "Software Engineer", "url": "https://example.com/job1"},
            {"title": "Product Manager", "url": "https://example.com/job2"},
        ]
        source_url = "https://example.com/careers"

        with patch("app.core.message_queue.aio_pika.Message") as mock_message_class:
            mock_message = MagicMock()
            mock_message_class.return_value = mock_message

            await queue.publish_jobs_from_url(jobs, source_url)

            # Verify message body contains jobs and source_url
            call_args = mock_message_class.call_args
            body = json.loads(call_args.kwargs["body"].decode())
            assert body["jobs"] == jobs
            assert body["source_url"] == source_url

            # Verify message was published
            mock_rabbitmq.channel.default_exchange.publish.assert_called_once()
            publish_call = mock_rabbitmq.channel.default_exchange.publish.call_args
            assert publish_call.kwargs["routing_key"] == JOBS_QUEUE

    @pytest.mark.asyncio
    async def test_publish_batch_handles_datetime_serialization(self, mock_rabbitmq):
        """Test that publish_batch() handles datetime objects via default=str."""
        from datetime import datetime

        queue = JobQueue(mock_rabbitmq)

        jobs = [{"title": "Engineer", "posted_at": datetime(2025, 1, 10)}]
        source_url = "https://example.com/careers"

        with patch("app.core.message_queue.aio_pika.Message") as mock_message_class:
            mock_message = MagicMock()
            mock_message_class.return_value = mock_message

            # Should not raise TypeError due to default=str in json.dumps
            await queue.publish_jobs_from_url(jobs, source_url)

            call_args = mock_message_class.call_args
            body = json.loads(call_args.kwargs["body"].decode())
            assert "2025-01-10" in body["jobs"][0]["posted_at"]


class TestBaseQueue:
    """Unit tests for BaseQueue class."""

    @pytest.mark.asyncio
    async def test_ensure_connected_calls_connect_when_channel_closed(self):
        """Test that _ensure_connected() reconnects when channel is closed."""
        mock_rabbitmq = MagicMock(spec=RabbitMQConnection)
        mock_rabbitmq.channel = None
        mock_rabbitmq.connect = AsyncMock()

        queue = BaseQueue(mock_rabbitmq, "test_queue")
        await queue._ensure_connected()

        mock_rabbitmq.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_connected_skips_when_channel_open(self):
        """Test that _ensure_connected() does nothing when channel is open."""
        mock_rabbitmq = MagicMock(spec=RabbitMQConnection)
        mock_rabbitmq.channel = AsyncMock()
        mock_rabbitmq.channel.is_closed = False
        mock_rabbitmq.connect = AsyncMock()

        queue = BaseQueue(mock_rabbitmq, "test_queue")
        await queue._ensure_connected()

        mock_rabbitmq.connect.assert_not_called()


# -----------------------------------------------------------------------------
# Integration Tests (require real RabbitMQ)
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestMessageQueueIntegration:
    """Integration tests requiring a running RabbitMQ instance.

    Run with: pytest -m integration tests/test_message_queue.py
    Requires RabbitMQ running on localhost:5672
    """

    @pytest.fixture
    async def rabbitmq_connection(self):
        """Create a real RabbitMQ connection for integration tests."""
        rabbitmq = RabbitMQConnection(host="localhost", port=5672)
        try:
            await rabbitmq.connect()
            yield rabbitmq
        finally:
            await rabbitmq.close()

    async def test_company_queue_roundtrip(self, rabbitmq_connection):
        """Test publishing and consuming a company through the queue via feed_queue."""
        company_queue = CompanyQueue(rabbitmq_connection)

        test_company = {
            "company_name": "Integration Test Corp",
            "domain": "comeet",
            "company_page_url": "https://jobs.comeet.co/integration-test",
        }

        # Publish
        await company_queue.publish(test_company)

        # Consume via feed_queue into an internal asyncio.Queue
        internal_queue: asyncio.Queue[QueueItem] = asyncio.Queue()

        async def feed_with_timeout():
            await company_queue.feed_queue(internal_queue, prefetch=1)

        # Start feed_queue in background, cancel after we get our message
        feed_task = asyncio.create_task(feed_with_timeout())

        try:
            # Wait for item to appear in internal queue
            item = await asyncio.wait_for(internal_queue.get(), timeout=5.0)
            received = item.data
            # Ack the message
            await item.message.ack()
        except asyncio.TimeoutError:
            received = None
        finally:
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass

        assert received is not None
        assert received["company_name"] == test_company["company_name"]
        assert received["domain"] == test_company["domain"]

    async def test_job_queue_batch_roundtrip(self, rabbitmq_connection):
        """Test publishing and consuming a job batch through the queue."""
        job_queue = JobQueue(rabbitmq_connection)

        test_jobs = [
            {"title": "Engineer", "url": "https://example.com/job1", "uid": "test-1"},
            {"title": "Designer", "url": "https://example.com/job2", "uid": "test-2"},
        ]
        test_source = "https://example.com/careers"

        # Publish batch
        await job_queue.publish_jobs_from_url(test_jobs, test_source)

        # Consume (with timeout)
        received_jobs = None
        received_source = None

        async def capture_jobs(jobs_data: dict):
            nonlocal received_jobs, received_source
            received_jobs = jobs_data.get("jobs")
            received_source = jobs_data.get("source_url")
            raise asyncio.CancelledError()

        try:
            await asyncio.wait_for(job_queue.consume(capture_jobs, prefetch=1), timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        assert received_jobs is not None
        assert len(received_jobs) == 2
        assert received_source == test_source
        assert received_jobs[0]["title"] == "Engineer"
