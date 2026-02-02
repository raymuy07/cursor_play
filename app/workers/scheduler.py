import asyncio
import logging
import signal
import time
from concurrent.futures import ThreadPoolExecutor

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.common.txt_embedder import TextEmbedder
from app.common.utils import load_config, setup_logging
from app.core.db_utils import CompaniesDB, JobsDB, PendingEmbeddedDB
from app.core.message_queue import CompanyQueue, RabbitMQConnection
from app.services.company_manager import CompanyManager
from app.services.job_persister import JobPersister
from app.workers.embedder_worker import run_daily_embedding
from app.workers.job_manager import JobManager

logger = logging.getLogger(__name__)


class Scheduler:
    """Background scheduler for periodic job tasks."""

    def __init__(self):
        self.config = load_config()
        self.scheduler = BackgroundScheduler(executors={"default": ThreadPoolExecutor(max_workers=5)})
        self._running = True

        # Initialize components
        self.companies_db = CompaniesDB()
        self.jobs_db = JobsDB()
        self.pending_db = PendingEmbeddedDB()
        self.rabbitmq = RabbitMQConnection()
        self.company_queue = CompanyQueue(self.rabbitmq)
        self.company_manager = CompanyManager(self.companies_db, self.config, self.company_queue)
        self.embedder = TextEmbedder()
        self.job_persister = JobPersister(self.jobs_db,self.pending_db,self.embedder)
        self.job_manager = JobManager(self.job_persister, self.pending_db, self.embedder, self.job_queue)
        
    def start(self):
        """Start the scheduler and keep the main thread alive."""
        self._setup_jobs()
        self.scheduler.start()
        logger.info("Scheduler started in background mode.")

        # Handle graceful shutdown on Ctrl+C
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # Keep main thread alive
        try:
            while self._running:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self._shutdown(None, None)

    def _setup_jobs(self):
        """Configure all scheduled tasks."""
        # 1. Publish stale companies for scraping (every 30 min)
        self.scheduler.add_job(
            self._run_async_task,
            trigger=IntervalTrigger(minutes=30),
            args=[self.company_manager.publish_stale_companies],
            id="publish_stale_companies",
            name="Publish stale companies for scraping",
        )

        # 2. Search for new companies (once a week)
        self.scheduler.add_job(
            self._run_async_task,
            trigger=IntervalTrigger(weeks=1),
            args=[self.company_manager.search_for_companies],
            id="search_for_companies",
            name="Search for new companies",
        )

        # 3. Daily embedding: drain job queue, filter, embed batch (once a day at 2 AM)
        self.scheduler.add_job(
            self._run_async_task,
            trigger=CronTrigger(hour=2, minute=0),
            args=[run_daily_embedding],
            id="daily_embedding",
            name="Daily job embedding batch",
        )

        # 4. Persist completed batches: check OpenAI, save jobs to DB (twice daily at 8 AM and 8 PM)
        self.scheduler.add_job(
            self._run_async_task,
            trigger=CronTrigger(hour=8, minute=0),
            args=[self.job_persister.persist_batch],
            id="persist_batch_morning",
            name="Persist embedding batches (morning)",
        )
        self.scheduler.add_job(
            self._run_async_task,
            trigger=CronTrigger(hour=20, minute=0),
            args=[self.job_persister.persist_batch],
            id="persist_batch_evening",
            name="Persist embedding batches (evening)",
        )

    def _run_async_task(self, coro_func, *args, **kwargs):
        """Helper to run async functions in the scheduler's thread pool."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(coro_func(*args, **kwargs))
        except Exception as e:
            logger.error(f"Error in async task {coro_func.__name__}: {e}", exc_info=True)
        finally:
            loop.close()

    def _shutdown(self, signum, frame):
        """Graceful shutdown handler."""
        logger.info("Shutting down scheduler...")
        self._running = False
        self.scheduler.shutdown(wait=True)
        # Close RabbitMQ connection
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.rabbitmq.close())
        loop.close()


if __name__ == "__main__":
    setup_logging()
    s = Scheduler()
    s.start()
