"""
Embedder Worker - drains job queue, filters, inserts jobs, and creates embedding batch.

Run with: python -m app.workers.embedder_worker
Scheduled: Once daily via scheduler
"""

from __future__ import annotations

import logging

from app.common.txt_embedder import TextEmbedder
from app.core.db_utils import PendingEmbeddedDB
from app.core.message_queue import JobQueue
from app.services.job_filter import JobFilter
from app.services.job_persister import JobPersister

logger = logging.getLogger("app.workers")


class JobManager:
    def __init__(
        self,
        jobs_persister: JobPersister,
        pending_db: PendingEmbeddedDB,
        text_embedder: TextEmbedder,
        job_queue: JobQueue,
    ):
        self.jobs_persister = jobs_persister
        self.pending_db = pending_db
        self.text_embedder = text_embedder
        self.job_queue = job_queue

    async def proccess_jobs_from_queue(self):
        await self.job_queue.connect()

        with self.pending_db as pending_db:
            logger.info("Draining jobs from queue...")
            all_jobs = await self.job_queue.drain_all(timeout_seconds=5.0)

            if not all_jobs:
                logger.info("No jobs in queue, nothing to embed")
                return

            logger.info(f"Drained {len(all_jobs)} jobs from queue")

            valid_jobs, filter_counts = JobFilter.filter_valid_jobs(all_jobs)
            logger.info(f"Filtered to {len(valid_jobs)} valid jobs (removed: {filter_counts})")

            if not valid_jobs:
                logger.info("No valid jobs after filtering, nothing to embed")
                return

            # Persist jobs and get (url_hash, text) tuples for embedding
            jobs_for_embedding = await self.jobs_persister.persist_jobs(valid_jobs)

            if not jobs_for_embedding:
                logger.info("No new jobs to embed (all duplicates or empty)")
                return

            # Create embedding batch using url_hash as custom_id
            batch_id = await self.text_embedder.create_embedding_batch(jobs_for_embedding)
            logger.info(f"Created embedding batch: {batch_id}")

            # Store batch_id in pending_batches
            await pending_db.insert_pending_batch_id(batch_id)
            logger.info(f"Saved batch {batch_id} to pending DB")
