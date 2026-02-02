"""
Job Persister - Retrieves embedding results and updates jobs in database.

Scheduled: Twice daily via scheduler (after embeddings complete)

Flow (Option B - simpler):
1. Jobs are already in jobs.db (inserted by embedder_worker with embedding = NULL)
2. Check OpenAI batch status
3. Get embeddings (keyed by job_id)
4. Update each job's embedding in jobs.db
"""

import logging
import pickle

from app.common.txt_embedder import TextEmbedder
from app.core.db_utils import JobsDB, PendingEmbeddedDB

logger = logging.getLogger(__name__)


class JobPersister:
    """Updates job embeddings after OpenAI batch completes."""

    def __init__(self, jobs_db: JobsDB, pending_db: PendingEmbeddedDB, embedder: TextEmbedder):
        self.embedder = embedder
        self.jobs_db = jobs_db
        self.pending_db = pending_db

    async def persist_jobs(self, jobs: list[dict]) -> list[tuple[str, str]]:

        jobs_for_embedding: list[tuple[str, str]] = []

        async with self.jobs_db as jobs_db:
            for job in jobs:
                text = self._extract_job_text(job)
                if not text.strip():
                    continue

                url_hash = await jobs_db.insert_job(job)
                if url_hash:
                    jobs_for_embedding.append((url_hash, text))

        return jobs_for_embedding

    ##TODO : Check if it extracts all the description cause in the jobs dict there is requiremnts inside the description!
    @staticmethod
    def _extract_job_text(job: dict) -> str:
        """Extract text content from a job for embedding."""
        description = job.get("description", "")
        if isinstance(description, dict):
            description = "\n".join(f"{k}: {v}" for k, v in description.items() if v)

        title = job.get("title", "")
        return f"{title}\n\n{description}".strip()

    async def persist_batch(self):
        """
        Check for completed embedding batches and update job embeddings.

        Flow:
        1. Get all 'processing' batches from pending DB
        2. Check each batch status with OpenAI
        3. For completed batches:
           - Retrieve embeddings (keyed by job_id)
           - Update each job's embedding in jobs.db
           - Mark batch as completed
        """
        async with self.pending_db as pending_db:
            batches = await pending_db.get_processing_batches()

            if not batches:
                logger.info("No pending batches to process.")
                return

            for batch in batches:
                batch_id = batch["batch_id"]
                await self._process_single_batch(pending_db, batch_id)

    async def _process_single_batch(self, pending_db: PendingEmbeddedDB, batch_id: str):
        """Process a single batch: check status, retrieve results, update embeddings."""
        try:
            status_info = await self.embedder.get_batch_status(batch_id)
            status = status_info["status"]

            if status == "completed":
                await self._handle_completed_batch(pending_db, batch_id)

            elif status == "failed":
                logger.error(f"Batch {batch_id} failed: {status_info}")
                await pending_db.update_batch_status(batch_id, "failed")

            elif status in ("validating", "in_progress", "finalizing"):
                logger.info(f"Batch {batch_id} still processing: {status}")
                # Leave as 'processing', will check again next run

            else:
                logger.warning(f"Batch {batch_id} has unexpected status: {status}")

        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}", exc_info=True)

    async def _handle_completed_batch(self, pending_db: PendingEmbeddedDB, batch_id: str):
        """Handle a completed batch: retrieve embeddings and update jobs."""
        logger.info(f"Batch {batch_id} completed. Retrieving results...")

        # 1. Get embeddings from OpenAI (keyed by url_hash: str)
        embeddings_dict = self.embedder.get_batch_results(batch_id)
        logger.info(f"Retrieved {len(embeddings_dict)} embeddings from batch {batch_id}")

        # 2. Update each job's embedding in jobs.db
        updated = 0
        failed = 0

        async with self.jobs_db as jobs_db:
            for url_hash, embedding_vector in embeddings_dict.items():
                # Pickle the embedding for BLOB storage
                embedding_bytes = pickle.dumps(embedding_vector)

                # Update the job's embedding by url_hash
                success = await jobs_db.update_job_embedding(url_hash, embedding_bytes)
                if success:
                    updated += 1
                else:
                    logger.warning(f"Failed to update embedding for url_hash {url_hash}")
                    failed += 1

        logger.info(f"Batch {batch_id}: updated {updated} embeddings, {failed} failed")

        # 3. Mark batch as completed
        await pending_db.update_batch_status(batch_id, "completed")
        logger.info(f"Batch {batch_id} processed and marked completed")
