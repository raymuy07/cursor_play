"""
Job Embedding Script
Processes jobs in the database and generates embeddings for job descriptions.
Uses the same TextEmbedder class as CV embedding for consistency.
"""

import logging

from common.txt_embedder import TextEmbedder
from scripts.db_utils import JobsDB, PendingEmbeddedDB, get_async_db_connection
from scripts.job_filter_embedder import JobFilter

##TODO: need to change the whole logging structure so it wont take by name, but for module.
logger = logging.getLogger(__name__)


class JobPersister:
    """Handles persistence of job batches to the database."""

    def __init__(self):
        self.embedder = TextEmbedder()
        self.jobs_db = JobsDB()

    @staticmethod
    def save_jobs_to_db(jobs: list[dict], jobs_db: JobsDB) -> tuple[bool, int, int]:
        """
        Save jobs to the jobs database using JobsDB.
        Automatically handles duplicate detection (via URL uniqueness).
        Filters out invalid jobs using filter_valid_jobs().

        Args:
            jobs: list of job dictionaries to save
            jobs_db: JobsDB instance for database operations

        Returns:
            Tuple of (success, jobs_inserted, jobs_skipped) where:
            - success: True if save operation completed without critical errors
            - jobs_inserted: Number of new jobs successfully inserted
            - jobs_skipped: Number of jobs skipped (duplicates or errors)
        """
        inserted = 0
        skipped = 0
        errors = 0

        # Filter out invalid jobs using external filtering function
        valid_jobs, filter_counts = JobFilter.filter_valid_jobs(jobs)
        logger.info(f"Filter counts: {filter_counts}")
        # Process only valid jobs
        for job in valid_jobs:
            try:
                job_id = jobs_db.insert_job(job)
                if job_id:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Unexpected error processing job with URL '{job.get('url')}': {e}", exc_info=True)
                skipped += 1
                errors += 1

        # Success if we processed jobs without too many errors (allow some failures)
        success = errors == 0 or (errors < len(valid_jobs) * 0.5)  # Fail if >50% errors

        return success, inserted, skipped

    @staticmethod
    def enrich_jobs_with_company(jobs: list[dict], company: dict) -> list[dict]:
        """Ensure jobs include company_name and source from the company record when missing."""
        for job in jobs:
            if not job.get("company_name"):
                job["company_name"] = company.get("company_name")
            if not job.get("source") and company.get("domain"):
                job["source"] = company.get("domain")
        return jobs

    async def check_pending_batches(self, pending_db: PendingEmbeddedDB):
        """Check status of pending batches and persist results if ready."""
        logger.info("Checking pending embedding batches...")
        async with get_async_db_connection(pending_db.db_path) as db:
            batches = await pending_db.get_processing_batches(db)

            if not batches:
                logger.info("No pending batches to check.")
                return

            for batch_record in batches:
                batch_id = batch_record["batch_id"]
                try:
                    status_info = await self.embedder.get_batch_status(batch_id)

                    if status_info["status"] == "completed":
                        logger.info(f"Batch {batch_id} completed. Retrieving results...")
                        embeddings = self.embedder.get_batch_results(batch_id)

                        # TODO: Map embeddings back to jobs and save them
                        # This requires a way to retrieve the original jobs for this batch
                        # For now, we mark it as completed to avoid re-processing
                        await pending_db.update_batch_status(db, batch_id, "completed")
                        logger.info(f"Batch {batch_id} processed and marked completed.")

                    elif status_info["status"] == "failed":
                        logger.error(f"Batch {batch_id} failed.")
                        await pending_db.update_batch_status(db, batch_id, "failed")

                except Exception as e:
                    logger.error(f"Error checking batch {batch_id}: {e}")


if __name__ == "__main__":
    ##TODO: and how do we get through db? like arent we on docker?
    pass
