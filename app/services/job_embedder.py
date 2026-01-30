import asyncio
import logging
from functools import partial

import aiosqlite

from app.common.txt_embedder import TextEmbedder
from app.common.utils import setup_logging
from app.core.db_utils import PendingEmbeddedDB
from app.services.message_queue import JobQueue, RabbitMQConnection

logger = logging.getLogger(__name__)


class JobEmbedder:
    """Handles filtering and validation of job batches."""

    def __init__(self,)



    def run():




    @staticmethod
    def is_hebrew_job(job: dict) -> bool:
        """
        Check if a job contains Hebrew text.
        I dont want to support hebrew jobs. for now.
        """
        # Check multiple fields for Hebrew characters
        # Handle description which might be a dict
        description = job.get("description", "")
        if isinstance(description, dict):
            description = " ".join(str(v) for v in description.values() if v)

        fields_to_check = [job.get("title", ""), description, job.get("location", ""), job.get("company_name", "")]

        # Combine all text fields
        combined_text = " ".join(str(field) for field in fields_to_check if field)

        if not combined_text:
            return False

        # Count Hebrew characters
        hebrew_chars = sum(1 for c in combined_text if "\u0590" <= c <= "\u05ff")
        total_alpha_chars = sum(1 for c in combined_text if c.isalpha())

        # If more than 10% of alphabetic characters are Hebrew, consider it a Hebrew job
        if total_alpha_chars > 0:
            hebrew_ratio = hebrew_chars / total_alpha_chars
            return hebrew_ratio > 0.1

        return False

    @staticmethod
    def is_in_israel_filter(job: dict) -> bool:
        """Check if the job location contains 'ISRAEL'."""
        location = job.get("location", "")
        return "ISRAEL" in location

    @staticmethod
    def filter_valid_jobs(jobs: list[dict]) -> tuple[list[dict], dict[str, int]]:
        """
        Filter jobs based on validation criteria.
        Returns valid jobs and a breakdown of filtered counts.

        Returns:
        Tuple of (valid_jobs, filter_counts) where:
        - valid_jobs: list of jobs that passed all filters
        - filter_counts: dictionary with counts of filtered jobs by reason
                        e.g., {'hebrew': 5, 'general_department': 2}
        """
        valid_jobs = []
        filter_counts = {"hebrew": 0, "general_department": 0, "job_not_in_israel": 0}

        for job in jobs:
            # Filter: Jobs not in Israel
            if not JobEmbedder.is_in_israel_filter(job):
                filter_counts["job_not_in_israel"] += 1
                continue

            # Filter: Hebrew jobs
            if JobEmbedder.is_hebrew_job(job):
                filter_counts["hebrew"] += 1
                continue

            # Filter: General department jobs
            try:
                department = str(job.get("department")).strip().lower()
                if department == "general":
                    filter_counts["general_department"] += 1
                    continue
            except AttributeError:
                pass

            valid_jobs.append(job)

        return valid_jobs, filter_counts



    async def filter_embedder_batch_call(jobs_data: dict, pending_embedded_db: PendingEmbeddedDB, db: aiosqlite.Connection):

        """This function is called by the job queue consumer. it will filter the jobs and then embed them.
        it gets the jobs_data dict as a batch of all the jobs that were scraped from a company."""

        total_jobs_for_batch = []
        jobs = jobs_data.get("jobs", [])
        source_url = jobs_data.get("source_url", "")

        ### TODO: This need to move cause we need to implemet it  with dependency injection.
        embedder = TextEmbedder()
        job_filter = JobEmbedder()

        valid_jobs, filter_counts = job_filter.filter_valid_jobs(jobs)

        if valid_jobs:
            logger.info(f"Filtered {len(valid_jobs)} jobs from {source_url}")
            # TODO add to the text embedder a batch call
            total_jobs_for_batch.extend(valid_jobs)
            if len(total_jobs_for_batch) >= 500:
                batch_id = await embedder.create_embedding_batch(total_jobs_for_batch)
                await pending_embedded_db.insert_pending_batch_id(db, batch_id)
                total_jobs_for_batch = []
                #!Question is who will check the db all the time for the completed? i think the scheduler
                ## the persister will not work as a pure consumer
        else:
            logger.warning(f"No valid jobs found from {source_url}")


