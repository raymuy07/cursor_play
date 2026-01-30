import logging

from app.common.txt_embedder import TextEmbedder
from app.core.db_utils import PendingEmbeddedDB

logger = logging.getLogger(__name__)


class JobEmbedder:
    """Handles filtering, validation, and embedding of job batches."""

    def __init__(self, pending_db: PendingEmbeddedDB, text_embedder: TextEmbedder):
        self.pending_db = pending_db
        self.embedder = text_embedder
        self.total_jobs_for_batch = []

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

    async def process_batch(self, jobs_data: dict):
        """Process a batch of jobs from the queue.

        Called by the job queue consumer. Filters jobs and submits for embedding.
        """
        jobs = jobs_data.get("jobs", [])
        source_url = jobs_data.get("source_url", "")

        valid_jobs, filter_counts = self.filter_valid_jobs(jobs)

        if valid_jobs:
            logger.info(f"Filtered {len(valid_jobs)} jobs from {source_url} (removed: {filter_counts})")
            self.total_jobs_for_batch.extend(valid_jobs)

            # When batch is large enough, submit for embedding
            if len(self.total_jobs_for_batch) >= 500:
                batch_id = await self.embedder.create_embedding_batch(self.total_jobs_for_batch)
                await self.pending_db.insert_pending_batch_id(batch_id)
                self.total_jobs_for_batch = []
                logger.info(f"Submitted embedding batch: {batch_id}")
        else:
            logger.warning(f"No valid jobs found from {source_url}")
