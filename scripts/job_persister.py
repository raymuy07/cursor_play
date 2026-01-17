"""
Job Embedding Script
Processes jobs in the database and generates embeddings for job descriptions.
Uses the same TextEmbedder class as CV embedding for consistency.
"""

import os
import pickle
import time
import argparse
import logging
from typing import Optional

from common.utils import load_config
from common.txt_embedder import TextEmbedder
from scripts.db_utils import JobsDB

logger = logging.getLogger(__name__)




class JobPersister:
    """Handles persistence of job batches to the database."""

    @staticmethod
    def save_jobs_to_db(jobs: list[dict], jobs_db: JobsDB) -> tuple[bool, int, int]:
        """
        Save jobs to the jobs database using JobsDB.
        Automatically handles duplicate detection (via URL uniqueness).
        Filters out invalid jobs using filter_valid_jobs().

        Args:
            jobs: List of job dictionaries to save
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
    def generate_url_hash(url: str) -> str:
        """Generate a hash from a URL for unique identification."""
        if not url:
            return ""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    @staticmethod
    def add_hash_to_jobs(jobs: list[dict]) -> List[Dict]:
        """Add a hash field to each job based on its URL."""
        for job in jobs:
            job['url_hash'] = JobPersister.generate_url_hash(job.get('url', ''))
        return jobs

    @staticmethod
    def enrich_jobs_with_company(jobs: list[dict], company: dict) -> list[dict]:
        """Ensure jobs include company_name and source from the company record when missing."""
        for job in jobs:
            if not job.get('company_name'):
                job['company_name'] = company.get('company_name')
            if not job.get('source') and company.get('domain'):
                job['source'] = company.get('domain')
        return jobs




if __name__ == "__main__":
    ##TODO: and how do we get through db? like arent we on docker?
    pass
