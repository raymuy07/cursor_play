import hashlib
from typing import List, Dict
from scripts.db_utils import JobsDB
import logging
logger = logging.getLogger(__name__)

class JobPersister:

    def save_jobs_to_db(jobs: List[Dict], jobs_db: JobsDB) -> tuple[bool, int, int]:
        """
        Save jobs to the jobs database using JobsDB.
        Automatically handles duplicate detection (via URL uniqueness).
        Filters out invalid jobs using filter_valid_jobs().

        Args:
            jobs: List of job dictionaries to save
            jobs_db: JobsDB instance for database operations
            logger: Optional logger instance

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
        valid_jobs, filter_counts = filter_valid_jobs(jobs)
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


    def generate_url_hash(url: str) -> str:
        """Generate a hash from a URL for unique identification."""
        if not url:
            return ""
        return hashlib.md5(url.encode('utf-8')).hexdigest()


    def add_hash_to_jobs(jobs: List[Dict]) -> List[Dict]:
        """Add a hash field to each job based on its URL."""
        for job in jobs:
            job['url_hash'] = generate_url_hash(job.get('url', ''))
        return jobs


    def enrich_jobs_with_company(jobs: List[Dict], company: Dict) -> List[Dict]:
        """Ensure jobs include company_name and source from the company record when missing."""
        for job in jobs:
            if not job.get('company_name'):
                job['company_name'] = company.get('company_name')
            if not job.get('source') and company.get('domain'):
                job['source'] = company.get('domain')
        return jobs



class JobFilter:

    def job_is_hebrew_filter(job: Dict) -> bool:
        """
        Check if a job contains Hebrew text.

        Args:
            job: Job dictionary with title, description, location, etc.

        Returns:
            True if job appears to be in Hebrew, False otherwise
        """
        # Check multiple fields for Hebrew characters
        # Handle description which might be a dict
        description = job.get('description', '')
        if isinstance(description, dict):
            description = ' '.join(str(v) for v in description.values() if v)

        fields_to_check = [
            job.get('title', ''),
            description,
            job.get('location', ''),
            job.get('company_name', '')
        ]

        # Combine all text fields
        combined_text = ' '.join(str(field) for field in fields_to_check if field)

        if not combined_text:
            return False

        # Count Hebrew characters
        hebrew_chars = sum(1 for c in combined_text if '\u0590' <= c <= '\u05FF')
        total_alpha_chars = sum(1 for c in combined_text if c.isalpha())

        # If more than 10% of alphabetic characters are Hebrew, consider it a Hebrew job
        if total_alpha_chars > 0:
            hebrew_ratio = hebrew_chars / total_alpha_chars
            return hebrew_ratio > 0.1

        return False

    def is_in_israel_filter(job: Dict) -> bool:

        location = job.get('location', '')
        return "ISRAEL" in location


    def filter_valid_jobs(jobs: List[Dict]) -> tuple[List[Dict], Dict[str, int]]:
        """
        Filter jobs based on validation criteria.
        Returns valid jobs and a breakdown of filtered counts.

        Returns:
        Tuple of (valid_jobs, filter_counts) where:
        - valid_jobs: List of jobs that passed all filters
        - filter_counts: Dictionary with counts of filtered jobs by reason
                        e.g., {'hebrew': 5, 'general_department': 2}
        """
        valid_jobs = []
        filter_counts = {
            'hebrew': 0,
            'general_department': 0,
            'job_not_in_israel': 0
        }

        for job in jobs:

            # Filter: Jobs not in Israel
            if not is_in_israel_filter(job):
                filter_counts['job_not_in_israel'] += 1
                continue

            # Filter: Hebrew jobs
            if job_is_hebrew_filter(job):
                filter_counts['hebrew'] += 1
                continue

            # Filter: General department jobs
            try:
                department = str(job.get('department')).strip().lower()
                if department == 'general':
                    filter_counts['general_department'] += 1
                    continue
            except AttributeError:
                pass

            valid_jobs.append(job)

        return valid_jobs, filter_counts
