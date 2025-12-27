"""
Job Embedding Script
Processes jobs in the database and generates embeddings for job descriptions.
Uses the same TextEmbedder class as CV embedding for consistency.
"""

import os
import sys
import pickle
import time
import argparse
import logging
from typing import Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utils import load_config, TextEmbedder
from scripts.db_utils import JobsDB

logger = logging.getLogger(__name__)


def embed_jobs(jobs_db:JobsDB, embedder:TextEmbedder, prompt_version:str) -> tuple[int, int]:
    pass



## TODO Need to decide what will be the batch size at it can be cheaper with open ai . and the embedder should be taken from config.
def process_jobs_with_embeddings(
    jobs_db: JobsDB,
    embedder: TextEmbedder,
    batch_size: int = 10,
    delay_seconds: float = 1.0,
    max_jobs: Optional[int] = None,
) -> tuple[int, int]:
    """
    Process jobs without embeddings and generate them.
    
    Args:
        jobs_db: JobsDB instance for database operations
        embedder: TextEmbedder instance for generating embeddings
        batch_size: Number of jobs to process before logging progress
        delay_seconds: Delay between processing each job (seconds)
        max_jobs: Maximum number of jobs to process (None for all)
        logger: Optional logger instance
        
    Returns:
        Tuple of (processed_count, error_count)
    """
    
    # Fetch jobs without embeddings
    logger.info(f"Fetching jobs without embeddings (limit: {max_jobs or 'all'})...")
    jobs = jobs_db.get_jobs_without_embeddings(limit=max_jobs)
    
    if not jobs:
        logger.info("No jobs found without embeddings.")
        return 0, 0
    
    total_jobs = len(jobs)
    logger.info(f"Found {total_jobs} jobs to process")
    
    processed = 0
    errors = 0
    
    logger.info(f"Starting embedding generation (batch_size={batch_size}, delay={delay_seconds}s)...")
    
    for i, job in enumerate(jobs, 1):
        job_id = job['id']
        job_title = job.get('title', 'Unknown')
        description = job.get('description', '')
        
        # Skip if no description
        if not description or not description.strip():
            logger.warning(f"Job {job_id} ({job_title}) has no description, skipping...")
            errors += 1
            continue
        
        try:
            # Generate embedding
            embedding = embedder.embed_text(description)
            
            # Serialize numpy array to bytes using pickle
            embedding_bytes = pickle.dumps(embedding)
            
            # Update job in database
            success = jobs_db.update_job_embedding(job_id, embedding_bytes)
            
            if success:
                processed += 1
            else:
                logger.warning(f"Failed to update embedding for job {job_id}")
                errors += 1
            
            # Log progress in batches
            if i % batch_size == 0:
                logger.info(f"Processed {i}/{total_jobs} jobs ({processed} successful, {errors} errors)...")
            
            # Delay between jobs to avoid overwhelming the system
            if i < total_jobs:  # Don't delay after last job
                time.sleep(delay_seconds)
                
        except Exception as e:
            logger.error(f"Error processing job {job_id} ({job_title}): {e}", exc_info=True)
            errors += 1
            continue
    
    logger.info(f"Completed: {processed} jobs processed successfully, {errors} errors")
    return processed, errors


def main():
    """Main function to process jobs and generate embeddings."""
     
    # Load configuration
    try:
        config = load_config()
        cv_config = config.get('cv_embedding', {})
        job_config = config.get('job_embedding', {})
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("JOB EMBEDDING GENERATION")
    logger.info("=" * 60)
    
    # Get configuration values
    model_name = job_config.get('model_name') or cv_config.get('model_name', 'all-MiniLM-L6-v2')
    batch_size = job_config.get('batch_size')
    delay_seconds = job_config.get('delay_seconds')
    max_jobs = job_config.get('max_jobs')
    
    logger.info(f"Configuration:")
    logger.info(f"  Model: {model_name}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Delay: {delay_seconds} seconds")
    logger.info(f"  Max jobs: {max_jobs}")
    
    try:
        # Initialize database and embedder
        logger.info("Initializing database connection...")
        jobs_db = JobsDB()
        
        logger.info(f"Initializing embedder with model: {model_name}...")
        embedder = TextEmbedder(model_name=model_name, logger=logger)
        
        # Process jobs
        processed, errors = process_jobs_with_embeddings(
            jobs_db=jobs_db,
            embedder=embedder,
            batch_size=batch_size,
            delay_seconds=delay_seconds,
            max_jobs=max_jobs,
            logger=logger
        )
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUCCESS! Job embeddings processing completed")
        logger.info("=" * 60)
        logger.info(f"Processed: {processed} jobs")
        logger.info(f"Errors: {errors} jobs")
        logger.info(f"Model: {model_name}")
        
    except Exception as e:
        logger.error(f"Error processing job embeddings: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

