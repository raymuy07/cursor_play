"""
Test CV-Job Matching using Embeddings and Cosine Similarity

This script:
1. Loads CV embeddings from pickle file
2. Extracts 30 jobs from jobs_raw.json
3. Embeds job descriptions (combining description + requirements)
4. Calculates cosine similarity between CV and each job
5. Ranks jobs by match score
"""

import os
import sys
import json
import pickle
import logging
from typing import List, Dict, Tuple, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.utils import load_config

logger = logging.getLogger(__name__)

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except ImportError:
    logger.error("Error: Required packages not installed.")
    logger.error("Please run: pip install sentence-transformers numpy")
    sys.exit(1)


class JobMatcher:
    """Matches jobs to CV using embedding similarity."""

    def __init__(
        self,
        cv_embedding_path: str,
        model_name: str = "all-MiniLM-L6-v2",
    ):
        """
        Initialize JobMatcher.

        Args:
            cv_embedding_path: Path to the CV embeddings pickle file
            model_name: Name of the sentence-transformer model (must match CV embedding model)
        """

        # Load CV embeddings
        logger.info(f"Loading CV embeddings from: {cv_embedding_path}")
        self.cv_data = self._load_cv_embeddings(cv_embedding_path)
        self.cv_embedding = self.cv_data['embedding']

        # Verify model consistency
        if self.cv_data['model_name'] != model_name:
            logger.warning(
                f"Model mismatch! CV was embedded with {self.cv_data['model_name']}, "
                f"but using {model_name} for jobs. This may affect accuracy."
            )

        # Load sentence transformer model
        logger.info(f"Loading sentence-transformer model: {model_name}")
        self.model = SentenceTransformer(model_name)

    def _load_cv_embeddings(self, path: str) -> Dict:
        """Load CV embeddings from pickle file."""
        if not os.path.exists(path):
            raise FileNotFoundError(f"CV embeddings file not found: {path}")

        with open(path, 'rb') as f:
            data = pickle.load(f)

        logger.info(f"CV embeddings loaded successfully")
        logger.info(f"  - Model: {data['model_name']}")
        logger.info(f"  - Embedding dimension: {data['embedding_dim']}")
        logger.info(f"  - CV text length: {data['text_length']} characters")
        logger.info(f"  - Generated: {data['timestamp']}")

        return data

    def load_jobs(self, jobs_file: str, max_jobs: int = 30) -> List[Dict]:
        """
        Load jobs from JSON file.

        Args:
            jobs_file: Path to jobs JSON file
            max_jobs: Maximum number of jobs to load

        Returns:
            List of job dictionaries
        """
        logger.info(f"Loading jobs from: {jobs_file}")

        if not os.path.exists(jobs_file):
            raise FileNotFoundError(f"Jobs file not found: {jobs_file}")

        with open(jobs_file, 'r', encoding='utf-8') as f:
            all_jobs = json.load(f)

        # Take first max_jobs
        jobs = all_jobs[:max_jobs]

        logger.info(f"Loaded {len(jobs)} jobs (out of {len(all_jobs)} total)")

        return jobs

    def extract_job_text(self, job: Dict) -> str:
        """
        Extract and combine description and requirements text from a job.

        Args:
            job: Job dictionary

        Returns:
            Combined text from description and requirements
        """
        description_obj = job.get('description', {})

        # Extract both description and requirements
        description_text = description_obj.get('description', '')
        requirements_text = description_obj.get('requirements', '')

        # Combine with clear separation
        combined_text = f"{description_text}\n\n{requirements_text}".strip()

        return combined_text

    def embed_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """
        Generate embeddings for all job descriptions.

        Args:
            jobs: List of job dictionaries

        Returns:
            List of jobs with added 'embedding' and 'full_text' fields
        """
        logger.info(f"Generating embeddings for {len(jobs)} jobs...")

        enriched_jobs = []

        for i, job in enumerate(jobs, 1):
            # Extract job text
            job_text = self.extract_job_text(job)

            if not job_text or not job_text.strip():
                logger.warning(f"Job {i} ({job.get('title', 'Unknown')}) has no description text, skipping...")
                continue

            # Generate embedding
            embedding = self.model.encode(job_text, convert_to_numpy=True)

            # Add to job data
            job_with_embedding = job.copy()
            job_with_embedding['embedding'] = embedding
            job_with_embedding['full_text'] = job_text
            job_with_embedding['text_length'] = len(job_text)

            enriched_jobs.append(job_with_embedding)

            if i % 10 == 0:
                logger.info(f"  Processed {i}/{len(jobs)} jobs...")

        logger.info(f"Successfully embedded {len(enriched_jobs)} jobs")

        return enriched_jobs

    def cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """
        Calculate cosine similarity between two vectors.

        Args:
            vec1: First vector
            vec2: Second vector

        Returns:
            Cosine similarity score (0 to 1, higher is more similar)
        """
        # Normalize vectors
        vec1_norm = vec1 / np.linalg.norm(vec1)
        vec2_norm = vec2 / np.linalg.norm(vec2)

        # Calculate dot product
        similarity = np.dot(vec1_norm, vec2_norm)

        return float(similarity)

    def score_jobs(self, jobs_with_embeddings: List[Dict]) -> List[Dict]:
        """
        Score jobs based on cosine similarity to CV.

        Args:
            jobs_with_embeddings: List of jobs with embeddings

        Returns:
            List of jobs with added 'similarity_score' field, sorted by score (descending)
        """
        logger.info(f"Calculating similarity scores for {len(jobs_with_embeddings)} jobs...")

        scored_jobs = []

        for job in jobs_with_embeddings:
            job_embedding = job['embedding']

            # Calculate cosine similarity
            similarity = self.cosine_similarity(self.cv_embedding, job_embedding)

            # Add score to job
            job_scored = job.copy()
            job_scored['similarity_score'] = similarity

            scored_jobs.append(job_scored)

        # Sort by similarity score (highest first)
        scored_jobs.sort(key=lambda x: x['similarity_score'], reverse=True)

        logger.info(f"Job scoring complete")

        return scored_jobs

    def display_results(self, scored_jobs: List[Dict], top_n: int = 10):
        """
        Display top matching jobs.

        Args:
            scored_jobs: List of scored jobs (sorted by score)
            top_n: Number of top results to display
        """
        logger.info("\n" + "=" * 80)
        logger.info(f"TOP {top_n} MATCHING JOBS")
        logger.info("=" * 80)

        for i, job in enumerate(scored_jobs[:top_n], 1):
            score_pct = job['similarity_score'] * 100

            logger.info(f"\n#{i} - Match Score: {score_pct:.2f}%")
            logger.info(f"  Title: {job.get('title', 'N/A')}")
            logger.info(f"  Company: {job.get('company_name', 'N/A')}")
            logger.info(f"  Location: {job.get('location', 'N/A')}")
            logger.info(f"  Department: {job.get('department', 'N/A')}")
            logger.info(f"  Experience Level: {job.get('experience_level', 'N/A')}")
            logger.info(f"  Workplace Type: {job.get('workplace_type', 'N/A')}")
            logger.info(f"  Description Length: {job.get('text_length', 0)} characters")
            logger.info(f"  URL: {job.get('url', 'N/A')}")

        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("STATISTICS")
        logger.info("=" * 80)

        scores = [job['similarity_score'] for job in scored_jobs]
        logger.info(f"Total jobs scored: {len(scored_jobs)}")
        logger.info(f"Average similarity: {np.mean(scores) * 100:.2f}%")
        logger.info(f"Highest similarity: {np.max(scores) * 100:.2f}%")
        logger.info(f"Lowest similarity: {np.min(scores) * 100:.2f}%")
        logger.info(f"Standard deviation: {np.std(scores) * 100:.2f}%")

    def save_results(self, scored_jobs: List[Dict], output_path: str):
        """
        Save scored jobs to JSON file.

        Args:
            scored_jobs: List of scored jobs
            output_path: Path to save results
        """
        # Prepare data for JSON (remove numpy arrays)
        jobs_for_export = []

        for job in scored_jobs:
            job_export = {k: v for k, v in job.items() if k not in ['embedding', 'full_text']}
            job_export['similarity_score'] = float(job['similarity_score'])
            jobs_for_export.append(job_export)

        # Create directory if needed
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save to JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(jobs_for_export, f, indent=2, ensure_ascii=False)

        logger.info(f"\nResults saved to: {output_path}")


def main():
    """Main test function."""

    # Load configuration
    try:
        config = load_config()
        cv_config = config.get('cv_embedding', {})
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    logger.info("=" * 80)
    logger.info("CV-JOB MATCHING TEST")
    logger.info("=" * 80)

    # Configuration
    cv_embeddings_path = cv_config.get('embeddings_output', 'data/cv_embeddings.pkl')
    jobs_file = 'data/jobs_raw.json'
    output_file = 'data/job_match_results.json'
    model_name = cv_config.get('model_name', 'all-MiniLM-L6-v2')
    num_jobs = 80  # Number of jobs to test
    top_n = 10  # Number of top results to display

    try:
        # Initialize matcher
        matcher = JobMatcher(cv_embeddings_path, model_name=model_name)

        # Load jobs
        jobs = matcher.load_jobs(jobs_file, max_jobs=num_jobs)

        # Generate embeddings for jobs
        jobs_with_embeddings = matcher.embed_jobs(jobs)

        if not jobs_with_embeddings:
            logger.warning("No jobs were successfully embedded. Exiting.")
            sys.exit(1)

        # Score jobs
        scored_jobs = matcher.score_jobs(jobs_with_embeddings)

        # Display results
        matcher.display_results(scored_jobs, top_n=top_n)

        # Save results
        matcher.save_results(scored_jobs, output_file)

        logger.info("\n" + "=" * 80)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

    except FileNotFoundError as e:
        logger.warning(f"File not found: {e}")
        logger.error("\nMake sure to run scripts/embed_cv.py first to generate CV embeddings!")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during matching: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

