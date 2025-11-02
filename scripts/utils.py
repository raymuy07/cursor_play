#!/usr/bin/env python3
"""
Utility Functions
Common helper functions used across scripts
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional
from pathlib import Path

from dynaconf import Dynaconf


_LOGGER: Optional[logging.Logger] = None
_SETTINGS: Optional[Dynaconf] = None


def load_config() -> Dict[str, Any]:
    """
    Load configuration using Dynaconf combining YAML and environment variables.
    """
    global _SETTINGS

    if _SETTINGS is None:
        base_dir = Path(__file__).resolve().parents[1]
        config_path = base_dir / 'config.yaml'

        if not config_path.exists():
            raise FileNotFoundError('config.yaml not found')

        dotenv_path = base_dir / '.env'

        dynaconf_kwargs: Dict[str, Any] = {
            'envvar_prefix': False,
            'settings_files': [str(config_path)],
            'load_dotenv': True,
            'merge_enabled': True,
        }

        if dotenv_path.exists():
            dynaconf_kwargs['dotenv_path'] = str(dotenv_path)

        _SETTINGS = Dynaconf(**dynaconf_kwargs)

    return _SETTINGS.as_dict()


def setup_logging() -> logging.Logger:
    """Configure and return the shared application logger."""
    global _LOGGER

    if _LOGGER is not None:
        return _LOGGER

    log_config: Dict[str, Any] = {}

    try:
        config = load_config()
        log_config = config.get('logging', {})
    except Exception:
        # Fall back to defaults if configuration cannot be loaded
        log_config = {}

    Path('logs').mkdir(exist_ok=True)

    logger = logging.getLogger('jobhunter')
    logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_path = log_config.get('file', 'logs/jobhunter.log')
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    _LOGGER = logger
    return _LOGGER




def deduplicate_companies(companies: List[Dict]) -> List[Dict]:
    """
    Remove duplicate companies based on job_page_url (the link)
    """
    seen_urls = set()
    unique_companies = []
    
    for company in companies:
        job_url = company.get('job_page_url')
        if job_url and job_url not in seen_urls:
            seen_urls.add(job_url)
            unique_companies.append(company)
    
    return unique_companies


def deduplicate_jobs(jobs: List[Dict]) -> List[Dict]:
    """
    Remove duplicate jobs based on title and company
    """
    seen_jobs = set()
    unique_jobs = []
    
    for job in jobs:
        # Create a unique identifier
        job_id = f"{job.get('title', '')}_{job.get('company', '')}"
        if job_id not in seen_jobs:
            seen_jobs.add(job_id)
            unique_jobs.append(job)
    
    return unique_jobs


def rate_limit_delay():
    """
    Apply rate limiting delay
    """
    config = load_config()
    delay = config.get('scraping', {}).get('rate_limit_delay', 2)
    time.sleep(delay)


def save_json(data: List[Dict], filepath: str):
    """
    Save data to JSON file with proper formatting
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(filepath: str) -> List[Dict]:
    """
    Load data from JSON file
    """
    if not Path(filepath).exists():
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)
