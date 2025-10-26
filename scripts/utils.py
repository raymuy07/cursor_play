#!/usr/bin/env python3
"""
Utility Functions
Common helper functions used across scripts
"""

import yaml
import json
import logging
import requests
import time
from typing import Dict, List, Any
from pathlib import Path
from logging.handlers import RotatingFileHandler


def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.yaml
    """
    config_path = Path('config.yaml')
    if not config_path.exists():
        raise FileNotFoundError("config.yaml not found")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load environment variables for sensitive data
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    # Replace placeholders with environment variables
    if 'openai_api_key' in config:
        config['openai_api_key'] = os.getenv('OPENAI_API_KEY', config['openai_api_key'])
    if 'telegram_bot_token' in config:
        config['telegram_bot_token'] = os.getenv('TELEGRAM_BOT_TOKEN', config['telegram_bot_token'])
    if 'telegram_chat_id' in config:
        config['telegram_chat_id'] = os.getenv('TELEGRAM_CHAT_ID', config['telegram_chat_id'])
    if 'serper_api_key' in config:
        config['serper_api_key'] = os.getenv('SERPER_API_KEY', config['serper_api_key'])
    
    return config


def setup_logging(config: Dict[str, Any] | None = None) -> logging.Logger:
    """
    Set up logging configuration with rotation support.
    Accepts optional config to avoid re-loading from disk.
    """
    if config is None:
        config = load_config()

    log_config = config.get('logging', {})

    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)

    # Prepare logger
    logger = logging.getLogger('jobhunter')
    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()

    level_name = str(log_config.get('level', 'INFO')).upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler with rotation
    log_file_path = log_config.get('file', 'logs/jobhunter.log')
    max_size_str = str(log_config.get('max_size', '10MB'))
    backup_count = int(log_config.get('backup_count', 5))

    def _parse_size_to_bytes(size_str: str) -> int:
        s = size_str.strip().upper()
        try:
            if s.endswith('KB'):
                return int(float(s[:-2]) * 1024)
            if s.endswith('MB'):
                return int(float(s[:-2]) * 1024 * 1024)
            if s.endswith('GB'):
                return int(float(s[:-2]) * 1024 * 1024 * 1024)
            # Raw integer bytes
            return int(s)
        except Exception:
            # Default to 10MB if parsing fails
            return 10 * 1024 * 1024

    max_bytes = _parse_size_to_bytes(max_size_str)

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(formatter)

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger




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
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(filepath: str) -> List[Dict]:
    """
    Load data from JSON file
    """
    if not Path(filepath).exists():
        return []
    
    with open(filepath, 'r') as f:
        return json.load(f)
