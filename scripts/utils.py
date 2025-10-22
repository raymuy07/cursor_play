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


def setup_logging() -> logging.Logger:
    """
    Set up logging configuration
    """
    config = load_config()
    log_config = config.get('logging', {})
    
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_config.get('file', 'logs/jobhunter.log')),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('jobhunter')




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
