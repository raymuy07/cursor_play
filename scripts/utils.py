#!/usr/bin/env python3
"""
Utility Functions
Common helper functions used across scripts
"""

import yaml
import json
import logging
import time
import sys
from typing import Dict, List, Any, Optional
from pathlib import Path
from openai import OpenAI
import os

_LOGGER: Optional[logging.Logger] = None


logger = logging.getLogger(__name__)



class TextEmbedder:
    """Shared base class for generating text embeddings"""
    
    def __init__(self):
        self.config = load_config()
        self.model_name = self.config.get('openai_model_name')
        self.client = OpenAI(api_key=self.config.get('openai_api_key'))
    
   
    def embed_text(self, text: str) -> dict:
        """
        Generate embedding for the given text.
        """
        if not text or not text.strip():
            raise ValueError("Cannot generate embeddings for empty text")
            
        ##the model is "text-embedding-3-small"
        try:
            # Generate embedding (returns 1D numpy array)
            embedding = self.client.embeddings.create(input=text, model=self.model_name).data[0].embedding
            
            self.logger.debug(f"Embedding generated successfully. Dimension: {len(embedding)}")
            
            return {
            'embedding': embedding,
            'model_name': self.embedder.model_name,
        }
            
        except Exception as e:
            raise RuntimeError(f"Error generating embedding: {str(e)}")

    def save_embedding(self, embedding_data: Dict, output_path: str):
        """
        Save embedding data to a pickle file.
        output_path: Path where to save the pickle file
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to pickle file
            with open(output_path, 'wb') as f:
                pickle.dump(embedding_data, f)
            
            self.logger.info(f"Embedding saved successfully to: {output_path}")
            
            # Print summary
            file_size = os.path.getsize(output_path) / 1024  # KB
            self.logger.info(f"File size: {file_size:.2f} KB")
            
        except Exception as e:
            raise RuntimeError(f"Error saving embedding: {str(e)}")



class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


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


def setup_logging():
    """Configure root logger - call once at service startup."""
    log_config = {}
    try:
        config = load_config()
        log_config = config.get('logging', {})
    except Exception:
        pass
    
    root_logger = logging.getLogger()  # ROOT logger
    root_logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
    
    ## !TODO,I defiently need to change looging structure into json.
    if not root_logger.handlers:
        formatter = logging.Formatter(
            log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


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
