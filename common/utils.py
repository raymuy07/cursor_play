#!/usr/bin/env python3
"""
Utility Functions
Common helper functions used across scripts
"""
import json
import logging
import os
import sys
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)




class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


def load_config() -> dict[str, any]:
    """
    Load configuration from config.yaml
    """
    config_path = Path("config.yaml")
    if not config_path.exists():
        raise FileNotFoundError("config.yaml not found")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Load environment variables for sensitive data
    from dotenv import load_dotenv

    load_dotenv()

    # Replace placeholders with environment variables
    if "openai_api_key" in config:
        config["openai_api_key"] = os.getenv("OPENAI_API_KEY", config["openai_api_key"])
    if "telegram_bot_token" in config:
        config["telegram_bot_token"] = os.getenv("TELEGRAM_BOT_TOKEN", config["telegram_bot_token"])
    if "telegram_chat_id" in config:
        config["telegram_chat_id"] = os.getenv("TELEGRAM_CHAT_ID", config["telegram_chat_id"])
    if "serper_api_key" in config:
        config["serper_api_key"] = os.getenv("SERPER_API_KEY", config["serper_api_key"])

    return config


def setup_logging():
    """Configure root logger - call once at service startup."""
    log_config = {}
    try:
        config = load_config()
        log_config = config.get("logging", {})
    except Exception:
        pass

    root_logger = logging.getLogger()  # ROOT logger
    root_logger.setLevel(getattr(logging, log_config.get("level", "INFO")))

    ## !TODO,I defiently need to change looging structure into json.
    if not root_logger.handlers:
        formatter = logging.Formatter(log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)


def deduplicate_companies(companies: list[dict]) -> list[dict]:
    """
    Remove duplicate companies based on company_page_url (the link)
    """
    seen_urls = set()
    unique_companies = []

    for company in companies:
        job_url = company.get("company_page_url")
        if job_url and job_url not in seen_urls:
            seen_urls.add(job_url)
            unique_companies.append(company)

    return unique_companies


def deduplicate_jobs(jobs: list[dict]) -> list[dict]:
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
    delay = config.get("scraping", {}).get("rate_limit_delay", 2)
    time.sleep(delay)


def save_json(data: list[dict], filepath: str):
    """
    Save data to JSON file with proper formatting
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(filepath: str) -> list[dict]:
    """
    Load data from JSON file
    """
    if not Path(filepath).exists():
        return []

    with open(filepath, encoding="utf-8") as f:
        return json.load(f)
