#!/usr/bin/env python3
"""
Company Discovery Script
Discovers company job pages using Google dork queries via Serper API
"""

import requests
import json
import time
from typing import List, Dict
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.utils import load_config
from scripts.db_utils import SearchQueriesDB, CompaniesDB, initialize_database, SEARCH_QUERIES_DB, COMPANIES_DB
from scripts.db_schema import get_search_queries_schema, get_companies_schema

import logging

logger = logging.getLogger(__name__)


"""
This whole script is deprecated.
We are now using the company_selector.py script to discover companies.
"""


def discover_companies() -> List[Dict]:
    """
    Discover company job pages using Google dork queries via Serper API
    Returns list of all companies currently in the database
    """
    config = load_config()

    # Initialize both databases if they don't exist
    try:
        initialize_database(SEARCH_QUERIES_DB, get_search_queries_schema())
        logger.info("search_queries.db initialized/verified")
    except Exception as e:
        logger.warning(f"Could not initialize search_queries.db: {e}")

    try:
        initialize_database(COMPANIES_DB, get_companies_schema())
        logger.info("companies.db initialized/verified")
    except Exception as e:
        logger.warning(f"Could not initialize companies.db: {e}")

    # Initialize database connections
    search_db = SearchQueriesDB()
    companies_db = CompaniesDB()

    # Get existing company count before starting
    existing_count = companies_db.count_companies()
    logger.info(f"Starting discovery - {existing_count} companies already in database")

    # Domains to search for job pages
    # For now only Comeet is supported, but we can add more later
    # TODO: Add more domains

    #"greenhouse.io",
    #"lever.co",
    #"workday.com",
    #"bamboohr.com"

    domains = [
        "comeet.com",

    ]

    new_companies_count = 0

    for domain in domains:
        logger.info(f"Searching for jobs on {domain}")

        try:
            # Search for job pages on this domain and insert into database
            discovered_count = search_domain_jobs(domain, config, search_db, companies_db)
            new_companies_count += discovered_count

        except Exception as e:
            logger.error(f"Error searching {domain}: {e}")

        # Rate limiting between domains
        time.sleep(config['company_scraping']['between_domains_delay'])

    # Get final count
    final_count = companies_db.count_companies()

    logger.info(f"Discovery complete - Total companies in database: {final_count} (new: {new_companies_count})")

    # Return all companies from database
    return companies_db.get_all_companies()


def search_domain_jobs(domain: str, config: Dict, companies_db: CompaniesDB = None) -> int:
    """
    Search for job pages on a specific domain using Serper API
    Returns the count of new companies added to the database
    """
    serper_api_key = config['serper_api_key']

    # Get domain-specific template or use default
    domain_templates = config['google_dork']['domain_templates']

    if domain in domain_templates:
        domain_config = domain_templates[domain]
        query_template = domain_config['query_template']
        max_pages = domain_config['max_pages']
    else:
        logger.error(f"No specific template for {domain}")
        return 0

    # Construct the search query
    query = query_template.format(domain=domain)

    new_companies_count = 0
    total_results_count = 0  # Track total results across all pages

    # Search multiple pages
    for page in range(1, max_pages + 1):
        logger.info(f"Searching page {page} for {domain}")

        try:
            # Prepare Serper API request
            url = "https://google.serper.dev/search"

            # Create payload for Serper API
            payload = json.dumps([{
                "q": query,
                "page": page
            }])

            headers = {
                'X-API-KEY': serper_api_key,
                'Content-Type': 'application/json'
            }

            # Make API request
            response = requests.post(url, headers=headers, data=payload, timeout=30)
            response.raise_for_status()

            # Parse response
            search_results = response.json()
            search_results = search_results[0]

            if not search_results or 'organic' not in search_results:
                logger.warning(f"No organic results found for {domain} page {page}")
                break

            # Count results from this page
            page_results_count = len(search_results.get('organic', []))
            total_results_count += page_results_count

            # Process search results and insert into database
            page_new_count = process_search_results(search_results['organic'], domain, companies_db)
            new_companies_count += page_new_count

            # If we got fewer than 10 results, we've likely reached the end
            if len(search_results['organic']) < 10:
                logger.info(f"Reached end of results for {domain} at page {page}")
                break

            # Rate limiting between pages
            time.sleep(config['company_scraping']['between_pages_delay'])

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {domain} page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"Error processing {domain} page {page}: {e}")
            break


        logger.info(f"Logged search for {domain}: {total_results_count} total results collected, {new_companies_count} new companies added")

    return new_companies_count


def process_search_results(organic_results: List[Dict], domain: str, companies_db: CompaniesDB = None) -> int:
    """
    Process search results, extract company information, and insert into database
    Returns the count of new companies added
    """
    new_companies_count = 0

    for result in organic_results:
        try:
            # Extract data from search result
            title = result.get('title', '')
            link = result.get('link', '')

            # Extract company name from title
            company_name = extract_company_name_from_title(title, domain)

            if company_name and link:
                # Clean the URL to remove job-specific paths
                clean_url = clean_job_page_url(link)

                company_data = {
                    "company_name": company_name,
                    "domain": domain,
                    "job_page_url": clean_url,
                    "title": title,
                    "source": "google_serper"
                }

                # Insert into database if companies_db is provided
                if companies_db:
                    company_id = companies_db.insert_company(company_data)
                    if company_id:
                        new_companies_count += 1
                        logger.debug(f"Added new company: {company_name} at {clean_url}")
                    else:
                        logger.debug(f"Company already exists: {company_name} at {clean_url}")
                else:
                    logger.debug(f"Found company: {company_name} at {clean_url}")

        except Exception as e:
            logger.error(f"Error processing search result: {e}")
            continue

    return new_companies_count


def extract_company_name_from_title(title: str, domain: str) -> str:
    """
    Extract company name from job page title
    Examples:
    - "Jobs at Flare - Comeet" -> "Flare"
    - "Jobs at Tesla" -> "Tesla" greenhouse.io
    """
    try:
        # Remove domain-specific suffixes
        title_clean = title.replace(f" - {domain}", "").replace(f" | {domain}", "")

        # Common patterns to extract company name
        patterns = [
            "Jobs at ",      # "Jobs at Flare" -> "Flare"
            "Careers at ",   # "Careers at Tesla" -> "Tesla"
            "Work at ",      # "Work at Google" -> "Google"
        ]

        for pattern in patterns:
            if pattern in title_clean:
                company_name = title_clean.split(pattern)[1].strip()
                # Remove any trailing job-related words
                company_name = company_name.split(" Jobs")[0].strip()
                company_name = company_name.split(" Careers")[0].strip()
                company_name = company_name.split(" - ")[0].strip()
                company_name = company_name.split(" | ")[0].strip()
                return company_name

        # If no prefix pattern found, try to extract from the beginning
        if "Jobs" in title_clean:
            parts = title_clean.split("Jobs")
            if len(parts) > 1:
                company_name = parts[0].strip()
                return company_name

        # Fallback: return the first part before any separator
        company_name = title_clean.split(" - ")[0].split(" | ")[0].strip()
        return company_name if company_name else None

    except Exception:
        return None


def clean_job_page_url(url: str) -> str:
    """
    Clean job page URL to remove job-specific paths
    Examples:
    - "https://www.comeet.com/jobs/syqe/F1.00C/back-end-developer/3B.01B" -> "https://www.comeet.com/jobs/syqe/F1.00C"
    - "https://www.comeet.com/jobs/flare/36.00F" -> "https://www.comeet.com/jobs/flare/36.00F"
    """
    try:
        # For Comeet URLs, keep only up to the company ID part
        # Pattern: /jobs/{company_name}/{company_id}/...
        if 'comeet.com/jobs/' in url:
            parts = url.split('/')
            if len(parts) >= 6:  # https://www.comeet.com/jobs/company/id
                # Keep only up to the company ID
                return '/'.join(parts[:6])

        return url
    except Exception:
        return url


if __name__ == "__main__":
    from common.utils import setup_logging
    setup_logging()

    logger.info("Discovering Companies")
    discover_companies()
