#!/usr/bin/env python3
"""
Company Discovery Script
Discovers company job pages using Google dork queries via Serper API
"""

import requests
import json
import time
import random
import re
from typing import List, Dict, Any, Optional
from utils import (
    load_config,
    setup_logging,
    deduplicate_companies,
    rate_limit_delay,
    save_json,
)


def discover_companies() -> List[Dict]:
    """
    Discover company job pages using Google dork queries via Serper API
    """
    config = load_config()
    logger = setup_logging(config)
    
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
    
    all_companies = []
    
    for domain in domains:
        logger.info(f"Searching for jobs on {domain}")
        
        try:
            # Search for job pages on this domain
            domain_companies = search_domain_jobs(domain, config, logger)
            all_companies.extend(domain_companies)
            
        except Exception as e:
            logger.error(f"Error searching {domain}: {e}")
        
        # Rate limiting between domains
        rate_limit_delay()
    
    # Load existing companies and merge with new ones
    existing_companies = load_existing_companies()
    all_companies.extend(existing_companies)
    
    # Deduplicate and save
    unique_companies = deduplicate_companies(all_companies)

    save_json(unique_companies, 'data/companies.json')
    
    logger.info(f"Total companies: {len(unique_companies)} (existing: {len(existing_companies)}, new: {len(unique_companies) - len(existing_companies)})")
    return unique_companies


def search_domain_jobs(domain: str, config: Dict[str, Any], logger) -> List[Dict]:
    """
    Search for job pages on a specific domain using Serper API
    """
    serper_api_key = config.get('serper_api_key')
    if not serper_api_key or str(serper_api_key).startswith('${'):
        logger.warning("SERPER_API_KEY not configured. Skipping Serper search for domain '%s'", domain)
        return []

    # Get domain-specific template or use default
    domain_templates = config['google_dork']['domain_templates']
    
    if domain in domain_templates:
        domain_config = domain_templates[domain]
        query_template = domain_config['query_template']
        max_pages = domain_config['max_pages']
    else:
        logger.error(f"No specific template for {domain}")
        return []
    
    # Construct the search query
    query = query_template.format(domain=domain)
    
    companies = []
    
    # Search multiple pages
    timeout_seconds = config.get('scraping', {}).get('timeout', 30)
    max_retries = int(config.get('scraping', {}).get('max_retries', 3))

    for page in range(1, max_pages + 1):
        logger.info(f"Searching page {page} for {domain}")
        
        try:
            # Prepare Serper API request
            url = "https://google.serper.dev/search"
            
            # Create payload for Serper API (single request)
            payload_obj = {
                "q": query,
                "page": page
            }
            
            headers = {
                'X-API-KEY': serper_api_key,
                'Content-Type': 'application/json'
            }
            
            # Make API request with retries and backoff
            attempt = 0
            response = None
            while attempt < max_retries:
                attempt += 1
                try:
                    response = requests.post(
                        url,
                        headers=headers,
                        json=payload_obj,
                        timeout=timeout_seconds,
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.HTTPError as http_err:
                    status = getattr(http_err.response, 'status_code', None)
                    if status in (429, 500, 502, 503, 504) and attempt < max_retries:
                        backoff = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                        logger.warning(
                            "Serper HTTP %s on attempt %s/%s for page %s; backing off %.2fs",
                            status, attempt, max_retries, page, backoff
                        )
                        time.sleep(backoff)
                        continue
                    raise
                except requests.exceptions.RequestException as req_err:
                    if attempt < max_retries:
                        backoff = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                        logger.warning(
                            "Serper request error on attempt %s/%s for page %s; backing off %.2fs: %s",
                            attempt, max_retries, page, backoff, req_err
                        )
                        time.sleep(backoff)
                        continue
                    raise
            
            # Parse response
            search_results = response.json()
            
            if not search_results or 'organic' not in search_results:
                logger.warning(f"No organic results found for {domain} page {page}")
                break
            
            # Process search results
            page_companies = process_search_results(search_results['organic'], domain, logger)
            companies.extend(page_companies)
            
            # If we got fewer than 10 results, we've likely reached the end
            if len(search_results['organic']) < 10:
                logger.info(f"Reached end of results for {domain} at page {page}")
                break
            
            # Rate limiting between pages
            rate_limit_delay()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {domain} page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"Error processing {domain} page {page}: {e}")
            break
    
    return companies


def process_search_results(organic_results: List[Dict], domain: str, logger) -> List[Dict]:
    """
    Process search results and extract company information
    """
    companies = []
    
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
                    "discovered_at": time.time(),
                    "last_scraped": None,
                }
                
                companies.append(company_data)
                logger.debug(f"Found company: {company_name} at {clean_url}")
            
        except Exception as e:
            logger.error(f"Error processing search result: {e}")
            continue
    
    return companies


def extract_company_name_from_title(title: str, domain: str) -> Optional[str]:
    """
    Extract company name from job page title
    Examples:
    - "Jobs at Flare - Comeet" -> "Flare"
    - "Jobs at Tesla" -> "Tesla" greenhouse.io
    """
    try:
        # Remove common provider brand suffixes regardless of case
        providers = [
            'Comeet', 'Greenhouse', 'Lever', 'Workday', 'BambooHR'
        ]
        title_clean = title
        for provider in providers:
            for sep in [' - ', ' | ', ' — ']:
                pattern = re.compile(re.escape(sep + provider) + r"$", flags=re.IGNORECASE)
                title_clean = pattern.sub('', title_clean)
        
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
        company_name = title_clean.split(" - ")[0].split(" | ")[0].split(" — ")[0].strip()
        return company_name if company_name else None
        
    except Exception:
        return None


def load_existing_companies() -> List[Dict]:
    """
    Load existing companies from companies.json
    """
    try:
        with open('data/companies.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


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
    print("Discovering companies...")
    discover_companies()
