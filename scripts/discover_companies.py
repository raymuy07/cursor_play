#!/usr/bin/env python3
"""
Company Discovery Script
Discovers company job pages using Google dork queries via Serper API
"""

import requests
import json
import time
from typing import List, Dict
from utils import load_config, setup_logging, deduplicate_companies
from db_utils import SearchQueriesDB, initialize_database, SEARCH_QUERIES_DB
from db_schema import get_search_queries_schema


def discover_companies() -> List[Dict]:
    """
    Discover company job pages using Google dork queries via Serper API
    """
    config = load_config()
    logger = setup_logging()
    
    # Initialize search_queries database if it doesn't exist
    try:
        initialize_database(SEARCH_QUERIES_DB, get_search_queries_schema())
        logger.info("search_queries.db initialized/verified")
    except Exception as e:
        logger.warning(f"Could not initialize search_queries.db: {e}")
    
    # Initialize database connection
    search_db = SearchQueriesDB()
    
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
            domain_companies = search_domain_jobs(domain, config, logger, search_db)
            all_companies.extend(domain_companies)
            
        except Exception as e:
            logger.error(f"Error searching {domain}: {e}")
        
        # Rate limiting between domains
        time.sleep(config['scraping']['rate_limit_delay'])
    
    # Load existing companies and merge with new ones
    existing_companies = load_existing_companies()
    all_companies.extend(existing_companies)
    
    # Deduplicate and save
    unique_companies = deduplicate_companies(all_companies)
    
    with open('data/companies.json', 'w') as f:
        json.dump(unique_companies, f, indent=2)
    
    logger.info(f"Total companies: {len(unique_companies)} (existing: {len(existing_companies)}, new: {len(unique_companies) - len(existing_companies)})")
    return unique_companies


def search_domain_jobs(domain: str, config: Dict, logger, search_db: SearchQueriesDB = None) -> List[Dict]:
    """
    Search for job pages on a specific domain using Serper API
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
        return []
    
    # Construct the search query
    query = query_template.format(domain=domain)
    
    companies = []
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
            
            # Process search results
            page_companies = process_search_results(search_results['organic'], domain, logger)
            companies.extend(page_companies)
            
            # If we got fewer than 10 results, we've likely reached the end
            if len(search_results['organic']) < 10:
                logger.info(f"Reached end of results for {domain} at page {page}")
                break
            
            # Rate limiting between pages
            time.sleep(config['scraping']['rate_limit_delay'])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {domain} page {page}: {e}")
            break
        except Exception as e:
            logger.error(f"Error processing {domain} page {page}: {e}")
            break
    
    # Log search query with total results count to database
    if search_db:
        try:
            search_db.log_search(domain, query, 'google_serper', total_results_count)
            logger.info(f"Logged search for {domain}: {total_results_count} total results collected")
        except Exception as e:
            logger.warning(f"Failed to log search query: {e}")
    
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
