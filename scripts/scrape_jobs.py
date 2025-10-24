#!/usr/bin/env python3
"""
Job Scraper Script
Scrapes engineering-relevant job listings from Comeet career pages
"""

import requests
import json
import time
import re
from typing import List, Dict, Set
from bs4 import BeautifulSoup
from utils import load_config, setup_logging, load_json, save_json


def is_engineering_job(job_title: str, department: str = "") -> bool:
    """
    Determine if a job is engineering-relevant based on keywords
    """
    # Combine job title and department for analysis
    text = f"{job_title} {department}".lower()
    
    # Include keywords (engineering-relevant)
    include_keywords = [
        "engineer", "developer", "software", "backend", "frontend", "fullstack", 
        "full-stack", "devops", "data", "r&d", "research", "architect", 
        "programmer", "technical", "qa", "automation", "ml", "ai", "cloud", 
        "infrastructure", "security", "cyber", "sre", "platform", "api", 
        "mobile", "ios", "android", "web", "full stack", "machine learning",
        "artificial intelligence", "data science", "analytics", "python",
        "javascript", "java", "c++", "react", "angular", "vue", "node",
        "kubernetes", "docker", "aws", "azure", "gcp", "microservices"
    ]
    
    # Exclude keywords (non-engineering)
    exclude_keywords = [
        "marketing", "hr", "human resources", "sales", "finance", "accounting",
        "legal", "admin", "office manager", "recruiter", "talent acquisition",
        "customer success", "support", "operations", "business", "product manager",
        "project manager", "scrum master", "designer", "ui/ux", "content",
        "social media", "community", "partnership", "business development"
    ]
    
    # Check for exclude keywords first
    for keyword in exclude_keywords:
        if keyword in text:
            return False
    
    # Check for include keywords
    for keyword in include_keywords:
        if keyword in text:
            return True
    
    return False


def extract_jobs_from_page(html_content: str, company_url: str, company_name: str) -> List[Dict]:
    """
    Extract job listings from Comeet HTML page
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    jobs = []
    
    try:
        
        # Look for Comeet-specific job patterns
        # Pattern 1: Comeet positionItem links (most common pattern)
        position_items = soup.find_all('a', class_='positionItem')
        
        for position_item in position_items:
            href = position_item.get('href', '')
            
            # Skip if not a valid job URL
            if not href or not href.startswith('https://'):
                continue
            
            # Extract job title from positionLink span
            job_title = ""
            position_link = position_item.find('span', class_='positionLink')
            if position_link:
                job_title = position_link.get_text(strip=True)
            
            # Skip if no title found
            if not job_title:
                continue
            
            # Extract job details from positionDetails
            department = ""
            experience_level = ""
            employment_type = ""
            location = ""
            
            position_details = position_item.find('ul', class_='positionDetails')
            if position_details:
                detail_items = position_details.find_all('li')
                for item in detail_items:
                    text = item.get_text(strip=True)
                    
                    # Check for department (usually appears as "Software", "R&D", etc.)
                    if any(dept in text.lower() for dept in ["software", "r&d", "engineering", "development", "technology", "tech"]):
                        department = text
                    # Check for experience level
                    elif any(level in text.lower() for level in ["junior", "senior", "mid", "lead", "principal", "staff"]):
                        experience_level = text
                    # Check for employment type
                    elif any(emp_type in text.lower() for emp_type in ["full-time", "part-time", "contract", "intern"]):
                        employment_type = text
                    # Check for location (usually has map marker icon)
                    elif item.find('i', class_='fa-map-marker'):
                        location = text
            
            # Filter for engineering jobs
            if is_engineering_job(job_title, department):
                job_data = {
                    "job_title": job_title,
                    "job_url": href,
                    "department": department or "General",
                    "experience_level": experience_level,
                    "employment_type": employment_type,
                    "location": location,
                    "scraped_at": time.time()
                }
                jobs.append(job_data)
                print(f"Found engineering job: {job_title} ({department or 'General'})")
        
        # Pattern 2: Fallback - look for any job-related links
        if not jobs:
            print(f"No positionItem found for {company_name}, trying fallback patterns...")
            
            # Look for links that might be job postings
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                
                # Check if this looks like a job URL
                if ('jobs' in href or 'careers' in href) and href.startswith('https://'):
                    # Try to extract job title from link text or nearby elements
                    job_title = link.get_text(strip=True)
                    
                    # If no text in link, try to find nearby text
                    if not job_title:
                        parent = link.parent
                        if parent:
                            job_title = parent.get_text(strip=True)
                    
                    # Skip if still no title or if it's just the company name
                    if not job_title or job_title.lower() == company_name.lower():
                        continue
                    
                    # Try to extract department from context
                    department = ""
                    current_element = link.parent
                    for _ in range(3):  # Check up to 3 levels up
                        if current_element:
                            dept_text = current_element.get_text(strip=True).lower()
                            if any(dept in dept_text for dept in ["r&d", "engineering", "development", "technology", "tech", "software"]):
                                department = "R&D"
                                break
                            current_element = current_element.parent
                    
                    # Filter for engineering jobs
                    if is_engineering_job(job_title, department):
                        job_data = {
                            "job_title": job_title,
                            "job_url": href,
                            "department": department or "General",
                            "scraped_at": time.time()
                        }
                        jobs.append(job_data)
                        print(f"Found engineering job (fallback): {job_title}")
    
    except Exception as e:
        print(f"Error parsing HTML for {company_name}: {e}")
    
    return jobs


def scrape_company_jobs(company_data: Dict, config: Dict, logger) -> List[Dict]:
    """
    Scrape jobs from a single company's career page
    """
    company_url = company_data['job_page_url']
    company_name = company_data['company_name']
    
    logger.info(f"Scraping jobs for {company_name} at {company_url}")
    
    # Rate limiting
    time.sleep(config['job_scraping']['rate_limit_delay'])
    
    # Retry logic
    max_retries = config['job_scraping']['max_retries']
    timeout = config['job_scraping']['timeout']
    
    for attempt in range(max_retries):
        try:
            # Set headers to mimic a real browser
            headers = {
                'User-Agent': config['scraping']['user_agent'],
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(company_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # Extract jobs from HTML
            jobs = extract_jobs_from_page(response.text, company_url, company_name)
            
            logger.info(f"Found {len(jobs)} engineering jobs for {company_name}")
            return jobs
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {company_name}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"All attempts failed for {company_name}")
                return []
        except Exception as e:
            logger.error(f"Unexpected error scraping {company_name}: {e}")
            return []
    
    return []


def deduplicate_jobs(jobs_data: Dict) -> Dict:
    """
    Remove duplicate jobs based on job_url
    """
    seen_job_urls = set()
    deduplicated_data = {}
    
    for company_url, company_data in jobs_data.items():
        if 'jobs' in company_data:
            unique_jobs = []
            for job in company_data['jobs']:
                job_url = job.get('job_url')
                if job_url and job_url not in seen_job_urls:
                    seen_job_urls.add(job_url)
                    unique_jobs.append(job)
            
            # Only keep companies with jobs
            if unique_jobs:
                company_data['jobs'] = unique_jobs
                deduplicated_data[company_url] = company_data
    
    return deduplicated_data


def update_companies_timestamp(companies: List[Dict], scraped_companies: Set[str]):
    """
    Update last_scraped timestamp for successfully scraped companies
    """
    current_time = time.time()
    for company in companies:
        if company['job_page_url'] in scraped_companies:
            company['last_scraped'] = current_time


def scrape_all_jobs() -> Dict:
    """
    Scrape jobs from all companies in companies.json
    """
    config = load_config()
    logger = setup_logging()
    
    # Load existing companies
    companies = load_json('data/companies.json')
    if not companies:
        logger.error("No companies found in data/companies.json")
        return {}
    
    # Load existing jobs data
    existing_jobs = load_json('data/jobs_raw.json')
    if not existing_jobs:
        existing_jobs = {}
    
    logger.info(f"Starting job scraping for {len(companies)} companies")
    
    scraped_companies = set()
    total_jobs_found = 0
    
    for company in companies:
        try:
            # Scrape jobs for this company
            jobs = scrape_company_jobs(company, config, logger)
            
            if jobs:
                company_url = company['job_page_url']
                company_name = company['company_name']
                
                # Add to jobs data structure
                existing_jobs[company_url] = {
                    "company_name": company_name,
                    "company_url": company_url,
                    "last_scraped": time.time(),
                    "jobs": jobs
                }
                
                scraped_companies.add(company_url)
                total_jobs_found += len(jobs)
                
                logger.info(f"Added {len(jobs)} jobs for {company_name}")
            
        except Exception as e:
            logger.error(f"Error processing company {company.get('company_name', 'Unknown')}: {e}")
            continue
    
    # Deduplicate jobs
    deduplicated_jobs = deduplicate_jobs(existing_jobs)
    
    # Save jobs data
    save_json(deduplicated_jobs, 'data/jobs_raw.json')
    
    # Update companies timestamps
    update_companies_timestamp(companies, scraped_companies)
    save_json(companies, 'data/companies.json')
    
    logger.info(f"Job scraping completed. Found {total_jobs_found} total jobs from {len(scraped_companies)} companies")
    
    return deduplicated_jobs


def run_scheduled_scraping():
    """
    Run job scraping on a schedule (every 3 minutes)
    """
    config = load_config()
    logger = setup_logging()
    schedule_interval = config['job_scraping']['schedule_interval']
    
    logger.info(f"Starting scheduled job scraping (every {schedule_interval} seconds)")
    
    while True:
        try:
            logger.info("Starting scheduled job scraping cycle")
            scrape_all_jobs()
            logger.info(f"Completed scraping cycle. Waiting {schedule_interval} seconds...")
            time.sleep(schedule_interval)
            
        except KeyboardInterrupt:
            logger.info("Job scraping stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in scheduled scraping: {e}")
            logger.info(f"Waiting {schedule_interval} seconds before retry...")
            time.sleep(schedule_interval)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        # Run in scheduled mode
        run_scheduled_scraping()
    else:
        # Run once
        print("Scraping jobs from all companies...")
        jobs = scrape_all_jobs()
        print(f"Scraping completed. Check data/jobs_raw.json for results.")
