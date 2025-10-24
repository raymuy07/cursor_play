


import requests
import json
import time
import re
from typing import List, Dict, Set
from bs4 import BeautifulSoup
from utils import load_config, setup_logging, load_json, save_json




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



if __name__ == "__main__":
    import os

    print("Current working directory:", os.getcwd())
    print("Files in this directory:", os.listdir())

    # Then your load command:
    with open('scripts/debug_lumenis.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    job_posting = soup.find('a', class_='positionItem')
    
    for a in soup.select("a.positionItem"):
        link = a.get("href")
        position_id = link.split("/")[-1] if link else None
        details = [li.get_text(strip=True) for li in a.select("ul.positionDetails li")]
        experience = next((d for d in details if d in ["Intern", "Junior", "Senior"]), None)
        employment = next((d for d in details if d in ["Full-time", "Part-time", "Contract"]), None)
        print(position_id, link, experience, employment)
        
    