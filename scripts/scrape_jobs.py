import json
import re
import hashlib
import os
import sys
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.utils import setup_logging
from scripts.db_utils import JobsDB, initialize_database, COMPANIES_PAGES_DB, generate_job_hash
from scripts.db_schema import get_jobs_schema


class JobExtractor:
    """Extract job information from HTML using multiple parsing strategies."""
    
    def __init__(self, html_content: str):
        self.html_content = html_content
        self.soup = BeautifulSoup(html_content, 'html.parser')
    
    def extract_jobs(self) -> List[Dict]:
        """
        Try multiple extraction methods and return the first successful result.
        
        Returns:
            List of job dictionaries
        """
        # Method 1: Extract from JavaScript variable (Comeet pattern)
        jobs = self._extract_from_js_variable()
        if jobs:
            return jobs
        
        # Method 2: Extract from HTML elements (placeholder for alternative pattern)
        jobs = self._extract_from_html_elements()
        if jobs:
            return jobs
        
        # Method 3: Extract from JSON-LD schema (placeholder)
        
        return []
    
    def _extract_from_js_variable(self) -> List[Dict]:
        """
        Extract job data from JavaScript variable (Comeet pattern).
        Pattern: COMPANY_POSITIONS_DATA = [...];
        """
        try:
            # Find the COMPANY_POSITIONS_DATA variable
            pattern = r'COMPANY_POSITIONS_DATA\s*=\s*(\[.*?\]);'
            match = re.search(pattern, self.html_content, re.DOTALL)
            
            if match:
                json_str = match.group(1)
                jobs_data = json.loads(json_str)
                
                # Parse and structure the job information
                jobs = []
                for job in jobs_data:
                    job_info = {
                        'title': job.get('name'),
                        'department': job.get('department'),
                        'location': self._parse_location(job.get('location', {})),
                        'employment_type': job.get('employment_type'),
                        'experience_level': job.get('experience_level'),
                        'workplace_type': job.get('workplace_type'),
                        'uid': job.get('uid'),
                        'url': job.get('url_comeet_hosted_page'),
                        'company_name': job.get('company_name'),
                        'last_updated': job.get('time_updated'),
                        'description': self._parse_custom_fields(job.get('custom_fields', {}))
                    }
                    jobs.append(job_info)
                
                return jobs
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Error parsing JS variable: {e}")
        
        return []
    
    def _extract_from_html_elements(self) -> List[Dict]:
        """
        Extract job data from HTML elements (alternative pattern).
        Handles multiple common patterns including Angular-based job listings.
        """
        jobs = []
        
        # Pattern 1: Angular/Comeet positionItem links
        job_links = self.soup.find_all('a', class_='positionItem')
        
        for link in job_links:
            # Extract title
            title_elem = link.find('span', class_='positionLink')
            title = title_elem.get_text(strip=True) if title_elem else None
            
            # Extract URL
            url = link.get('href') or link.get('ng-href')
            
            # Extract details from the list
            details_list = link.find('ul', class_='positionDetails')
            location = None
            experience_level = None
            employment_type = None
            
            if details_list:
                items = details_list.find_all('li')
                for item in items:
                    text = item.get_text(strip=True)
                    
                    # Check if it contains location icon
                    if item.find('i', class_='fa-map-marker'):
                        location = text
                    # Check for common employment type keywords
                    elif any(keyword in text.lower() for keyword in ['full-time', 'part-time', 'contract', 'temporary', 'freelance']):
                        employment_type = text
                    # Check for experience level keywords
                    elif any(keyword in text.lower() for keyword in ['senior', 'junior', 'mid-level', 'entry', 'lead', 'principal', 'intern']):
                        experience_level = text
                    # If none of the above, try to infer
                    else:
                        # If it's a short text without special chars, might be experience or type
                        if len(text.split()) <= 2:
                            if not experience_level:
                                experience_level = text
                            elif not employment_type:
                                employment_type = text
            
            job_info = {
                'title': title,
                'location': location,
                'employment_type': employment_type,
                'experience_level': experience_level,
                'url': url
            }
            
            # Only add if we found at least a title
            if job_info['title']:
                jobs.append(job_info)
        
        # If no jobs found with Pattern 1, try Pattern 2: Generic job cards
        if not jobs:
            job_cards = self.soup.find_all('div', class_=['job-card', 'job-listing', 'job-item', 'position-card'])
            
            for card in job_cards:
                job_info = {
                    'title': self._safe_extract(card, ['h2', 'h3', '.job-title', '.position-title']),
                    'department': self._safe_extract(card, ['.department', '.team', '.category']),
                    'location': self._safe_extract(card, ['.location', '.job-location']),
                    'employment_type': self._safe_extract(card, ['.employment-type', '.job-type']),
                    'url': self._extract_link(card)
                }
                
                # Only add if we found at least a title
                if job_info['title']:
                    jobs.append(job_info)
        
        return jobs
    
    
    def _parse_location(self, location_dict: Dict) -> str:
        """Parse location dictionary into readable string."""
        if not location_dict:
            return "Not specified"
        
        parts = []
        if location_dict.get('city'):
            parts.append(location_dict['city'])
        if location_dict.get('country'):
            parts.append(location_dict['country'])
        
        if location_dict.get('is_remote'):
            parts.append("(Remote)")
        
        return ", ".join(parts) if parts else location_dict.get('name', 'Not specified')
    
    def _parse_custom_fields(self, custom_fields: Dict) -> Dict:
        """Extract description and requirements from custom fields."""
        result = {}
        
        if 'details' in custom_fields:
            for detail in custom_fields['details']:
                name = detail.get('name', '').lower()
                value = detail.get('value', '')
                
                # Skip if value is None or not a string
                if value and isinstance(value, str):
                    # Remove HTML tags for cleaner text
                    clean_value = BeautifulSoup(value, 'html.parser').get_text(separator='\n').strip()
                    result[name] = clean_value
        
        return result
    
    def _safe_extract(self, element, selectors: List[str]) -> Optional[str]:
        """Safely extract text from element using multiple selector attempts."""
        for selector in selectors:
            try:
                if selector.startswith('.'):
                    found = element.find(class_=selector[1:])
                else:
                    found = element.find(selector)
                
                if found:
                    return found.get_text(strip=True)
            except:
                continue
        return None
    
    def _extract_link(self, element) -> Optional[str]:
        """Extract job URL from element."""
        link = element.find('a', href=True)
        return link['href'] if link else None


def generate_url_hash(url: str) -> str:
    """Generate a hash from a URL for unique identification."""
    if not url:
        return ""
    return hashlib.md5(url.encode('utf-8')).hexdigest()


def add_hash_to_jobs(jobs: List[Dict]) -> List[Dict]:
    """Add a hash field to each job based on its URL."""
    for job in jobs:
        job['url_hash'] = generate_url_hash(job.get('url', ''))
    return jobs


def merge_jobs(new_jobs: List[Dict], existing_jobs: List[Dict]) -> List[Dict]:
    """
    Merge new jobs with existing jobs, avoiding duplicates based on URL.
    
    Args:
        new_jobs: List of newly extracted jobs
        existing_jobs: List of existing jobs
        
    Returns:
        Merged list of unique jobs
    """
    # Create a set of existing URL hashes for quick lookup
    existing_hashes = {job.get('url_hash', '') for job in existing_jobs}
    
    # Filter out jobs that already exist
    unique_new_jobs = [
        job for job in new_jobs 
        if job.get('url_hash', '') not in existing_hashes
    ]
    
    # Combine existing and new unique jobs
    merged_jobs = existing_jobs + unique_new_jobs
    
    return merged_jobs


def save_jobs_to_db(jobs: List[Dict], source: str = 'comeet') -> tuple[int, int]:
    """
    Save jobs to the database.
    
    Args:
        jobs: List of job dictionaries
        source: Source of the jobs (e.g., 'comeet', 'google_api')
        
    Returns:
        Tuple of (inserted_count, duplicate_count)
    """
    # Initialize database if needed
    try:
        initialize_database(COMPANIES_PAGES_DB, get_jobs_schema())
    except Exception as e:
        print(f"Warning: Could not initialize database: {e}")
        return 0, 0
    
    jobs_db = JobsDB()
    inserted = 0
    duplicates = 0
    
    for job in jobs:
        # Prepare job data for database
        job_data = {
            'url': job.get('url', ''),
            'title': job.get('title', ''),
            'company': job.get('company_name', job.get('company', '')),
            'company_name': job.get('company_name', job.get('company', '')),
            'source': source,
            'department': job.get('department'),
            'location': job.get('location'),
            'employment_type': job.get('employment_type'),
            'experience_level': job.get('experience_level'),
            'workplace_type': job.get('workplace_type'),
            'uid': job.get('uid'),
            'last_updated': job.get('last_updated'),
        }
        
        # Handle description (might be a dict)
        if isinstance(job.get('description'), dict):
            job_data['description'] = json.dumps(job['description'])
        else:
            job_data['description'] = job.get('description')
        
        # Generate job hash
        job_data['job_hash'] = generate_job_hash(job_data['url'], job_data['title'])
        
        # Try to insert
        result = jobs_db.insert_job(job_data)
        if result:
            inserted += 1
        else:
            duplicates += 1
            # Update last_checked for duplicate
            jobs_db.update_job_checked(job_data['job_hash'])
    
    return inserted, duplicates


def load_existing_jobs(filepath: str) -> List[Dict]:
    """Load existing jobs from file if it exists."""
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                jobs = json.load(f)
                # Ensure all jobs have hashes
                for job in jobs:
                    if 'url_hash' not in job or not job.get('url_hash'):
                        job['url_hash'] = generate_url_hash(job.get('url', ''))
                return jobs
        except json.JSONDecodeError:
            print(f"Warning: Could not parse {filepath}, starting with empty list")
            return []
    return []


if __name__ == "__main__":
    import os

    # Setup logging
    logger = setup_logging()
    
    logger.info("Starting job scraping process")
    print("Current working directory:", os.getcwd())
    print("Files in this directory:", os.listdir())

    # Load existing jobs from data/jobs_raw.json
    existing_jobs = load_existing_jobs('data/jobs_raw.json')
    logger.info(f"Loaded {len(existing_jobs)} existing jobs from data/jobs_raw.json")
    print(f"Loaded {len(existing_jobs)} existing jobs from data/jobs_raw.json")

    # Then your load command:
    with open('scripts/debug_dream.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    

    extractor = JobExtractor(html_content)
    new_jobs = extractor.extract_jobs()
    
    # Add hash to new jobs
    new_jobs = add_hash_to_jobs(new_jobs)
    
    logger.info(f"Extracted {len(new_jobs)} jobs from HTML")
    print(f"Found {len(new_jobs)} jobs in HTML")
    
    # Merge with existing jobs
    all_jobs = merge_jobs(new_jobs, existing_jobs)
    
    # Calculate how many were actually added (unique by url_hash)
    jobs_added = len(all_jobs) - len(existing_jobs)
    
    # Print results
    logger.info(f"Total jobs after merging: {len(all_jobs)}, New jobs added: {jobs_added}")
    print(f"\nTotal jobs after merging: {len(all_jobs)}")
    print(f"New jobs added: {jobs_added}\n")
    
    for i, job in enumerate(all_jobs, 1):
        print(f"Job {i}:")
        print(f"  Title: {job.get('title')}")
        print(f"  Department: {job.get('department')}")
        print(f"  Location: {job.get('location')}")
        print(f"  Type: {job.get('employment_type')}")
        print(f"  Experience: {job.get('experience_level')}")
        print(f"  Workplace: {job.get('workplace_type')}")
        print(f"  URL: {job.get('url')}")
        print(f"  URL Hash: {job.get('url_hash')}")
        
        # Print description if available
        if isinstance(job.get('description'), dict):
            if 'description' in job['description']:
                print(f"  Description: {job['description']['description'][:200]}...")
        
        print()
    
    # Save to database
    logger.info("Saving jobs to database...")
    db_inserted, db_duplicates = save_jobs_to_db(all_jobs, source='comeet')
    logger.info(f"Database: {db_inserted} jobs inserted, {db_duplicates} duplicates found")
    print(f"\nDatabase: {db_inserted} jobs inserted, {db_duplicates} duplicates found")
    
    # Also save to JSON as backup (for now)
    with open('data/jobs_raw.json', 'w', encoding='utf-8') as f:
        json.dump(all_jobs, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Successfully saved {len(all_jobs)} jobs to data/jobs_raw.json ({jobs_added} new jobs added)")
    print(f"Jobs also saved to data/jobs_raw.json (backup)")

    # soup = BeautifulSoup(html_content, "html.parser")
    # job_posting = soup.find('a', class_='positionItem')
    
    # for a in soup.select("a.positionItem"):
    #     link = a.get("href")
    #     position_id = link.split("/")[-1] if link else None
    #     details = [li.get_text(strip=True) for li in a.select("ul.positionDetails li")]
    #     experience = next((d for d in details if d in ["Intern", "Junior", "Senior"]), None)
    #     employment = next((d for d in details if d in ["Full-time", "Part-time", "Contract"]), None)
    #     print(position_id, link, experience, employment)
        
    