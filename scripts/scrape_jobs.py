import json
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup


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
    
    

if __name__ == "__main__":
    import os

    print("Current working directory:", os.getcwd())
    print("Files in this directory:", os.listdir())

    # Then your load command:
    with open('scripts/debug_lumenis.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    

    extractor = JobExtractor(html_content)
    jobs = extractor.extract_jobs()
    
    # Print results
    print(f"Found {len(jobs)} jobs:\n")
    
    for i, job in enumerate(jobs, 1):
        print(f"Job {i}:")
        print(f"  Title: {job.get('title')}")
        print(f"  Department: {job.get('department')}")
        print(f"  Location: {job.get('location')}")
        print(f"  Type: {job.get('employment_type')}")
        print(f"  Experience: {job.get('experience_level')}")
        print(f"  Workplace: {job.get('workplace_type')}")
        print(f"  URL: {job.get('url')}")
        
        # Print description if available
        if isinstance(job.get('description'), dict):
            if 'description' in job['description']:
                print(f"  Description: {job['description']['description'][:200]}...")
        
        print()
    
    # Optionally save to JSON
    with open('jobs.json', 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    print(f"Jobs saved to jobs.json")

    # soup = BeautifulSoup(html_content, "html.parser")
    # job_posting = soup.find('a', class_='positionItem')
    
    # for a in soup.select("a.positionItem"):
    #     link = a.get("href")
    #     position_id = link.split("/")[-1] if link else None
    #     details = [li.get_text(strip=True) for li in a.select("ul.positionDetails li")]
    #     experience = next((d for d in details if d in ["Intern", "Junior", "Senior"]), None)
    #     employment = next((d for d in details if d in ["Full-time", "Part-time", "Contract"]), None)
    #     print(position_id, link, experience, employment)
        
    