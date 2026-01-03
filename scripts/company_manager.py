import logging
from typing import List, Dict
from datetime import datetime
from scripts.db_utils import CompaniesDB
from scripts.queue import CompanyQueue
from common.utils import load_config

import json
import requests


logger = logging.getLogger(__name__)


class CompanyManager:

    """This class is responsible for managing companies.
    it will use the companies_db to get the companies to scrape and the config to get the max companies per run and the max age hours.
    it will publish the companies to the company_queue.
    it will also consume the companies from the company_queue and scrape the jobs.
    """
    def __init__(self, companies_db: CompaniesDB, config: Dict, company_queue: CompanyQueue):
        self.companies_db = companies_db
        self.config = config
        self.company_queue = company_queue



    def get_domains_to_search(self) -> List[str]:
        """Get the domains to search for job pages."""

        # For now only Comeet is supported, but we can add more later
        # TODO: Add more domains

        #"greenhouse.io",
        #"lever.co",
        #"workday.com",
        #"bamboohr.com"

        return ["comeet.com"]

    @property
    def publish_stale_companies(self) -> List[Dict]:
        """Select companies to scrape and publish them to the company_queue."""
        max_age_hours = self.config.get('max_age_hours')
        companies = self.companies_db.get_stale_companies(max_age_hours)
        if not companies:
            logger.info("No companies require scraping at this time.")
            return
        logger.info(f"Preparing to scrape {len(companies)} company pages")
        for company in companies:
            self.company_queue.publish(company)

    @property
    def search_for_companies(self) -> List[Dict]:
        """Search for companies on the web."""
        domains = self.get_domains_to_search()

        serper_api_key = self.config['serper_api_key']

        # Get domain-specific template or use default
        domain_templates = self.config['google_dork']['domain_templates']

        new_companies_count = 0
        total_results_count = 0  # Track total results across all pages

        for domain in domains:

            if domain not in domain_templates:
                logger.error(f"No specific template for {domain}")
                continue

            query = domain_templates[domain]['query_template']

            for page in range(1,51): ##51 is the max pages i can allow.
                try:
                    # Prepare Serper API request
                    url = "https://google.serper.dev/search"

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
                    page_new_count = self._process_search_results(search_results['organic'], domain)
                    new_companies_count += page_new_count

                    # If we got fewer than 10 results, we've likely reached the end
                    if len(search_results['organic']) < 10:
                        logger.info(f"Reached end of results for {domain} at page {page}")
                        break

                except requests.exceptions.RequestException as e:
                    logger.error(f"API request failed for {domain} page {page}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing {domain} page {page}: {e}")
                    break

    def _process_search_results(self, organic_results: List[Dict], domain: str) -> int:
        """Process search results, extract company info, and insert into database."""
        new_companies_count = 0

        for result in organic_results:
            try:
                title = result.get('title', '')
                link = result.get('link', '')

                company_name = self._extract_company_name_from_title(title, domain)

                if company_name and link:
                    # Clean the URL to remove job-specific paths
                    clean_url = self._clean_job_page_url(link)

                    company_data = {
                        "company_name": company_name,
                        "domain": domain,
                        "job_page_url": clean_url,
                        "title": title,
                        "source": "google_serper"
                    }

                    company_id = self.companies_db.insert_company(company_data)
                    if company_id:
                        new_companies_count += 1
                        logger.debug(f"Added new company: {company_name}")
                    else:
                        logger.debug(f"Company already exists: {company_name}")

            except Exception as e:
                logger.error(f"Error processing search result: {e}")
                continue

        return new_companies_count

    def _extract_company_name_from_title(self, title: str, domain: str) -> str:
        """
        Extract company name from job page title.
        - Comeet: "Jobs at Flare - Comeet" -> "Flare"
        - Lever: title is the company name directly
        """
        if domain == "comeet.com":
            # Format: "Jobs at {name} - Comeet"
            name = title.replace("Jobs at ", "").replace(" - Comeet", "").strip()
            return name if name else None

        if domain == "lever.co":
            # Lever titles are just the company name
            return title.strip() if title else None

        # Fallback for other domains
        return title.strip() if title else None


    def _clean_job_page_url(self, url: str) -> str:
        """
        Clean job page URL to remove job-specific paths
        Examples:
        - "https://www.comeet.com/jobs/syqe/F1.00C/back-end-developer/3B.01B" -> "https://www.comeet.com/jobs/syqe/F1.00C"
        - "https://www.comeet.com/jobs/flare/36.00F" -> "https://www.comeet.com/jobs/flare/36.00F"
        - "https://www.comeet.com/jobs/firefly/39.000?3433df04_page=8" -> "https://www.comeet.com/jobs/firefly/39.000"
        - "https://www.comeet.com/jobs/justt/36.001?coref=1.3.uC4_71F" -> "https://www.comeet.com/jobs/justt/36.001"
        """
        try:
            # For Comeet URLs, keep only up to the company ID part
            # Pattern: /jobs/{company_name}/{company_id}/...
            if 'comeet.com/jobs/' in url:
                parts = url.split('/')
                if len(parts) >= 6:  # https://www.comeet.com/jobs/company/id
                    # Keep only up to the company ID
                    return '/'.join(parts[:6])
            if 'lever.co/jobs/' in url:
                parts = url.split('/')
                if len(parts) >= 3:  # https://lever.co/company
                    # Keep only up to the company ID
                    return '/'.join(parts[:3])

            return url
        except Exception:
            return url



if __name__ == "__main__":
    companies_db = CompaniesDB()
    config = load_config()
    company_queue = CompanyQueue()
    company_manager = CompanyManager(companies_db, config, company_queue)

    ##For now the company manager is being initialized but I still need to figure out the timings.
    ##cause we got publishing and we also have searching.
