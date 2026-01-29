from __future__ import annotations

import asyncio
import json
import logging

import httpx

from app.common.utils import load_config
from app.services.db_utils import CompaniesDB
from app.services.message_queue import CompanyQueue


##TODO Change it to the proper module.
logger = logging.getLogger(__name__)


###TODO:
# class Company(Base):
#     __tablename__ = "companies"
#     id = Column(Integer, primary_key=True)
#     company_name = Column(String, nullable=False)
#     company_page_url = Column(String, unique=True)
#     last_scraped = Column(DateTime)
#     # ...


DOMAIN_QUERIES = {
    # Domain-specific search templates
    "comeet.com": 'site:comeet.com intitle:"jobs at"',
    "lever.co": "site:jobs.lever.co -inurl:/job-",
    ## "greenhouse.io": 'site:boards.greenhouse.io OR site:greenhouse.io intitle:"jobs"',
}

URL_PATTERNS = {
    "comeet.com": "https://www.comeet.com/jobs/{company_name}/{company_id}",
    "lever.co": "https://jobs.lever.co/{company_name}",
}


class CompanyManager:
    """This class is responsible for managing companies.
    it will use the companies_db to get the companies to scrape and the config to get the max companies per run and the max age hours.
    it will publish the companies to the company_queue.
    it will also consume the companies from the company_queue and scrape the jobs.
    """

    def __init__(self, companies_db: CompaniesDB, config: dict, company_queue: CompanyQueue):
        self.companies_db = companies_db
        self.config = config
        self.company_queue = company_queue

    def get_domains_to_search(self) -> list[str]:
        """Get the domains to search for job pages."""

        # For now only Comeet is supported, but we can add more later
        # TODO: Add more domains

        # "greenhouse.io",
        # "lever.co",
        # "workday.com",
        # "bamboohr.com"

        return ["comeet.com"]

    ##TODO: we are posting to an async rabbit mq but our function is not async
    async def publish_stale_companies(self) -> None:
        """Select companies to scrape and publish them to the company_queue."""
        max_age_hours = self.config.get("max_age_hours", 24)
        companies = self.companies_db.get_stale_companies(max_age_hours)
        if not companies:
            logger.info("No companies require scraping at this time.")
            return
        logger.info(f"Preparing to scrape {len(companies)} company pages")
        for company in companies:
            await self.company_queue.publish(company)

    async def search_companies_in_domain(self, domain: str) -> None:
        """Search for companies in a domain."""

        serper_api_key = self.config["serper_api_key"]
        headers = {"X-API-KEY": serper_api_key, "Content-Type": "application/json"}

        query = DOMAIN_QUERIES[domain]
        logger.info(f"Searching for companies in {domain} with query {query}")
        async with httpx.AsyncClient() as client:
            tasks = [self.fetch_companies_from_page(query, page, headers, client, domain) for page in range(1, 51)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        total_new = sum(res for res in results if isinstance(res, int))
        logger.info(f"Finished domain {domain}. Found {total_new} new companies.")

    async def fetch_companies_from_page(
        self, query: str, page: int, headers: dict, client: httpx.AsyncClient, domain: str
    ) -> None:
        """Fetch companies from a page."""

        # Prepare Serper API request
        url = "https://google.serper.dev/search"
        payload = json.dumps([{"q": query, "page": page}])

        # Make API request
        response = await client.post(url, headers=headers, data=payload, timeout=30)
        response.raise_for_status()
        await asyncio.sleep(0.5)

        # Parse response
        search_results = response.json()[0]

        if not search_results or "organic" not in search_results:
            logger.warning(f"No organic results found for page {page}")
            return 0

        new_companies_count = self._process_search_results(search_results["organic"], domain)
        return new_companies_count

    def _process_search_results(self, organic_results: list[dict], domain: str) -> int:
        """Process search results, extract company info, and insert into database."""
        new_companies_count = 0

        for result in organic_results:
            try:
                title = result.get("title", "")
                link = result.get("link", "")

                company_name = self._extract_company_name_from_title(title, domain)

                if company_name and link:
                    # Clean the URL to remove job-specific paths
                    clean_url = self._clean_company_page_url(link)

                    company_data = {
                        "company_name": company_name,
                        "domain": domain,
                        "company_page_url": clean_url,
                        "title": title,
                        "source": "google_serper",
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
        if not title:
            return None

        name = title
        if domain == "comeet.com":
            # Format: "Jobs at {name} - Comeet"
            name = name.replace("Jobs at ", "").replace(" - Comeet", "")

        # General cleaning for all domains
        name = name.strip()

        # Lever titles are just the company name

        # Remove trailing separators and extra whitespace
        # This handles cases like "Company Name -" or "Company Name |"
        while name and name[-1] in ("-", "|", ":", "·"):
            name = name[:-1].strip()

        return name if name else None

    def _clean_company_page_url(self, url: str) -> str:
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
            if "comeet.com/jobs/" in url:
                parts = url.split("/")
                if len(parts) >= 6:  # https://www.comeet.com/jobs/company/id
                    # Keep only up to the company ID
                    return "/".join(parts[:6])
            if "lever.co/jobs/" in url:
                parts = url.split("/")
                if len(parts) >= 3:  # https://lever.co/company
                    # Keep only up to the company ID
                    return "/".join(parts[:3])

            return url
        except Exception:
            return url


if __name__ == "__main__":
    companies_db = CompaniesDB()
    config = load_config()
    company_queue = CompanyQueue()
    company_manager = CompanyManager(companies_db, config, company_queue)

