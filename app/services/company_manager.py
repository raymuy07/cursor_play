from __future__ import annotations

import json
import logging

import httpx
import asyncio
from app.core.db_utils import CompaniesDB
from app.core.message_queue import CompanyQueue

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

    async def search_for_companies(self) -> None:
        """Search all supported domains for new companies. Called by scheduler weekly."""
        domains = self.get_domains_to_search()
        for domain in domains:
            await self.search_companies_in_domain(domain)

    async def search_companies_in_domain(self, domain: str) -> None:
        """Search for companies in a domain, page by page, until results run out."""
        serper_api_key = self.config["serper_api_key"]
        headers = {"X-API-KEY": serper_api_key, "Content-Type": "application/json"}
        query = DOMAIN_QUERIES[domain]

        logger.info(f"Searching for companies in {domain} with query: {query}")

        total_new = 0
        async with httpx.AsyncClient() as client:
            for page in range(1, 51):
                new_count, result_count = await self._fetch_companies_from_page(
                    query, page, headers, client, domain
                )
                total_new += new_count
                await asyncio.sleep(1)

                # If fewer than 10 results, we've reached the end
                if result_count < 10:
                    logger.info(f"Reached end of results at page {page} ({result_count} results)")
                    break

        logger.info(f"Finished domain {domain}. Found {total_new} new companies.")

    async def _fetch_companies_from_page(
        self, query: str, page: int, headers: dict, client: httpx.AsyncClient, domain: str
    ) -> tuple[int, int]:
        """Fetch companies from a single page.

        Returns:
            tuple: (new_companies_count, search_results_count)
        """
        url = "https://google.serper.dev/search"
        payload = json.dumps([{"q": query, "page": page}])

        try:
            response = await client.post(url, headers=headers, data=payload, timeout=30)
            response.raise_for_status()

            search_results = response.json()[0]

            if not search_results or "organic" not in search_results:
                logger.debug(f"No organic results found for page {page}")
                return 0, 0

            organic = search_results["organic"]
            new_count = self._process_search_results(organic, domain)
            return new_count, len(organic)

        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error on page {page}: {e.response.status_code}")
            return 0, 0
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            return 0, 0

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
