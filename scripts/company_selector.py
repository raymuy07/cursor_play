import logging
from typing import List, Dict
from datetime import datetime
from scripts.db_utils import CompaniesDB
from scripts.queue import CompanyQueue

logger = logging.getLogger(__name__)


class CompanySelector:
    """This class is responsible for selecting companies to scrape.
    it will use the companies_db to get the companies to scrape and the config to get the max companies per run and the max age hours.
    it will publish the companies to the company_queue.
    """
    def __init__(self, companies_db: CompaniesDB, config: Dict, company_queue: CompanyQueue):
        self.companies_db = companies_db
        self.config = config
        self.company_queue = company_queue

    def _parse_timestamp(self, ts: str) -> float:
        """Parse timestamp string to unix timestamp float."""
        return datetime.fromisoformat(ts.replace(' ', 'T')).timestamp()

    def select_and_publish(self) -> List[Dict]:
        """Select companies to scrape and publish them to the company_queue."""
        max_age_hours = self.config.get('max_age_hours')
        companies = self.companies_db.get_stale_companies(max_age_hours)
        if not companies:
            logger.info("No companies require scraping at this time.")
            return
        logger.info(f"Preparing to scrape {len(companies)} company pages")
        for company in companies:
            self.company_queue.publish(company)
