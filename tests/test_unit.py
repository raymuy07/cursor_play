"""
Unit tests for core components: utilities, filtering, database operations, and persistence.

These tests run without external services (no RabbitMQ, no HTTP).
"""

from __future__ import annotations

from app.core.db_utils import generate_url_hash
from app.services.job_filter import JobFilter

# ============================================================================
# URL Hash Utility Tests
# ============================================================================


class TestUrlHash:
    """Tests for generate_url_hash utility function."""

    def test_is_deterministic(self):
        url = "https://example.com/jobs/12345"
        assert generate_url_hash(url) == generate_url_hash(url)

    def test_varies_with_input(self):
        base = "https://example.com/job"
        assert generate_url_hash(base + "?a=1") != generate_url_hash(base + "?a=2")

    def test_handles_empty_url(self):
        assert generate_url_hash("") == ""


# ============================================================================
# Job Filter Tests
# ============================================================================


class TestJobFilter:
    """Tests for JobFilter filtering logic."""

    def test_is_hebrew_job_detects_hebrew_text(self):
        hebrew_job = {"title": "מהנדס תוכנה", "description": "משרה"}
        english_job = {"title": "Software Engineer", "description": "English"}
        assert JobFilter.is_hebrew_job(hebrew_job) is True
        assert JobFilter.is_hebrew_job(english_job) is False

    def test_filter_valid_jobs_filters_hebrew_and_general_jobs(self):
        jobs = [
            {
                "title": "Software Engineer",
                "department": "Engineering",
                "description": "Build features",
                "url": "https://example.com/eng",
                "location": "Tel Aviv, ISRAEL",
            },
            {
                "title": "General Role",
                "department": "General",
                "description": "",
                "url": "https://example.com/general",
                "location": "Tel Aviv, ISRAEL",
            },
            {
                "title": "מהנדס",
                "department": "Engineering",
                "description": "תיאור משרה",
                "url": "https://example.com/hebrew",
                "location": "Tel Aviv, ISRAEL",
            },
        ]

        valid_jobs, filter_counts = JobFilter.filter_valid_jobs(jobs)

        assert len(valid_jobs) == 1
        assert valid_jobs[0]["title"] == "Software Engineer"
        assert filter_counts["general_department"] == 1
        assert filter_counts["hebrew"] == 1


# ============================================================================
# Companies Database Tests
# ============================================================================


class TestCompaniesDB:
    """Tests for CompaniesDB CRUD operations."""

    def test_crud_operations(self, temp_companies_db):
        company_data = {
            "company_name": "Example Co",
            "domain": "comeet",
            "company_page_url": "https://example.com/jobs",
            "title": "Jobs at Example Co",
            "source": "unit-test",
        }

        # Insert
        company_id = temp_companies_db.insert_company(company_data)
        assert company_id is not None

        # Duplicate insert returns None
        duplicate_id = temp_companies_db.insert_company(company_data)
        assert duplicate_id is None

        # Update last scraped
        assert temp_companies_db.update_last_scraped(company_data["company_page_url"]) is True
        stored = temp_companies_db.get_company_by_url(company_data["company_page_url"])
        assert stored is not None
        assert stored["last_scraped"] is not None

        # Mark inactive
        assert temp_companies_db.mark_company_inactive(company_data["company_page_url"]) is True
        stored = temp_companies_db.get_company_by_url(company_data["company_page_url"])
        assert stored["is_active"] == 0

        # Delete
        assert temp_companies_db.delete_company_by_url(company_data["company_page_url"]) is True
        assert temp_companies_db.get_company_by_url(company_data["company_page_url"]) is None
        assert temp_companies_db.delete_company_by_url(company_data["company_page_url"]) is False


# ============================================================================
# Jobs Database Tests
# ============================================================================


class TestJobsDB:
    """Tests for JobsDB CRUD operations (async)."""

    async def test_insert_duplicate_delete(self, temp_jobs_db):
        job_data = {
            "title": "Test Software Engineer",
            "company_name": "Example Co",
            "department": "Engineering",
            "location": "Tel Aviv",
            "workplace_type": "Hybrid",
            "experience_level": "Senior",
            "employment_type": "Full-time",
            "description": "Build and ship features",
            "url": "https://example.com/jobs/engineer",
            "uid": "JOB-123",
        }

        # Insert
        job_id = await temp_jobs_db.insert_job(job_data)
        assert job_id is not None

        # Duplicate insert returns None
        duplicate_id = await temp_jobs_db.insert_job(job_data)
        assert duplicate_id is None

        # Retrieve and verify
        stored = await temp_jobs_db.get_job_by_url(job_data["url"])
        assert stored is not None
        assert stored["title"] == job_data["title"]
        assert stored["department_id"] is not None
        assert stored["location_id"] is not None

        # Delete
        assert await temp_jobs_db.delete_job_by_url(job_data["url"]) is True
        assert await temp_jobs_db.get_job_by_url(job_data["url"]) is None
        assert await temp_jobs_db.delete_job_by_url(job_data["url"]) is False
