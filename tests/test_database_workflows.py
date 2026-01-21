from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.job_filter_parser import JobFilter, JobPersister


def test_companies_db_crud(temp_companies_db):
    company_data = {
        "company_name": "Example Co",
        "domain": "comeet",
        "company_page_url": "https://example.com/jobs",
        "title": "Jobs at Example Co",
        "source": "unit-test",
    }

    company_id = temp_companies_db.insert_company(company_data)
    assert company_id is not None

    duplicate_id = temp_companies_db.insert_company(company_data)
    assert duplicate_id is None

    assert temp_companies_db.update_last_scraped(company_data["company_page_url"]) is True
    stored = temp_companies_db.get_company_by_url(company_data["company_page_url"])
    assert stored is not None
    assert stored["last_scraped"] is not None

    assert temp_companies_db.mark_company_inactive(company_data["company_page_url"]) is True
    stored = temp_companies_db.get_company_by_url(company_data["company_page_url"])
    assert stored["is_active"] == 0

    assert temp_companies_db.delete_company_by_url(company_data["company_page_url"]) is True
    assert temp_companies_db.get_company_by_url(company_data["company_page_url"]) is None
    assert temp_companies_db.delete_company_by_url(company_data["company_page_url"]) is False


def test_jobs_db_insert_duplicate_delete(temp_jobs_db):
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

    job_id = temp_jobs_db.insert_job(job_data)
    assert job_id is not None

    duplicate_id = temp_jobs_db.insert_job(job_data)
    assert duplicate_id is None

    stored = temp_jobs_db.get_job_by_url(job_data["url"])
    assert stored is not None
    assert stored["title"] == job_data["title"]
    assert stored["department_id"] is not None
    assert stored["location_id"] is not None

    assert temp_jobs_db.delete_job_by_url(job_data["url"]) is True
    assert temp_jobs_db.get_job_by_url(job_data["url"]) is None
    assert temp_jobs_db.delete_job_by_url(job_data["url"]) is False


def test_save_jobs_flow_with_html_fixtures(temp_jobs_db, parse_fixture_jobs, html_fixture_names):
    if not html_fixture_names:
        pytest.skip("No HTML fixtures available")

    fixture_name = html_fixture_names[0]
    parsed_jobs = parse_fixture_jobs(fixture_name)

    valid_jobs, filter_counts = JobFilter.filter_valid_jobs(deepcopy(parsed_jobs))

    success, inserted, skipped = JobPersister.save_jobs_to_db(deepcopy(parsed_jobs), temp_jobs_db)

    assert inserted + skipped == len(valid_jobs)
    assert temp_jobs_db.count_jobs() == inserted

    success_again, inserted_again, skipped_again = JobPersister.save_jobs_to_db(deepcopy(parsed_jobs), temp_jobs_db)
    assert inserted_again == 0
    assert skipped_again == len(valid_jobs)
