from __future__ import annotations

from scripts.db_utils import generate_url_hash
from scripts.job_filter_embedder import JobFilter


def test_generate_url_hash_is_deterministic():
    url = "https://example.com/jobs/12345"
    assert generate_url_hash(url) == generate_url_hash(url)


def test_generate_url_hash_varies_with_input():
    base = "https://example.com/job"
    assert generate_url_hash(base + "?a=1") != generate_url_hash(base + "?a=2")


def test_generate_url_hash_handles_empty_url():
    assert generate_url_hash("") == ""


def test_is_hebrew_job_detects_hebrew_text():
    hebrew_job = {"title": "מהנדס תוכנה", "description": "משרה"}
    english_job = {"title": "Software Engineer", "description": "English"}
    assert JobFilter.is_hebrew_job(hebrew_job) is True
    assert JobFilter.is_hebrew_job(english_job) is False


def test_filter_valid_jobs_filters_hebrew_and_general_jobs():
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
