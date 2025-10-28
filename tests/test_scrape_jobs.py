import os
from pathlib import Path

from scripts.scrape_jobs import JobExtractor, add_hash_to_jobs, merge_jobs


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture_html(filename: str) -> str:
    path = FIXTURES_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def extract_jobs_from_fixture(filename: str):
    html = load_fixture_html(filename)
    extractor = JobExtractor(html)
    return extractor.extract_jobs()


def test_job_extraction_from_fixtures():
    fixture_names = ["debug_dream.html", "debug_lumenis.html"]
    total_found = 0

    for name in fixture_names:
        jobs = extract_jobs_from_fixture(name)
        total_found += len(jobs)
        if jobs:
            # Basic shape checks on first job
            first = jobs[0]
            assert isinstance(first, dict)
            assert "title" in first and first["title"]

    # Ensure at least one fixture yields jobs
    assert total_found > 0


def test_add_hash_and_merge_behaviour():
    # Use whichever fixture yields jobs
    candidates = ["debug_dream.html", "debug_lumenis.html"]
    jobs = []
    for name in candidates:
        jobs = extract_jobs_from_fixture(name)
        if jobs:
            break

    if not jobs:
        # Nothing to validate here if fixtures change; don't fail the suite
        return

    jobs_with_hash = add_hash_to_jobs(jobs)
    assert all(j.get("url_hash") for j in jobs_with_hash)

    merged = merge_jobs(jobs_with_hash, [])
    assert len(merged) == len(jobs_with_hash)
