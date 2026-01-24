"""Pytest shared fixtures for HTML parsing and snapshot generation."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from scripts.db_utils import generate_url_hash
import pytest

# Ensure project root is importable when tests run from the repository root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


FIXTURES_DIR = Path(__file__).parent / "fixtures"
SNAPSHOT_DIR = FIXTURES_DIR / "parsed_snapshots"


def _sanitize_job(job: dict) -> dict:
    """Drop empty fields for cleaner snapshot comparisons."""

    sanitized: dict = {}
    for key, value in job.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, dict):
            nested = {k: v for k, v in value.items() if v not in (None, "", [], {})}
            if nested:
                sanitized[key] = nested
            continue
        sanitized[key] = value
    return sanitized


def _sanitize_jobs(jobs: list[dict]) -> list[dict]:
    return [_sanitize_job(job) for job in jobs]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-job-snapshots",
        action="store_true",
        default=False,
        help="Regenerate parsed job snapshots from HTML fixtures.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests (require external services)")


@pytest.fixture(scope="session")
def html_fixture_names() -> list[str]:
    """All HTML fixture stem names available under `tests/fixtures`."""

    return sorted(path.stem for path in FIXTURES_DIR.glob("*.html"))


@pytest.fixture(scope="session")
def load_fixture_html() -> callable[[str], str]:
    """Factory that returns the raw HTML contents for a given fixture name."""

    def _loader(name: str) -> str:
        path = FIXTURES_DIR / f"{name}.html"
        if not path.exists():
            raise FileNotFoundError(f"Unknown HTML fixture: {name}")
        return path.read_text(encoding="utf-8")

    return _loader


@pytest.fixture(scope="session")
def parse_fixture_jobs(request: pytest.FixtureRequest, load_fixture_html) -> callable[[str], list[dict]]:
    """Parse jobs from an HTML fixture and optionally persist a JSON snapshot."""
    # Lazy imports to avoid cascading import errors for unrelated tests
    from scripts.job_scraper import JobScraper

    update_snapshots = request.config.getoption("--update-job-snapshots")

    def _parser(name: str, *, persist: bool | None = None) -> list[dict]:
        html_content = load_fixture_html(name)
        extractor = JobScraper(html_content)
        jobs = extractor.extract_jobs()
        for job in jobs:
            job["url_hash"] = generate_url_hash(job.get("url", ""), job.get("title", ""))
        sanitized = _sanitize_jobs(jobs)

        should_persist = persist if persist is not None else update_snapshots
        if should_persist:
            SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
            snapshot_path = SNAPSHOT_DIR / f"{name}.json"
            snapshot_path.write_text(
                json.dumps(sanitized, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

        return sanitized

    return _parser


@pytest.fixture(scope="session")
def parsed_job_snapshots(parse_fixture_jobs, html_fixture_names) -> dict[str, list[dict]]:
    """dictionary mapping fixture names to parsed job payloads."""

    parsed: dict[str, list[dict]] = {}
    for name in html_fixture_names:
        parsed[name] = parse_fixture_jobs(name)
    return parsed


@pytest.fixture()
def temp_companies_db(tmp_path):
    """Provision an isolated companies.db for tests."""
    from db.schema.db_schema import get_companies_schema
    from scripts.db_utils import CompaniesDB, initialize_database

    db_path = tmp_path / "companies.db"
    initialize_database(str(db_path), get_companies_schema())
    return CompaniesDB(db_path=str(db_path))


@pytest.fixture()
def temp_jobs_db(tmp_path):
    """Provision an isolated jobs.db for tests."""
    from db.schema.db_schema import get_jobs_schema
    from scripts.db_utils import JobsDB, initialize_database

    db_path = tmp_path / "jobs.db"
    initialize_database(str(db_path), get_jobs_schema())
    return JobsDB(db_path=str(db_path))
