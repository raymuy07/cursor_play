"""Pytest shared fixtures for HTML parsing and snapshot generation."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Callable, Dict, Iterable, List

import pytest


# Ensure project root is importable when tests run from the repository root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from scripts.db_schema import get_companies_schema, get_jobs_schema  # noqa: E402
from scripts.db_utils import CompaniesDB, JobsDB, initialize_database  # noqa: E402
from scripts.scrape_jobs import JobExtractor, add_hash_to_jobs  # noqa: E402


FIXTURES_DIR = Path(__file__).parent / "fixtures"
SNAPSHOT_DIR = FIXTURES_DIR / "parsed_snapshots"


def _sanitize_job(job: Dict) -> Dict:
    """Drop empty fields for cleaner snapshot comparisons."""

    sanitized: Dict = {}
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


def _sanitize_jobs(jobs: Iterable[Dict]) -> List[Dict]:
    return [_sanitize_job(job) for job in jobs]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-job-snapshots",
        action="store_true",
        default=False,
        help="Regenerate parsed job snapshots from HTML fixtures.",
    )


@pytest.fixture(scope="session")
def html_fixture_names() -> List[str]:
    """All HTML fixture stem names available under `tests/fixtures`."""

    return sorted(path.stem for path in FIXTURES_DIR.glob("*.html"))


@pytest.fixture(scope="session")
def load_fixture_html() -> Callable[[str], str]:
    """Factory that returns the raw HTML contents for a given fixture name."""

    def _loader(name: str) -> str:
        path = FIXTURES_DIR / f"{name}.html"
        if not path.exists():
            raise FileNotFoundError(f"Unknown HTML fixture: {name}")
        return path.read_text(encoding="utf-8")

    return _loader


@pytest.fixture(scope="session")
def parse_fixture_jobs(
    request: pytest.FixtureRequest, load_fixture_html
) -> Callable[[str], List[Dict]]:
    """Parse jobs from an HTML fixture and optionally persist a JSON snapshot."""

    update_snapshots = request.config.getoption("--update-job-snapshots")

    def _parser(name: str, *, persist: bool | None = None) -> List[Dict]:
        html_content = load_fixture_html(name)
        extractor = JobExtractor(html_content)
        jobs = add_hash_to_jobs(extractor.extract_jobs())
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
def parsed_job_snapshots(parse_fixture_jobs, html_fixture_names) -> Dict[str, List[Dict]]:
    """Dictionary mapping fixture names to parsed job payloads."""

    parsed: Dict[str, List[Dict]] = {}
    for name in html_fixture_names:
        parsed[name] = parse_fixture_jobs(name)
    return parsed


@pytest.fixture()
def temp_companies_db(tmp_path) -> CompaniesDB:
    """Provision an isolated companies.db for tests."""

    db_path = tmp_path / "companies.db"
    initialize_database(str(db_path), get_companies_schema())
    return CompaniesDB(db_path=str(db_path))


@pytest.fixture()
def temp_jobs_db(tmp_path) -> JobsDB:
    """Provision an isolated jobs.db for tests."""

    db_path = tmp_path / "jobs.db"
    initialize_database(str(db_path), get_jobs_schema())
    return JobsDB(db_path=str(db_path))


