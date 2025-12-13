from __future__ import annotations

import json
from pathlib import Path

import pytest


SNAPSHOT_DIR = Path(__file__).parent / "fixtures" / "parsed_snapshots"


def test_html_fixtures_parse(parse_fixture_jobs, html_fixture_names):
    """Ensure every HTML sample produces at least one structured job.
    Basic test to guarantee that the scraper didn't break."""


    for name in html_fixture_names:
        jobs = parse_fixture_jobs(name)
        assert isinstance(jobs, list)
        assert jobs, f"Expected at least one job in fixture '{name}'"
        for job in jobs:
            assert job.get("title"), f"Fixture '{name}' yielded job without title"
            assert job.get("url_hash") is not None, "url_hash should always be set"


def test_parsed_jobs_match_snapshots(parse_fixture_jobs, html_fixture_names):
    """Compare parsed job payloads against stored snapshots when available."""

    missing_snapshots = []

    for name in html_fixture_names:
        current_payload = parse_fixture_jobs(name)
        snapshot_path = SNAPSHOT_DIR / f"{name}.json"

        if not snapshot_path.exists():
            missing_snapshots.append(snapshot_path)
            continue

        expected_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert current_payload == expected_payload, (
            f"Parsed jobs for '{name}' differ from snapshot at {snapshot_path}. "
            "Run pytest --update-job-snapshots if the changes are expected."
        )

    if missing_snapshots:
        formatted = ", ".join(str(path) for path in missing_snapshots)
        pytest.skip(
            "Missing parsed job snapshots: "
            f"{formatted}. Run pytest --update-job-snapshots to generate them for review."
        )


