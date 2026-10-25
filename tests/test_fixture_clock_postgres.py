"""Exercise the historical clock view and source search through actual storage."""

import os
from pathlib import Path
from runpy import run_path

import pytest
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission, fixture_versions

from tank.analysis.page_access import LivePageAccess
from tank.event_tools import EventToolService
from tank.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


async def test_reference_clock_view_and_search_with_actual_postgres(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, postgres_template
) -> None:
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL QA connection")
    async with postgres_template.repository(dsn) as repo:
        service = EventToolService(
            repo,
            Settings(data_dir=tmp_path),
            None,
            None,
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
        case = run_path(str(Path(__file__).with_name("test_event_tools.py")))[
            "test_trusted_reference_clock_changes_logical_age_only"
        ]
        await case(service, tmp_path, monkeypatch)
