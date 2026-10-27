"""A retained report must keep the subject of the acquisition that produced it."""

from pathlib import Path
from typing import Never

import httpx
import pytest
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, source_definition
from test_storage import repo as repo

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_reports import SourceReportIntent, SourceReportLedger
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import StorageBackend
from tank.util import canonical_json

OLD = "https://source.example/old-feed"
NEW = "https://source.example/new-feed"
BROKEN = b'<rss version="2.0"><channel><title>Original source</title><item><title>Broken entry</title><description>No declared identity</description></item></channel></rss>'
EMPTY = b'<rss version="2.0"><channel><title>New source</title></channel></rss>'


class ReportSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def bind(service: SourceService, url: str, name: str) -> None:
    service.sources = {
        "S30": source_definition({"source_id": "S30", "name": name, "feeds": [{"url": url, "kind": "rss"}]})
    }


async def test_failed_report_restart_retains_original_feed_subject(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    requests: list[str] = []

    def response(http_request: httpx.Request) -> httpx.Response:
        url = str(http_request.url)
        assert url in {OLD, NEW}
        requests.append(url)
        return httpx.Response(200, content=BROKEN if url == OLD else EMPTY)

    async def unavailable(intent: SourceReportIntent) -> Never:
        raise OSError("Authored report sink outage")

    settings = ReportSettings(data_dir=tmp_path, ops_progress_enabled=True)
    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            original = SourceService(
                repo,
                settings,
                http=http,
                budgets=budgets,
                versions=ResolvedVersionIdentity(config_version="scope-old", code_version="scope-test"),
            )
            bind(original, OLD, "Original test publisher")
            with monkeypatch.context() as outage:
                outage.setattr(original, "report_error_intent", unavailable)
                assert (await original.poll_once("S30")).items == 0
            checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
            assert checkpoint is not None
            backlog = checkpoint["entry_error_reporting"]
            ledger = SourceReportLedger.model_validate(backlog)
            assert len(ledger.pending) == 1
            [intent] = ledger.pending.values()
            assert intent.subject.feed_url == OLD and intent.subject.source_name == "Original test publisher"
            assert intent.versions.config_version == "scope-old"
            request.node.user_properties.append(("source_error_backlog", canonical_json(backlog)))
            assert await repo.list("out.ops_notices") == []
            await original.close()
            restarted = SourceService(
                repo,
                settings,
                http=http,
                budgets=budgets,
                versions=ResolvedVersionIdentity(config_version="scope-new", code_version="scope-test"),
            )
            bind(restarted, NEW, "Renamed test publisher")
            assert (await restarted.poll_once("S30")).items == 0
            [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
            assert notice["feed_url"] == OLD
            assert notice["source_name"] == "Original test publisher"
            assert notice["source_id"] == "S30"
            assert requests == [OLD, NEW]
            settled = await repo.get("ops.source_checkpoints", "S30:0")
            assert settled is not None
            ledger = SourceReportLedger.model_validate(settled["entry_error_reporting"])
            assert ledger.pending == {} and ledger.last_outcome is not None
            assert ledger.last_outcome.outcome.kind == "RECORDED"
            assert ledger.last_outcome.outcome.scope.feed_url == OLD
            await restarted.close()
    finally:
        await budgets.shutdown()
