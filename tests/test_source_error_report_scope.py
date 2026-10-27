"""A retained report must keep the subject of the acquisition that produced it."""

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Literal, Never

import httpx
import pytest
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, source_definition
from test_storage import repo as repo

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_reports import (
    SourceReportIntent,
    SourceReportLedger,
    SourceReportMarkerReference,
    SourceReportOutcome,
    SourceReportRecorded,
    SourceReportScope,
    source_report_notice_id,
)
from tank.models import Record
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.reporting import ReportSink
from tank.storage import StorageBackend
from tank.util import canonical_json, sha256

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


def versions(scope: str) -> ResolvedVersionIdentity:
    return ResolvedVersionIdentity(config_version="report-" + scope, code_version="report-test")


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
            assert ledger.last_outcome.source_versions.config_version == "scope-old"
            assert ledger.last_outcome.execution_versions.config_version == "scope-new"
            await restarted.close()
    finally:
        await budgets.shutdown()


@pytest.mark.parametrize(
    "boundary", ["BEFORE_INTENT", "AFTER_INTENT", "AFTER_REPORT", "DURING_ACK", "AFTER_ACK"]
)
async def test_report_intent_crash_boundaries_resume_without_duplicate_notice(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: Literal["BEFORE_INTENT", "AFTER_INTENT", "AFTER_REPORT", "DURING_ACK", "AFTER_ACK"],
) -> None:
    settings = ReportSettings(data_dir=tmp_path, ops_progress_enabled=True)
    budgets = fixture_request_budgets()
    requested: list[str] = []
    original_put = repo.put
    failed = False

    def response(http_request: httpx.Request) -> httpx.Response:
        url = str(http_request.url)
        assert url in {OLD, NEW}
        requested.append(url)
        return httpx.Response(200, content=BROKEN if url == OLD else EMPTY)

    async def fail_write(
        collection: str, key: str, record: Mapping[str, JsonValue], *, immutable: bool = False
    ) -> Record:
        nonlocal failed
        stored = await original_put(collection, key, record, immutable=immutable)
        value = record.get("entry_error_reporting")
        if (
            collection == "ops.source_checkpoints"
            and key == "S30:0"
            and isinstance(value, dict)
            and not failed
        ):
            ledger = SourceReportLedger.model_validate(value)
            if (
                boundary == "BEFORE_INTENT"
                and ledger.pending
                or boundary == "DURING_ACK"
                and not ledger.pending
                and ledger.last_outcome is not None
                and ledger.last_outcome.outcome.kind in {"RECORDED", "DEDUPLICATED"}
            ):
                failed = True
                raise OSError("Authored checkpoint boundary failure")
        return stored

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            original = SourceService(repo, settings, http=http, budgets=budgets, versions=versions("old"))
            bind(original, OLD, "Original test publisher")
            acknowledge = original.poller.reports._acknowledge

            async def stop_drain(source_id: str, key: str, sink: ReportSink) -> Never:
                raise asyncio.CancelledError

            async def stop_ack(
                key: str, intent: SourceReportIntent, revision: str, outcome: SourceReportOutcome
            ) -> Never:
                assert outcome.kind == "RECORDED"
                visible = await asyncio.create_task(repo.get("out.ops_notices", outcome.notice_id))
                assert visible is not None
                if boundary == "AFTER_ACK":
                    await acknowledge(key, intent, revision, outcome)
                raise asyncio.CancelledError

            with monkeypatch.context() as interruption:
                if boundary in {"BEFORE_INTENT", "DURING_ACK"}:
                    interruption.setattr(repo, "put", fail_write)
                    await original.poll_once("S30")
                    assert failed
                else:
                    if boundary == "AFTER_INTENT":
                        interruption.setattr(original.poller.reports, "drain", stop_drain)
                    else:
                        interruption.setattr(original.poller.reports, "_acknowledge", stop_ack)
                    with pytest.raises(asyncio.CancelledError):
                        await original.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            ledger = SourceReportLedger.model_validate(state["entry_error_reporting"])
            notices = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
            if boundary == "BEFORE_INTENT":
                assert ledger.pending == {} and notices == []
                assert not state.get("entry_errors") and "etag" not in state
            elif boundary == "AFTER_ACK":
                assert ledger.pending == {} and len(notices) == 1
            else:
                assert len(ledger.pending) == 1
                assert len(notices) == (0 if boundary == "AFTER_INTENT" else 1)
            await original.close()
            restarted = SourceService(repo, settings, http=http, budgets=budgets, versions=versions("new"))
            bind(restarted, OLD if boundary == "BEFORE_INTENT" else NEW, "Restarted publisher")
            await restarted.poll_once("S30")
            final = await repo.get("ops.source_checkpoints", "S30:0")
            assert final is not None
            assert SourceReportLedger.model_validate(final["entry_error_reporting"]).pending == {}
            [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
            assert notice["feed_url"] == OLD
            assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
            assert requested == [OLD, OLD if boundary == "BEFORE_INTENT" else NEW]
            await restarted.close()
    finally:
        await budgets.shutdown()


@pytest.mark.parametrize("legacy", ["UNSCOPED", "NULL", "TEXT", "OBJECT", "EMPTY"])
async def test_legacy_report_evidence_is_not_relabelled_or_removed_by_new_reports(
    repo: StorageBackend,
    tmp_path: Path,
    legacy: Literal["UNSCOPED", "NULL", "TEXT", "OBJECT", "EMPTY"],
) -> None:
    retained: JsonValue = (
        [
            {
                "entry_errors": [
                    {
                        "entry": {"excerpt": "No declared identity", "title": "Broken entry"},
                        "entry_index": 0,
                        "entry_sha256": "d41ae6748f7ba84158d6cb00da530d854da728772558eb3a4da43a9b5964a9bb",
                        "payload_sha256": "c8fee11f227ecb39048b744e067fe2763d0a1f643917fa0901fe0ce92430607b",
                        "reason": "ENTRY_WITHOUT_ID_OR_URL",
                    }
                ],
                "seen_at": "2026-10-27T00:54:28.518729Z",
            }
        ]
        if legacy == "UNSCOPED"
        else None
        if legacy == "NULL"
        else ""
        if legacy == "TEXT"
        else {}
        if legacy == "OBJECT"
        else []
    )
    body = EMPTY
    budgets = fixture_request_budgets()
    settings = ReportSettings(data_dir=tmp_path, ops_progress_enabled=True)
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
        ) as http:
            service = SourceService(
                repo, settings, http=http, budgets=budgets, versions=versions("legacy-reader")
            )
            bind(service, NEW, "Current publisher")
            await service.poll_once("S30")
            current = await repo.get("ops.source_checkpoints", "S30:0")
            assert current is not None
            await repo.put("ops.source_checkpoints", "S30:0", current | {"entry_error_reports": retained})
            body = BROKEN
            await service.poll_once("S30")
            current = await repo.get("ops.source_checkpoints", "S30:0")
            assert current is not None and current["entry_error_reports"] == retained
            ledger = SourceReportLedger.model_validate(current["entry_error_reporting"])
            assert ledger.pending == {}
            if legacy == "EMPTY":
                assert ledger.unresolved_legacy is None
            else:
                assert ledger.unresolved_legacy is not None
                assert ledger.unresolved_legacy.field_sha256 == sha256(retained)
                assert ledger.unresolved_legacy.reason == (
                    "UNSCOPED_LEGACY_REPORTS" if legacy == "UNSCOPED" else "MALFORMED_LEGACY_REPORTS"
                )
            [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
            assert notice["feed_url"] == NEW and notice["source_name"] == "Current publisher"
            assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
            await service.poll_once("S30")
            assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})) == 1
            current = await repo.get("ops.source_checkpoints", "S30:0")
            assert current is not None and current["entry_error_reports"] == retained
            await service.close()
    finally:
        await budgets.shutdown()


async def test_requested_report_waits_through_disabled_mode_and_resumes_original_scope(
    repo: StorageBackend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    budgets = fixture_request_budgets()
    requested: list[str] = []

    def response(http_request: httpx.Request) -> httpx.Response:
        url = str(http_request.url)
        assert url in {OLD, NEW}
        requested.append(url)
        return httpx.Response(200, content=BROKEN if url == OLD else EMPTY)

    async def unavailable(intent: SourceReportIntent) -> Never:
        raise OSError("Authored sink outage before mode change")

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            original = SourceService(
                repo,
                ReportSettings(data_dir=tmp_path, ops_progress_enabled=True),
                http=http,
                budgets=budgets,
                versions=versions("requested"),
            )
            bind(original, OLD, "Original publisher")
            with monkeypatch.context() as outage:
                outage.setattr(original, "report_error_intent", unavailable)
                await original.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            [pending] = SourceReportLedger.model_validate(state["entry_error_reporting"]).pending.values()
            disabled = SourceService(
                repo,
                ReportSettings(data_dir=tmp_path, ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=versions("disabled"),
            )
            bind(disabled, NEW, "Current publisher")
            await disabled.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            ledger = SourceReportLedger.model_validate(state["entry_error_reporting"])
            assert ledger.pending == {pending.intent_id: pending}
            assert ledger.last_outcome is not None and ledger.last_outcome.outcome.kind == "NOT_REQUESTED"
            assert ledger.last_outcome.intent_id == pending.intent_id
            assert ledger.last_outcome.source_versions == pending.versions
            assert ledger.last_outcome.execution_versions == versions("disabled")
            assert await repo.list("out.ops_notices") == []
            enabled = SourceService(
                repo,
                ReportSettings(data_dir=tmp_path, ops_progress_enabled=True),
                http=http,
                budgets=budgets,
                versions=versions("enabled"),
            )
            bind(enabled, NEW, "Current publisher")
            await enabled.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            assert SourceReportLedger.model_validate(state["entry_error_reporting"]).pending == {}
            [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
            assert notice["feed_url"] == OLD and notice["source_name"] == "Original publisher"
            assert requested == [OLD, NEW, NEW]
            await original.close()
            await disabled.close()
            await enabled.close()
    finally:
        await budgets.shutdown()


async def test_concurrent_acquisitions_commit_intents_and_one_actual_report(
    repo: StorageBackend, tmp_path: Path
) -> None:
    budgets = fixture_request_budgets()
    requests: list[str] = []

    def response(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == OLD
        requests.append(str(request.url))
        return httpx.Response(200, content=BROKEN)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
            settings = ReportSettings(data_dir=tmp_path, ops_progress_enabled=True)
            first = SourceService(repo, settings, http=http, budgets=budgets, versions=versions("one"))
            second = SourceService(repo, settings, http=http, budgets=budgets, versions=versions("two"))
            bind(first, OLD, "Concurrent publisher")
            bind(second, OLD, "Concurrent publisher")
            await asyncio.gather(first.poll_once("S30"), second.poll_once("S30"))
            current = await repo.get("ops.source_checkpoints", "S30:0")
            assert current is not None
            ledger = SourceReportLedger.model_validate(current["entry_error_reporting"])
            assert ledger.pending == {} and ledger.last_outcome is not None
            assert ledger.last_outcome.outcome.kind in {"RECORDED", "DEDUPLICATED"}
            assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})) == 1
            assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})) == 1
            assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
            assert requests == [OLD, OLD]
            await first.close()
            await second.close()
    finally:
        await budgets.shutdown()


@pytest.mark.parametrize("invalid", ["UNBACKED", "FOREIGN_SCOPE"])
async def test_model_only_or_foreign_confirmation_cannot_acknowledge_report_intent(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: Literal["UNBACKED", "FOREIGN_SCOPE"],
) -> None:
    budgets = fixture_request_budgets()
    settings = ReportSettings(data_dir=tmp_path, ops_progress_enabled=True)

    async def unsupported_confirmation(intent: SourceReportIntent) -> SourceReportOutcome:
        scope = intent.subject.scope
        if invalid == "FOREIGN_SCOPE":
            scope = SourceReportScope(source_id="S15", feed_url=scope.feed_url)
        notice_id = source_report_notice_id(scope, intent.error_keys)
        return SourceReportRecorded(
            kind="RECORDED",
            scope=scope,
            notice_id=notice_id,
            markers=[
                SourceReportMarkerReference(
                    error_key=key,
                    marker_id="entry_error_" + key,
                    notice_id=notice_id,
                    marker_sha256="a" * 64,
                    notice_sha256="b" * 64,
                )
                for key in intent.error_keys
            ],
        )

    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=BROKEN))
        ) as http:
            service = SourceService(repo, settings, http=http, budgets=budgets, versions=versions("unbacked"))
            bind(service, OLD, "Original publisher")
            with monkeypatch.context() as invalid_sink:
                invalid_sink.setattr(service, "report_error_intent", unsupported_confirmation)
                await service.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            ledger = SourceReportLedger.model_validate(state["entry_error_reporting"])
            assert len(ledger.pending) == 1 and ledger.last_outcome is None
            assert await repo.list("out.ops_notices") == []
            assert await repo.list("ops.jobs", filters={"type": "DELIVER"}) == []
            await service.poll_once("S30")
            state = await repo.get("ops.source_checkpoints", "S30:0")
            assert state is not None
            assert SourceReportLedger.model_validate(state["entry_error_reporting"]).pending == {}
            assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})) == 1
            assert len(await repo.list("ops.jobs", filters={"type": "DELIVER"})) == 1
            await service.close()
    finally:
        await budgets.shutdown()
