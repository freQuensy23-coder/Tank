"""Capture and source coverage contracts exercise real validation and persistence decisions."""

import base64

import httpx
import pytest
from pydantic import ValidationError

from tank.sources.capture_fetch import CaptureState, FetchRequest, FetchResult, capture_fetch
from tank.sources.read_report import FeedReads, ReadCounter, ReadSnapshot, report
from tank.storage import MemoryRepository
from tank.util import sha256


def result(payload: bytes) -> FetchResult:
    return FetchResult(
        status_code=200,
        content_type="application/octet-stream",
        sha256=sha256(payload),
        fetched_at="2026-10-24T00:00:00Z",
        bytes=len(payload),
        source_id="S14",
        body_b64=base64.b64encode(payload).decode(),
    )


def test_capture_rejects_missing_and_corrupt_body() -> None:
    wire = result(b"evidence").model_dump(mode="json")
    del wire["status_code"]
    with pytest.raises(ValidationError):
        FetchResult.model_validate(wire)
    wire = result(b"evidence").model_dump(mode="json")
    wire["body_b64"] = base64.b64encode(b"changed!").decode()
    with pytest.raises(ValidationError, match="digest"):
        FetchResult.model_validate(wire)


def test_capture_binary_and_request_policy() -> None:
    assert result(b"\xff\x00").content() == b"\xff\x00"
    with pytest.raises(ValidationError):
        FetchRequest(method="GET", url="http://api.binance.com/api/v3/time", source_id="S14")
    with pytest.raises(ValidationError):
        FetchRequest.model_validate({"url": "https://api.binance.com/api/v3/time", "source_id": "S14"})


@pytest.mark.asyncio
async def test_capture_preserves_invalid_terminal_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    module = importlib.import_module("tank.sources.capture_fetch")
    repo = MemoryRepository()
    request_id = "CF-corrupt"
    wire = result(b"retained").model_dump(mode="json")
    wire["bytes"] = 999
    await repo.put(
        "mkt.capture_requests", request_id, {"request_id": request_id, "status": "COMPLETE", "result": wire}
    )

    async def completed(*args: object) -> tuple[str, CaptureState]:
        return request_id, CaptureState(status="COMPLETE", reason=None, result=wire)

    monkeypatch.setattr(module, "_capture_request", completed)
    with pytest.raises(ValidationError):
        await capture_fetch(repo, "GET", "https://api.binance.com/api/v3/time")
    assert await repo.get("mkt.capture_requests", request_id) is not None


@pytest.mark.asyncio
async def test_capture_verified_response_releases_queue_record(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    module = importlib.import_module("tank.sources.capture_fetch")
    repo = MemoryRepository()
    request_id = "CF-valid"
    wire = result(b"\xff\x00").model_dump(mode="json")
    await repo.put(
        "mkt.capture_requests", request_id, {"request_id": request_id, "status": "COMPLETE", "result": wire}
    )

    async def completed(*args: object) -> tuple[str, CaptureState]:
        return request_id, CaptureState(status="COMPLETE", reason=None, result=wire)

    monkeypatch.setattr(module, "_capture_request", completed)
    response = await capture_fetch(repo, "GET", "https://api.binance.com/api/v3/time")
    assert response.status_code == httpx.codes.OK
    assert response.content == b"\xff\x00"
    assert await repo.get("mkt.capture_requests", request_id) is None


def test_read_window_rejects_counter_resets_and_aggregates_details() -> None:
    def window(ok: int, failed: int) -> ReadSnapshot:
        return ReadSnapshot(
            taken_at="2026-10-24T00:00:00Z",
            feeds={
                "S01:0": FeedReads(
                    reads=ReadCounter(ok=ok, failed=failed),
                    detail_reads=ReadCounter(ok=2, failed=1),
                    status="DEGRADED",
                    error="publisher unavailable",
                )
            },
        )

    observed = report(window(20, 3), window(10, 1), ["S01", "S02"])
    assert observed.sources["S01"].ok == 10
    assert observed.sources["S01"].failed == 2
    assert observed.sources["S01"].degraded_feeds[0].error == "publisher unavailable"
    assert observed.sources["S02"].failed_rate is None
    assert observed.all_pass is False
    with pytest.raises(ValueError, match="counter reset"):
        report(window(9, 1), window(10, 1))


def test_source_configuration_requires_adapter_inputs_and_unique_identities() -> None:
    from tank.contracts.sources import SourceConfiguration, SourceFeed
    from tank.settings import Settings

    configuration = SourceConfiguration.model_validate(Settings().load_config("sources"))
    with pytest.raises(ValidationError, match="duplicate source IDs"):
        SourceConfiguration(
            version=configuration.version,
            contact_email=configuration.contact_email,
            sources=[configuration.sources[0], configuration.sources[0]],
        )
    with pytest.raises(ValidationError, match="article pattern"):
        SourceFeed(url="https://example.org/news", kind="html", interval_s=60)
    with pytest.raises(ValidationError, match="venue and market_type"):
        SourceFeed(url="https://example.org/instruments", kind="instruments", interval_s=60)
    with pytest.raises(ValidationError, match="POST and its request body"):
        SourceFeed(url="https://example.org/graphql", kind="graphql", interval_s=60)
    with pytest.raises(ValidationError):
        SourceFeed.model_validate({"url": "https://example.org/feed", "kind": "rss"})


def test_entry_error_variants_preserve_run_identity_and_validate_evidence() -> None:
    from tank.ops_entry_errors import error_key
    from tank.sources.errors import AccountEntryError, parse_entry_error

    run_error = parse_entry_error({"reason": "RUN_DEGRADED", "account": None})
    assert isinstance(run_error, AccountEntryError)
    assert run_error.identity().account == ""
    repeated = parse_entry_error({"reason": "RUN_DEGRADED", "account": None, "error": "retry 2"})
    assert error_key("S29", "https://api.apify.com/run", run_error) == error_key(
        "S29", "https://api.apify.com/run", repeated
    )
    account_error = parse_entry_error({"reason": "RUN_DEGRADED", "account": "alice"})
    assert error_key("S29", "https://api.apify.com/run", run_error) != error_key(
        "S29", "https://api.apify.com/run", account_error
    )
    with pytest.raises(ValidationError):
        parse_entry_error({"account": "alice"})
    with pytest.raises(ValidationError):
        parse_entry_error({"reason": "INVALID_PUBLISHER_ENTRY", "entry_index": -1})
