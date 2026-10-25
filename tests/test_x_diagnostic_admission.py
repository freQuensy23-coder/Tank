"""Strict recorded diagnostic facts precede sanitization and source acknowledgement."""

from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from source_support import fixture_request_budgets, fixture_versions, source_definition
from test_xsensor_wire_privacy import TOKEN, URL, PrivacyFixtureSettings, healthy_reply

from tank.contracts.sources import SourceAccount
from tank.sources import SourceService
from tank.sources.errors import XDiagnosticAttempt, XDiagnosticRequest, XDiagnosticResponse, XDiagnostics
from tank.sources.x_diagnostics import sanitize
from tank.storage import MemoryRepository
from tank.util import sha256


def recorded_diagnostics(*, response: bool = True) -> XDiagnostics:
    return XDiagnostics(
        request=XDiagnosticRequest(method="GET", url="https://x.com/alice", body=None),
        outcome="exhausted",
        attemptCount=1,
        attempts=[
            XDiagnosticAttempt(
                attempt=1,
                at="2026-10-25T12:00:00Z",
                status=503 if response else None,
                retryable=True,
                error="PUBLIC_PROFILE_HTTP_503" if response else "PUBLIC_PROFILE_TRANSPORT",
                response=XDiagnosticResponse(
                    status=503,
                    headers={"content-type": "text/plain"},
                    body="",
                    bodyEncoding="utf-8",
                    bodyBytes=0,
                    bodySha256=sha256(b""),
                    bodyTruncated=False,
                    redacted=False,
                )
                if response
                else None,
            )
        ],
    )


def test_absent_response_and_actual_empty_response_remain_distinct() -> None:
    unavailable = sanitize(recorded_diagnostics(response=False))
    empty = sanitize(recorded_diagnostics())
    assert unavailable.attempts[0].response is None
    assert unavailable.attempts[0].status is None
    assert empty.attempts[0].response is not None
    assert empty.attempts[0].response.body == ""
    assert empty.attempts[0].response.bodyBytes == 0
    assert empty.attempts[0].response.bodySha256 == sha256(b"")
    assert not empty.attempts[0].response.redacted


@pytest.mark.parametrize("defect", ["method", "status", "body", "encoding", "flag", "count", "digest"])
@pytest.mark.asyncio
async def test_invalid_diagnostic_facts_refuse_before_raw_or_cursor_advance(
    defect: str, tmp_path: Path
) -> None:
    evidence = recorded_diagnostics().model_dump(mode="json")
    request = evidence["request"]
    attempts = evidence["attempts"]
    assert isinstance(request, dict) and isinstance(attempts, list)
    attempt = attempts[0]
    assert isinstance(attempt, dict)
    response = attempt["response"]
    assert isinstance(response, dict)
    if defect == "method":
        request["method"] = "POST"
    elif defect in {"status", "body"}:
        del response[defect]
    elif defect == "encoding":
        response["bodyEncoding"] = "base64"
        response["body"] = "not!base64"
    elif defect == "flag":
        response["redacted"] = 1
    elif defect == "count":
        evidence["attemptCount"] = 0
    else:
        response["bodySha256"] = "0" * 64
    with pytest.raises(ValidationError) as caught:
        sanitize(evidence)
    assert "not!base64" not in str(caught.value)
    payload = healthy_reply()
    payload["health"] = "degraded"
    payload["accounts"] = [
        {"handle": "alice", "status": "failed", "error": "PUBLIC_PROFILE_HTTP_503", "diagnostics": evidence}
    ]
    repo = MemoryRepository()
    budgets = fixture_request_budgets()
    settings = PrivacyFixtureSettings(data_dir=tmp_path, apify_api_token=TOKEN, ops_progress_enabled=False)
    source = source_definition({"source_id": "S29", "feeds": [{"kind": "x", "url": URL}]})
    previous: dict[str, JsonValue] = {"account_cursors": {"alice": "100"}}
    await repo.put("ops.source_checkpoints", "S29:0", previous)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=payload))
    ) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        service.sources = {"S29": source}
        service.accounts = {
            "alice": SourceAccount(handle="alice", role="OTHER", entity_id="alice", token_id=None)
        }
        try:
            result = await service.poll_once("S29")
            assert result.status == "DEGRADED" and result.items == 0
            checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
            assert checkpoint is not None and checkpoint["account_cursors"] == previous["account_cursors"]
            assert not await repo.list("ingest.source_items")
            assert not list(tmp_path.rglob("*.zst"))
        finally:
            await service.close()
    await budgets.shutdown()
