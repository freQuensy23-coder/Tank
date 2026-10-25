"""Full decoded actor replies are a privacy boundary before raw retention or cursor acknowledgement."""

import base64
import json
from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import (
    fixture_http_admission,
    fixture_request_budgets,
    fixture_versions,
    source_definition,
)

from tank.contracts.json import encode_json
from tank.contracts.sources import SourceAccount
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository

TOKEN = "synthetic-private-apify-key-Aa19"
URL = "https://api.apify.com/v2/acts/test~reader/run-sync-get-dataset-items"


def healthy_reply() -> dict[str, JsonValue]:
    return {
        "health": "healthy",
        "posts": [
            {
                "id": "105",
                "author": "alice",
                "text": "Actual public text",
                "url": "https://x.com/alice/status/105",
                "publishedAt": "2026-10-25T12:00:00Z",
            }
        ],
        "accounts": [{"handle": "alice", "status": "ok", "posts": 1, "historyGap": False}],
        "nextCursors": {"alice": "105"},
    }


def escaped(token: str) -> bytes:
    return "".join(chr(92) + "u" + f"{ord(character):04x}" for character in token).encode()


def tainted_reply(location: Literal["VALUE", "ESCAPED", "KEY", "NESTED", "ENCODED", "UNSAFE_TAG"]) -> bytes:
    payload = healthy_reply()
    if location == "KEY":
        payload["telemetry"] = {"prefix-" + TOKEN: "ignored value"}
    elif location == "NESTED":
        payload["telemetry"] = ["safe", {"more": [{"credential": TOKEN}]}]
    elif location == "ENCODED":
        payload["telemetry"] = {
            "bodyEncoding": "base64",
            "body": base64.b64encode(("prefix-" + TOKEN).encode()).decode(),
        }
    elif location == "UNSAFE_TAG":
        payload["accounts"] = [{"handle": "alice", "status": TOKEN, "posts": 1, "historyGap": False}]
    else:
        payload["telemetry"] = {"unconsumed": TOKEN}
    wire = encode_json(payload)
    if location in {"ESCAPED", "KEY", "NESTED", "UNSAFE_TAG"}:
        wire = wire.replace(TOKEN.encode(), escaped(TOKEN))
        assert TOKEN.encode() not in wire
    return wire


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["VALUE", "ESCAPED", "KEY", "NESTED", "ENCODED", "UNSAFE_TAG"])
async def test_credential_in_any_decoded_wire_location_refuses_raw_rows_with_safe_error(
    location: Literal["VALUE", "ESCAPED", "KEY", "NESTED", "ENCODED", "UNSAFE_TAG"],
) -> None:
    wire = tainted_reply(location)
    cursors = {"alice": "100"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        sensor = XSensor(http, TOKEN, admission=fixture_http_admission())
        with pytest.raises(ValueError) as failure:
            await sensor.posts(URL, ["alice"], cursors)
    assert cursors == {"alice": "100"}
    assert str(failure.value) == "Apify X response contains credential material"
    assert TOKEN not in str(failure.value) and escaped(TOKEN).decode() not in str(failure.value)


@pytest.mark.asyncio
async def test_safe_unknown_telemetry_retains_exact_original_bytes_and_valid_cursor() -> None:
    payload = healthy_reply()
    payload["telemetry"] = {"nested": ["snowman ☃", {"safe": TOKEN[:-1]}]}
    wire = json.dumps(payload, ensure_ascii=True, indent=2).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        sensor = XSensor(http, TOKEN, admission=fixture_http_admission())
        observed = await sensor.posts(URL, ["alice"], {"alice": "100"})
    assert observed.cursors == {"alice": "105"} and not observed.failures
    assert len(observed.rows) == 1 and observed.rows[0].payload == wire
    assert observed.rows[0].discovery.external_id == "105"


@pytest.mark.asyncio
async def test_tainted_reply_with_diagnostics_keeps_only_sanitized_failure_evidence() -> None:
    payload = healthy_reply()
    payload["health"] = "degraded"
    payload["accounts"] = [
        {
            "handle": "alice",
            "status": "failed",
            "error": "PUBLIC_PROFILE_HTTP_503",
            "diagnostics": {
                "request": {"method": "GET", "url": "https://x.com/alice", "body": None},
                "outcome": "exhausted",
                "attemptCount": 1,
                "attempts": [
                    {
                        "attempt": 1,
                        "at": "2026-10-25T12:00:00Z",
                        "status": 503,
                        "retryable": True,
                        "error": "PUBLIC_PROFILE_HTTP_503",
                        "response": {
                            "status": 503,
                            "headers": {"content-type": "text/plain"},
                            "body": "before " + TOKEN + " after",
                            "bodyEncoding": "utf-8",
                            "bodyTruncated": False,
                        },
                    }
                ],
            },
        }
    ]
    wire = encode_json(payload).replace(TOKEN.encode(), escaped(TOKEN))
    assert TOKEN.encode() not in wire
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        observed = await XSensor(http, TOKEN, admission=fixture_http_admission()).posts(
            URL, ["alice"], {"alice": "100"}
        )
    assert observed.rows == [] and observed.cursors == {"alice": "100"}
    diagnostic_failures = [failure for failure in observed.failures if failure.diagnostics is not None]
    assert len(diagnostic_failures) == 1
    diagnostics = diagnostic_failures[0].diagnostics
    assert diagnostics is not None and diagnostics.attempts[0].response is not None
    response = diagnostics.attempts[0].response
    assert response.body == "before [REDACTED] after" and response.redacted
    assert any(failure.reason == "CREDENTIAL_MATERIAL_IN_ACTOR_RESPONSE" for failure in observed.failures)
    assert not any(TOKEN in failure.model_dump_json() for failure in observed.failures)


class PrivacyFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.asyncio
async def test_actual_source_cannot_persist_or_acknowledge_escaped_credential_wire(tmp_path: Path) -> None:
    wire = tainted_reply("NESTED")
    repo = MemoryRepository()
    settings = PrivacyFixtureSettings(data_dir=tmp_path, apify_api_token=TOKEN, ops_progress_enabled=False)
    budgets = fixture_request_budgets()
    source = source_definition({"source_id": "S29", "feeds": [{"kind": "x", "url": URL}]})
    await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "100"}})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        service.sources = {"S29": source}
        service.accounts = {
            "alice": SourceAccount(handle="alice", role="OTHER", entity_id="alice", token_id=None)
        }
        try:
            result = await service.poll_once("S29")
            assert result.status == "DEGRADED" and result.items == 0
            assert result.errors == ["Apify X response contains credential material"]
            checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
            assert checkpoint is not None and checkpoint["account_cursors"] == {"alice": "100"}
            assert not await repo.list("ingest.source_items")
            assert not list(tmp_path.rglob("*.zst"))
            assert TOKEN not in json.dumps(checkpoint)
        finally:
            await service.close()
    await budgets.shutdown()
