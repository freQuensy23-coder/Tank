import importlib
import json
import os

import httpx
import pytest
from test_sources import settings as source_settings
from test_x_diagnostics import failed_run
from test_x_reader_integration import LoopbackTelegramTransport
from test_x_reader_integration import wire_server as wire_server
from test_xsensor import URL

from tank.delivery import DeliveryService
from tank.sources import SourceService
from tank.sources.xsensor import XSensor, account_error


@pytest.mark.parametrize(
    "error,token,expected",
    [
        (None, "distinct-apify-credential", None),
        ("", "distinct-apify-credential", ""),
        ("PUBLIC_PROFILE_HTTP_503", "distinct-apify-credential", "PUBLIC_PROFILE_HTTP_503"),
        ("safe upstream text", "distinct-apify-credential", "safe upstream text"),
        (
            "safe distinct-apify-credential suffix",
            "distinct-apify-credential",
            "safe [REDACTED] suffix",
        ),
        ("safe\x00text", "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
        ("safe\ud800text", "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
        ("safe\udffftext", "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
        ("x" * 8192, "distinct-apify-credential", "x" * 8192),
        ("x" * 8193, "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
        ("x" * 8185 + "distinct-apify-credential", "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
        ("x" * 8191 + "z", "z", "INVALID_ACCOUNT_ERROR"),
        ("safe\x00text", "INVALID_ACCOUNT_ERROR", "?"),
        ("safe REDACTED text", "REDACTED", "INVALID_ACCOUNT_ERROR"),
        ("safe [NUL] text", "[NUL]", "safe [REDACTED] text"),
        ("safe?text", "?", "safe[REDACTED]text"),
    ],
)
def test_account_error_is_bounded_jsonb_safe_without_truncating_credentials(error, token, expected):
    safe = account_error(error, token)
    assert safe == expected
    if safe:
        assert len(safe) <= 8192 and token not in safe and "\x00" not in safe
        assert safe.encode("utf-8").decode("utf-8") == safe


@pytest.mark.parametrize(
    "error,expected",
    [
        (None, "FAILED"),
        ("", "FAILED"),
        ("PUBLIC_PROFILE_HTTP_503", "PUBLIC_PROFILE_HTTP_503"),
        ("safe upstream text", "safe upstream text"),
        ("safe distinct-apify-credential suffix", "safe [REDACTED] suffix"),
    ],
)
async def test_failed_scan_redacts_account_error_without_diagnostics(error, expected):
    token = "distinct-apify-credential"
    broken = failed_run(None)
    broken["accounts"][0]["error"] = error
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=broken))
    ) as http:
        result = await XSensor(http, token).posts(URL, ["alice"], {"alice": "90"})
    assert result.failed and result.rows == [] and result.cursors == {"alice": "90"}
    assert result.error == "PUBLIC_SOURCE_UNAVAILABLE"
    assert result.failures[0].account == "alice"
    assert result.failures[0].reason == expected
    assert result.failures[0].diagnostics is None


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize(
    "error,expected",
    [
        ("safe distinct-apify-credential suffix", "safe [REDACTED] suffix"),
        ("safe\x00text", "INVALID_ACCOUNT_ERROR"),
        ("x" * 8185 + "distinct-apify-credential", "INVALID_ACCOUNT_ERROR"),
    ],
)
async def test_account_reason_jsonb_checkpoint_immutable_ops_and_wire_delivery(
    tmp_path, postgres_template, wire_server, monkeypatch, error, expected
):
    origin, state = wire_server
    broken = failed_run(None)
    broken["accounts"][0]["error"] = error

    async def upstream(payload):
        assert payload == {"handles": ["alice"], "cursors": {"alice": "90"}}
        return broken

    monkeypatch.setattr(importlib.import_module("direct_fetch"), "fetch_snapshot", upstream)
    config = source_settings(
        tmp_path,
        [{"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": origin + "/actor"}]}],
    )
    config.apify_api_token = "distinct-apify-credential"
    config.telegram_token = "distinct-telegram-credential"
    config.telegram_ops_chat = "ops"
    config.ops_progress_enabled = True
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.put(
            "ops.source_checkpoints",
            "S29:0",
            {"account_cursors": {"alice": "90"}, "last_success_at": "2026-10-24T11:59:00Z"},
        )
        async with httpx.AsyncClient() as http:
            service = SourceService(repo, config, http=http)
            service.accounts = {"alice": {"handle": "alice", "role": "OTHER", "entity_id": "alice"}}
            result = await service.poll_once("S29")
        checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
        errors = [{"account": "alice", "reason": expected, "diagnostics": None}]
        assert result["status"] == "DEGRADED" and result["items"] == 0
        assert checkpoint["entry_errors"] == errors
        assert checkpoint["account_cursors"] == {"alice": "90"}
        assert checkpoint["acquisition_error"] == checkpoint["error"] == "PUBLIC_SOURCE_UNAVAILABLE"
        assert checkpoint["reads"] == {"ok": 0, "failed": 1}
        assert checkpoint["last_success_at"] == "2026-10-24T11:59:00Z"
        assert not await repo.list("ingest.source_items")
        [notice] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})
        [marker] = await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})
        assert notice["entry_errors"] == errors and expected in notice["text"]
        assert marker["entry_error"] == errors[0] and marker["text"] == expected
        assert not notice.get("document_manifest")
        [job] = await repo.list("ops.jobs", filters={"type": "DELIVER"})
        async with httpx.AsyncClient(transport=LoopbackTelegramTransport(origin)) as http:
            await DeliveryService(repo, config, http=http).deliver(job["payload"])
            await repo.close()
            await repo.open()
            committed = await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]})
            await DeliveryService(repo, config, http=http).deliver(job["payload"])
        assert len(state["telegram"]) == 1
        path, _, body = state["telegram"][0]
        assert path.endswith("sendMessage") and expected in json.loads(body)["text"]
        assert len(committed) == 2
        assert sorted(row["status"] for row in committed) == ["GENERATED", "SENT"]
        for value in (checkpoint, notice, marker, committed, json.loads(body)):
            serialized = json.dumps(value, ensure_ascii=False).encode("utf-8")
            assert b"distinct-apify-credential" not in serialized
            assert b"\\u0000" not in serialized and b"\x00" not in serialized
        assert await repo.get("out.ops_notices", notice["notice_id"]) == notice
        assert await repo.get("out.ops_notices", marker["notice_id"]) == marker
        assert await repo.list("out.deliveries", filters={"notice_id": notice["notice_id"]}) == committed
