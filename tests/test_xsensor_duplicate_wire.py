"""Ambiguous actor JSON cannot hide credentials or authorize raw evidence/cursors."""

import json
from pathlib import Path
from typing import Literal

import httpx
import pytest
from source_support import (
    fixture_http_admission,
    fixture_request_budgets,
    fixture_versions,
    source_definition,
)
from test_xsensor_wire_privacy import TOKEN, URL, PrivacyFixtureSettings, escaped, healthy_reply

from tank.contracts.json import encode_json
from tank.contracts.sources import SourceAccount
from tank.sources import SourceService
from tank.sources.xsensor import XSensor
from tank.storage import MemoryRepository

type DuplicateLocation = Literal["VALUE", "NESTED_VALUE", "ESCAPED_KEY", "CREDENTIAL_KEY"]
LOCATIONS: tuple[DuplicateLocation, ...] = ("VALUE", "NESTED_VALUE", "ESCAPED_KEY", "CREDENTIAL_KEY")


def ambiguous_reply(location: DuplicateLocation) -> bytes:
    start = encode_json(healthy_reply())[:-1]
    credential = escaped(TOKEN)
    if location == "NESTED_VALUE":
        suffix = b',"telemetry":{"ignored":"' + credential + b'","ignored":"safe"}}'
    elif location == "ESCAPED_KEY":
        suffix = b',"telemetry":{"ignored":"' + credential + b'"},"' + escaped("telemetry") + b'":{}}'
    elif location == "CREDENTIAL_KEY":
        suffix = b',"telemetry":{"' + credential + b'":"ignored"},"telemetry":{}}'
    else:
        suffix = b',"telemetry":{"ignored":"' + credential + b'"},"telemetry":{}}'
    wire = start + suffix
    assert TOKEN.encode() not in wire
    assert credential in wire
    return wire


@pytest.mark.parametrize("location", LOCATIONS)
async def test_duplicate_actor_wire_refuses_before_projection_with_a_static_safe_error(
    location: DuplicateLocation,
) -> None:
    wire = ambiguous_reply(location)
    cursors = {"alice": "100"}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        sensor = XSensor(http, TOKEN, admission=fixture_http_admission())
        with pytest.raises(ValueError) as failure:
            await sensor.posts(URL, ["alice"], cursors)
    assert str(failure.value) == "Apify X response JSON is invalid or ambiguous"
    assert TOKEN not in str(failure.value) and escaped(TOKEN).decode() not in str(failure.value)
    assert cursors == {"alice": "100"}


@pytest.mark.parametrize("location", LOCATIONS)
async def test_actual_source_cannot_retain_ambiguous_wire_or_advance_its_cursor(
    tmp_path: Path,
    location: DuplicateLocation,
) -> None:
    wire = ambiguous_reply(location)
    repo = MemoryRepository()
    settings = PrivacyFixtureSettings(data_dir=tmp_path, apify_api_token=TOKEN, ops_progress_enabled=False)
    budgets = fixture_request_budgets()
    source = source_definition({"source_id": "S29", "feeds": [{"kind": "x", "url": URL}]})
    await repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"alice": "100"}})
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
        ) as http:
            service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
            service.sources = {"S29": source}
            service.accounts = {
                "alice": SourceAccount(handle="alice", role="OTHER", entity_id="alice", token_id=None)
            }
            try:
                outcome = await service.poll_once("S29")
                assert outcome.status == "DEGRADED" and outcome.items == 0
                assert outcome.errors == ["Apify X response JSON is invalid or ambiguous"]
                checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
                assert checkpoint is not None and checkpoint["account_cursors"] == {"alice": "100"}
                assert not await repo.list("ingest.source_items")
                assert not list(tmp_path.rglob("*.zst"))
                assert TOKEN not in json.dumps(checkpoint)
            finally:
                await service.close()
    finally:
        await budgets.shutdown()


async def test_unique_json_keeps_original_wire_and_only_authors_the_observed_cursor() -> None:
    payload = healthy_reply()
    payload["telemetry"] = {"nested": ["snowman ☃", TOKEN[:-1]]}
    payload["Telemetry"] = {"case_sensitive_distinct_name": True}
    wire = json.dumps(payload, ensure_ascii=True, indent=2).encode()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=wire))
    ) as http:
        observed = await XSensor(http, TOKEN, admission=fixture_http_admission()).posts(
            URL, ["alice"], {"alice": "100"}
        )
    assert not observed.failures and observed.cursors == {"alice": "105"}
    assert len(observed.rows) == 1 and observed.rows[0].payload == wire
    assert observed.rows[0].discovery.external_id == "105"
