"""Typed instrument observations preserve evidence and reject malformed identities."""

import json
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from source_support import fixture_request_budgets, fixture_versions, instrument_snapshot_fixture, source_feed

from tank.contracts.source_checkpoints import InstrumentSnapshot
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.instruments import InstrumentPage, instrument_changes
from tank.storage import MemoryRepository
from tank.util import JsonInput, sha256


def test_snapshot_roundtrip_preserves_explicit_unknowns_and_absent_historical_flags() -> None:
    before = instrument_snapshot_fixture(
        {"ETH-USD": {"base": "ETH", "quote": "USD", "status": "online"}}, venue="coinbase"
    )
    wire = before.storage_record()
    record = wire["ETH-USD"]
    assert isinstance(record, dict)
    assert "status_flags" not in record
    assert InstrumentSnapshot.model_validate(wire).storage_record() == wire
    assert sha256(before.storage_record()) == sha256(wire)
    malformed = dict(wire)
    malformed["different-symbol"] = malformed.pop("ETH-USD")
    with pytest.raises(ValidationError, match="declared symbol"):
        InstrumentSnapshot.model_validate(malformed)


@pytest.mark.parametrize("field", ["market_type", "symbol", "base", "quote", "status", "contract_type"])
def test_required_instrument_observation_fields_cannot_be_omitted(field: str) -> None:
    snapshot = instrument_snapshot_fixture({"ABC": {"status": "online"}}, venue="kraken")
    record = snapshot.root["ABC"].model_dump(mode="json")
    record.pop(field)
    with pytest.raises(ValidationError):
        InstrumentSnapshot.model_validate({"ABC": record})


def test_grouped_identity_matches_preexisting_serialized_snapshot_algorithm() -> None:
    previous = instrument_snapshot_fixture(
        {"A": {"status": "online"}, "B": {"status": "online"}}, venue="kraken"
    )
    current = instrument_snapshot_fixture(
        {"A": {"status": "cancel_only"}, "B": {"status": "cancel_only"}}, venue="kraken"
    )
    page = InstrumentPage(payload=b"actual venue response", observed_at="2026-10-25T05:00:00Z")
    rows, digest = instrument_changes(
        current,
        previous,
        source_feed(
            {"url": "https://api.kraken.com/0/public/AssetPairs", "venue": "kraken", "market_type": "SPOT"}
        ),
        [page],
        {"A": 0, "B": 0},
        "2026-10-25T05:00:01Z",
    )
    [result] = rows
    old_group_wire: list[JsonInput] = [
        (symbol, current.storage_record()[symbol], previous.storage_record()[symbol]) for symbol in ("A", "B")
    ]
    assert result.discovery.external_id == "kraken|SPOT|changes|" + sha256(old_group_wire)
    assert digest == sha256(current.storage_record())
    assert result.payload == page.payload and result.observation.observed_at == page.observed_at
    evidence = json.loads(result.discovery.text)
    assert [entry["symbol"] for entry in evidence["changes"]] == ["A", "B"]
    assert evidence["changes"][0]["before"]["status"] == "online"
    assert evidence["changes"][0]["after"]["status"] == "cancel_only"


def test_snapshot_error_does_not_disclose_other_raw_record_fields() -> None:
    with pytest.raises(ValidationError) as failure:
        InstrumentSnapshot.model_validate({"secret-symbol": {"opaque": "private-retained-source-body"}})
    assert "private-retained-source-body" not in str(failure.value)


def test_instrument_page_requires_its_actual_aware_observation_clock() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        InstrumentPage(payload=b"actual response", observed_at="2026-10-25T05:00:00")


def test_changed_snapshot_requires_actual_response_evidence_and_declared_feed_owner() -> None:
    previous = instrument_snapshot_fixture({"A": {"status": "online"}}, venue="kraken")
    current = instrument_snapshot_fixture({"A": {"status": "offline"}}, venue="kraken")
    feed = source_feed(
        {"url": "https://api.kraken.com/0/public/AssetPairs", "venue": "kraken", "market_type": "SPOT"}
    )
    with pytest.raises(ValueError, match="response evidence"):
        instrument_changes(current, previous, feed, [], {}, "2026-10-25T05:00:01Z")
    wrong_owner = feed.model_copy(update={"venue": "binance"})
    with pytest.raises(ValueError, match="feed owner"):
        instrument_changes(
            current,
            previous,
            wrong_owner,
            [InstrumentPage(payload=b"actual response", observed_at="2026-10-25T05:00:00Z")],
            {"A": 0},
            "2026-10-25T05:00:01Z",
        )


@pytest.mark.asyncio
async def test_invalid_live_snapshot_degrades_without_replacing_prior_observations(tmp_path: Path) -> None:
    requests = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        record: dict[str, JsonValue] = {"base": "ABC", "quote": "USD", "status": "online"}
        if requests > 1:
            record.pop("status")
            record["private_unparsed_field"] = "private-retained-provider-body"
        return httpx.Response(200, json={"error": [], "result": {"ABCUSD": record}})

    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(
            repo,
            Settings(data_dir=tmp_path),
            http=http,
            versions=fixture_versions(),
            budgets=fixture_request_budgets(),
        )
        service.sources["S14"] = service.sources["S14"].model_copy(
            update={
                "feeds": [
                    source_feed(
                        {
                            "kind": "instruments",
                            "url": "https://synthetic.example/instruments",
                            "venue": "kraken",
                            "market_type": "SPOT",
                        }
                    )
                ]
            }
        )
        try:
            first = await service.poll_once("S14")
            assert first.status == "HEALTHY" and first.items == 0
            previous = await repo.get("ops.source_checkpoints", "S14:0:instruments")
            assert previous is not None
            failed = await service.poll_once("S14")
            assert failed.status == "DEGRADED" and failed.items == 0
            assert failed.errors and all(
                "private-retained-provider-body" not in error for error in failed.errors
            )
            assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == previous
            assert not await repo.list("ingest.source_items")
        finally:
            await service.close()
