"""Malformed exchange envelopes cannot become successful empty catalog observations."""

from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from source_support import fixture_request_budgets, fixture_versions, source_definition

from tank.contracts.sources import FeedParsingSpec
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.instrument_adapters import parse_instrument_page
from tank.storage import MemoryRepository

type Venue = Literal["binance", "coinbase", "kraken", "okx", "bybit", "hyperliquid"]


def feed(venue: Venue) -> FeedParsingSpec:
    return FeedParsingSpec(url="https://synthetic.example/catalog", venue=venue, market_type="SPOT")


@pytest.mark.parametrize(
    ("venue", "payload"),
    [
        ("binance", {"symbols": []}),
        ("coinbase", []),
        ("kraken", {"error": [], "result": {}}),
        ("okx", {"code": "0", "data": []}),
        ("bybit", {"retCode": 0, "result": {"list": []}}),
        ("hyperliquid", {"universe": []}),
    ],
)
def test_explicit_successful_empty_catalogs(venue: Venue, payload: JsonValue) -> None:
    assert parse_instrument_page(payload, feed(venue)).snapshot.root == {}


@pytest.mark.parametrize(
    ("venue", "payload"),
    [
        ("bybit", {"result": {"list": []}}),
        ("bybit", {"retCode": None, "result": {"list": []}}),
        ("bybit", {"retCode": False, "result": {"list": []}}),
        ("bybit", {"retCode": "0", "result": {"list": []}}),
        ("bybit", {"retCode": 1, "result": {"list": []}}),
        ("bybit", {"retCode": 0, "result": {}}),
        ("bybit", {"retCode": 0, "result": {"list": [], "nextPageCursor": None}}),
        ("okx", {"data": []}),
        ("okx", {"code": None, "data": []}),
        ("okx", {"code": 0, "data": []}),
        ("okx", {"code": "1", "data": []}),
        ("kraken", {"result": {}}),
        ("kraken", {"error": None, "result": {}}),
        ("kraken", {"error": ["failure"], "result": {}}),
        ("binance", {"code": -1, "symbols": []}),
        ("coinbase", {"message": "failure"}),
        ("hyperliquid", {}),
    ],
)
def test_required_protocol_facts_refuse_missing_null_or_wrong_types(venue: Venue, payload: JsonValue) -> None:
    with pytest.raises((ValidationError, ValueError)):
        parse_instrument_page(payload, feed(venue))


def test_hyperliquid_sparse_delisting_flag_is_not_an_execution_availability_claim() -> None:
    page = parse_instrument_page(
        {"universe": [{"name": "BTC"}, {"name": "LOOM", "isDelisted": True}]}, feed("hyperliquid")
    )
    assert page.snapshot.root["BTC"].status == "TRADING"
    assert page.snapshot.root["BTC"].quote is None
    assert page.snapshot.root["LOOM"].status == "DELISTED"


@pytest.mark.parametrize("value", [None, "false", 0, []])
def test_declared_boolean_flags_must_be_actual_booleans(value: JsonValue) -> None:
    with pytest.raises(ValidationError):
        parse_instrument_page({"universe": [{"name": "BTC", "isDelisted": value}]}, feed("hyperliquid"))
    with pytest.raises(ValidationError):
        parse_instrument_page(
            [
                {
                    "id": "BTC-USD",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "status": "online",
                    "trading_disabled": value,
                }
            ],
            feed("coinbase"),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("venue", ["bybit", "okx"])
async def test_missing_success_code_cannot_remove_catalog_or_advance_snapshot(
    tmp_path: Path, venue: Literal["bybit", "okx"]
) -> None:
    invalid = False
    valid_empty = False
    sent: list[bytes] = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "synthetic.example"
        if venue == "bybit":
            payload: dict[str, JsonValue] = {
                "result": {
                    "list": []
                    if invalid or valid_empty
                    else [{"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "Trading"}]
                }
            }
            if not invalid:
                payload["retCode"] = 0
        else:
            payload = {
                "data": []
                if invalid or valid_empty
                else [{"instId": "BTC-USDT", "baseCcy": "BTC", "quoteCcy": "USDT", "state": "live"}]
            }
            if not invalid:
                payload["code"] = "0"
        response = httpx.Response(200, json=payload)
        sent.append(response.content)
        return response

    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    source = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": "https://synthetic.example/catalog",
                    "venue": venue,
                    "market_type": "SPOT",
                }
            ],
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        service.sources["S14"] = source
        assert (await service.poll_once("S14")).items == 0
        original = await repo.get("ops.source_checkpoints", "S14:0:instruments")
        assert original is not None
        invalid = True
        outcome = await service.poll_once("S14")
        assert outcome.status == "DEGRADED" and outcome.items == 0
        assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == original
        assert await repo.list("ingest.source_items") == []
        restarted = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        restarted.sources["S14"] = source
        assert (await restarted.poll_once("S14")).status == "DEGRADED"
        assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == original
        assert sent[-1] == sent[-2]
        invalid = False
        valid_empty = True
        removed = await restarted.poll_once("S14")
        assert removed.status == "HEALTHY" and removed.items == 1
        records = await repo.list("ingest.source_items")
        assert len(records) == 1 and records[0]["change"] == "REMOVED"
        assert (await repo.get("ops.source_checkpoints", "S14:0:instruments")) != original
        await service.close()
        await restarted.close()
    await budgets.shutdown()


def test_bybit_cursor_is_preserved_and_empty_or_absent_means_no_next_page() -> None:
    declared = feed("bybit")
    assert (
        parse_instrument_page(
            {"retCode": 0, "result": {"list": [], "nextPageCursor": "next"}}, declared
        ).next_cursor
        == "next"
    )
    assert (
        parse_instrument_page(
            {"retCode": 0, "result": {"list": [], "nextPageCursor": ""}}, declared
        ).next_cursor
        is None
    )


def test_protocol_validation_does_not_expose_raw_provider_payload() -> None:
    with pytest.raises(ValidationError) as failure:
        parse_instrument_page(
            {"retCode": {"secret": "provider-payload-private"}, "result": {"list": []}},
            feed("bybit"),
        )
    assert "provider-payload-private" not in str(failure.value)
