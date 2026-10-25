"""Live catalog projections require actual facts; retained unknowns never become false changes."""

import os
from copy import deepcopy
from pathlib import Path
from typing import Literal

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition, source_feed

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json, encode_json
from tank.contracts.source_checkpoints import CoinbaseInstrument, InstrumentSnapshot
from tank.contracts.sources import FeedParsingSpec
from tank.settings import Settings
from tank.sources import SourceService
from tank.sources.instrument_adapters import MarketType, Venue, parse_instrument_page
from tank.sources.instruments import InstrumentPage, instrument_changes
from tank.storage import MemoryRepository, Repository

CLOCK = "2026-10-25T12:00:00Z"


class RowCase(DomainModel):
    venue: Venue
    market_type: MarketType
    row: dict[str, JsonValue]
    native_key: str | None
    required: list[str]
    rejected_field: str
    status_field: str
    changed_status: str | bool

    def feed(self) -> FeedParsingSpec:
        return FeedParsingSpec(
            url="https://synthetic.example/catalog", venue=self.venue, market_type=self.market_type
        )


def cases() -> list[RowCase]:
    result: list[RowCase] = []
    for venue in ("binance", "bybit"):
        for market_type in ("SPOT", "PERP_LINEAR"):
            if venue == "binance":
                row: dict[str, JsonValue] = {
                    "symbol": "AAAUSDT",
                    "baseAsset": "AAA",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                }
                required = ["symbol", "baseAsset", "quoteAsset", "status"]
                rejected = "baseAsset"
            else:
                row = {"symbol": "AAAUSDT", "baseCoin": "AAA", "quoteCoin": "USDT", "status": "Trading"}
                required = ["symbol", "baseCoin", "quoteCoin", "status"]
                rejected = "baseCoin"
            if market_type == "PERP_LINEAR":
                row["contractType"] = "PERPETUAL" if venue == "binance" else "LinearPerpetual"
                required.append("contractType")
                settlement_field = "marginAsset" if venue == "binance" else "settleCoin"
                row[settlement_field] = "USDT"
                required.append(settlement_field)
            result.append(
                RowCase(
                    venue=venue,
                    market_type=market_type,
                    row=row,
                    native_key=None,
                    required=required,
                    rejected_field=rejected,
                    status_field="status",
                    changed_status="BREAK" if venue == "binance" else "Settling",
                )
            )
    result.extend(
        [
            RowCase(
                venue="kraken",
                market_type="SPOT",
                row={"base": "XAAA", "quote": "ZUSD", "status": "online"},
                native_key="XAAAZUSD",
                required=["base", "quote", "status"],
                rejected_field="base",
                status_field="status",
                changed_status="cancel_only",
            ),
            RowCase(
                venue="okx",
                market_type="SPOT",
                row={"instId": "AAA-USDT", "baseCcy": "AAA", "quoteCcy": "USDT", "state": "live"},
                native_key=None,
                required=["instId", "baseCcy", "quoteCcy", "state"],
                rejected_field="baseCcy",
                status_field="state",
                changed_status="suspend",
            ),
            RowCase(
                venue="okx",
                market_type="PERP_LINEAR",
                row={"instId": "AAA-USDT-SWAP", "state": "live", "ctType": "linear"},
                native_key=None,
                required=["instId", "state", "ctType"],
                rejected_field="ctType",
                status_field="state",
                changed_status="suspend",
            ),
            RowCase(
                venue="coinbase",
                market_type="SPOT",
                row={
                    "id": "AAA-USD",
                    "base_currency": "AAA",
                    "quote_currency": "USD",
                    "status": "online",
                    "post_only": False,
                    "limit_only": False,
                    "cancel_only": False,
                    "auction_mode": False,
                },
                native_key=None,
                required=[
                    "id",
                    "base_currency",
                    "quote_currency",
                    "status",
                    "post_only",
                    "limit_only",
                    "cancel_only",
                    "auction_mode",
                ],
                rejected_field="cancel_only",
                status_field="status",
                changed_status="offline",
            ),
            RowCase(
                venue="hyperliquid",
                market_type="PERP_LINEAR",
                row={"name": "AAA"},
                native_key=None,
                required=["name"],
                rejected_field="name",
                status_field="isDelisted",
                changed_status=True,
            ),
        ]
    )
    return result


def payload(case: RowCase, row: dict[str, JsonValue]) -> JsonValue:
    if case.venue == "binance":
        return {"symbols": [row]}
    if case.venue == "bybit":
        return {
            "retCode": 0,
            "result": {
                "category": "spot" if case.market_type == "SPOT" else "linear",
                "list": [row],
                "nextPageCursor": "",
            },
        }
    if case.venue == "okx":
        return {"code": "0", "data": [row]}
    if case.venue == "coinbase":
        return [row]
    if case.venue == "hyperliquid":
        return {"universe": [row]}
    if case.native_key is None:
        raise ValueError("Kraken fixture requires its actual native result key")
    return {"error": [], "result": {case.native_key: row}}


@pytest.mark.parametrize(
    ("case", "field", "mutation"),
    [
        pytest.param(case, field, mutation, id=f"{case.venue}-{case.market_type}-{field}-{mutation}")
        for case in cases()
        for field in case.required
        for mutation in ("MISSING", "NULL", "NUMBER", "EMPTY", "SPACE")
    ],
)
def test_required_live_facts_reject_missing_null_and_wrong_types(
    case: RowCase, field: str, mutation: Literal["MISSING", "NULL", "NUMBER", "EMPTY", "SPACE"]
) -> None:
    row = deepcopy(case.row)
    if mutation == "MISSING":
        row.pop(field)
    elif mutation == "NULL":
        row[field] = None
    elif mutation == "NUMBER":
        row[field] = 123
    else:
        row[field] = "" if mutation == "EMPTY" else " "
    with pytest.raises(ValidationError):
        parse_instrument_page(payload(case, row), case.feed())


class SyntheticSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", cases(), ids=lambda case: f"{case.venue}-{case.market_type}")
async def test_invalid_row_cannot_authorize_a_status_change_or_advance_baseline(
    tmp_path: Path, case: RowCase
) -> None:
    phase: Literal["BASELINE", "INVALID", "VALID"] = "BASELINE"

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "synthetic.example"
        row = deepcopy(case.row)
        if phase != "BASELINE":
            row[case.status_field] = case.changed_status
        if phase == "INVALID":
            row.pop(case.rejected_field)
        return httpx.Response(200, json=payload(case, row))

    repo = MemoryRepository()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    source = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": case.feed().url,
                    "venue": case.venue,
                    "market_type": case.market_type,
                }
            ],
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        service.sources["S14"] = source
        try:
            assert (await service.poll_once("S14")).items == 0
            before = await repo.get("ops.source_checkpoints", "S14:0:instruments")
            assert before is not None
            phase = "INVALID"
            refused = await service.poll_once("S14")
            assert refused.status == "DEGRADED" and refused.items == 0
            assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == before
            assert not await repo.list("ingest.source_items")
            phase = "VALID"
            accepted = await service.poll_once("S14")
            assert accepted.status == "HEALTHY" and accepted.items == 1
            [stored] = await repo.list("ingest.source_items")
            assert stored["change"] == "STATE_UPDATE"
        finally:
            await service.close()
    await budgets.shutdown()


def test_unknown_metadata_enrichment_is_not_a_proven_instrument_change() -> None:
    before = InstrumentSnapshot.model_validate(
        {
            "AAAUSDT": {
                "venue": "bybit",
                "market_type": "PERP_LINEAR",
                "symbol": "AAAUSDT",
                "base": None,
                "quote": None,
                "contract_type": None,
                "status": "Trading",
            }
        }
    )
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    after = parse_instrument_page(payload(case, case.row), case.feed()).snapshot
    configured = source_feed(
        {"kind": "instruments", "url": case.feed().url, "venue": "bybit", "market_type": "PERP_LINEAR"}
    )
    pages = [InstrumentPage(payload=encode_json(payload(case, case.row)), observed_at=CLOCK)]
    enriched, _ = instrument_changes(after, before, configured, pages, {"AAAUSDT": 0}, CLOCK)
    assert enriched == []
    changed = deepcopy(case.row)
    changed["status"] = "Settling"
    observed = parse_instrument_page(payload(case, changed), case.feed()).snapshot
    transitions, _ = instrument_changes(observed, before, configured, pages, {"AAAUSDT": 0}, CLOCK)
    assert len(transitions) == 1 and transitions[0].discovery.change == "STATE_UPDATE"
    assert '"base": null' in transitions[0].discovery.text


def test_known_metadata_change_still_creates_an_instrument_transition() -> None:
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    before = parse_instrument_page(payload(case, case.row), case.feed()).snapshot
    changed = deepcopy(case.row)
    changed["quoteCoin"] = "USDC"
    after = parse_instrument_page(payload(case, changed), case.feed()).snapshot
    configured = source_feed(
        {"kind": "instruments", "url": case.feed().url, "venue": "bybit", "market_type": "PERP_LINEAR"}
    )
    rows, _ = instrument_changes(
        after,
        before,
        configured,
        [InstrumentPage(payload=encode_json(payload(case, changed)), observed_at=CLOCK)],
        {"AAAUSDT": 0},
        CLOCK,
    )
    assert len(rows) == 1 and rows[0].discovery.change == "STATE_UPDATE"


def test_documented_optional_live_fields_remain_unknown_when_not_declared() -> None:
    for case in cases():
        page = parse_instrument_page(payload(case, case.row), case.feed())
        observed = next(iter(page.snapshot.root.values()))
        if case.venue == "hyperliquid":
            assert observed.status == "TRADING" and observed.quote is None
        if case.venue == "okx" and case.market_type == "PERP_LINEAR":
            assert observed.base is None and observed.quote is None
        if case.venue == "coinbase":
            assert isinstance(observed, CoinbaseInstrument) and observed.status_flags is not None
            assert observed.status_flags.trading_disabled is None
            assert "trading_disabled" not in observed.status_flags.model_fields_set


@pytest.mark.parametrize("market_type", ["SPOT", "PERP_LINEAR"])
@pytest.mark.parametrize("category", [None, True, 123, "inverse", "WRONG"])
def test_bybit_declared_category_cannot_be_guessed_from_the_feed(
    market_type: MarketType,
    category: JsonValue,
) -> None:
    result: dict[str, JsonValue] = {"category": category, "list": [], "nextPageCursor": ""}
    declared = FeedParsingSpec(
        url="https://synthetic.example/catalog", venue="bybit", market_type=market_type
    )
    with pytest.raises(ValidationError):
        parse_instrument_page({"retCode": 0, "result": result}, declared)
    result.pop("category")
    with pytest.raises(ValidationError):
        parse_instrument_page({"retCode": 0, "result": result}, declared)


@pytest.mark.asyncio
@pytest.mark.parametrize("market_type", ["SPOT", "PERP_LINEAR"])
async def test_bybit_wrong_catalog_category_cannot_authorize_removal(
    tmp_path: Path,
    market_type: MarketType,
) -> None:
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == market_type)
    phase: Literal["BASELINE", "WRONG_CATEGORY", "VALID_EMPTY"] = "BASELINE"
    expected_category = "spot" if market_type == "SPOT" else "linear"

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "synthetic.example"
        if phase == "BASELINE":
            return httpx.Response(200, json=payload(case, case.row))
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {
                    "category": ("linear" if expected_category == "spot" else "spot")
                    if phase == "WRONG_CATEGORY"
                    else expected_category,
                    "list": [],
                    "nextPageCursor": "",
                },
            },
        )

    repo = MemoryRepository()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    configured = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": case.feed().url,
                    "venue": "bybit",
                    "market_type": market_type,
                }
            ],
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
        service.sources["S14"] = configured
        try:
            assert (await service.poll_once("S14")).items == 0
            baseline = await repo.get("ops.source_checkpoints", "S14:0:instruments")
            phase = "WRONG_CATEGORY"
            rejected = await service.poll_once("S14")
            assert rejected.status == "DEGRADED" and rejected.items == 0
            assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == baseline
            assert not await repo.list("ingest.source_items")
            phase = "VALID_EMPTY"
            admitted = await service.poll_once("S14")
            assert admitted.status == "HEALTHY" and admitted.items == 1
            [removed] = await repo.list("ingest.source_items")
            assert removed["change"] == "REMOVED"
            assert (await service.poll_once("S14")).items == 0
        finally:
            await service.close()
    await budgets.shutdown()


@pytest.mark.parametrize(
    ("venue", "market_type", "wire"),
    [
        ("coinbase", "PERP_LINEAR", []),
        ("kraken", "PERP_LINEAR", {"error": [], "result": {}}),
        ("hyperliquid", "SPOT", {"universe": []}),
    ],
)
def test_adapter_refuses_a_market_surface_it_does_not_implement(
    venue: Venue,
    market_type: MarketType,
    wire: JsonValue,
) -> None:
    with pytest.raises(ValueError, match="adapter implements only"):
        parse_instrument_page(
            wire,
            FeedParsingSpec(
                url="https://synthetic.example/catalog",
                venue=venue,
                market_type=market_type,
            ),
        )


@pytest.mark.parametrize(
    ("venue", "market_type", "field"),
    [
        ("hyperliquid", "PERP_LINEAR", "quote"),
        ("okx", "SPOT", "ctType"),
        ("okx", "PERP_LINEAR", "baseCcy"),
        ("okx", "PERP_LINEAR", "quoteCcy"),
        ("coinbase", "SPOT", "trading_disabled"),
    ],
)
@pytest.mark.parametrize("value", [None, 123, [], {}])
def test_optional_protocol_fact_cannot_be_present_with_an_invalid_type(
    venue: Venue,
    market_type: MarketType,
    field: str,
    value: JsonValue,
) -> None:
    case = next(case for case in cases() if case.venue == venue and case.market_type == market_type)
    assert parse_instrument_page(payload(case, case.row), case.feed()).snapshot.root
    row = {**case.row, field: value}
    with pytest.raises(ValidationError) as failure:
        parse_instrument_page(payload(case, row), case.feed())
    errors = failure.value.errors(include_input=False)
    assert any(field in error["loc"] or error["type"] == "value_error" for error in errors)


def test_coinbase_auction_is_independent_of_the_exclusive_order_restriction_flags() -> None:
    case = next(case for case in cases() if case.venue == "coinbase")
    auction = {**case.row, "auction_mode": True, "limit_only": True}
    parsed = parse_instrument_page(payload(case, auction), case.feed()).snapshot.root["AAA-USD"]
    assert isinstance(parsed, CoinbaseInstrument) and parsed.status_flags is not None
    assert parsed.status_flags.auction_mode is True and parsed.status_flags.limit_only is True
    with pytest.raises(ValidationError, match="contradictory trading restrictions"):
        parse_instrument_page(payload(case, {**auction, "post_only": True}), case.feed())


type DuplicateFact = Literal["SUCCESS", "CATEGORY", "LIST", "ROW_STATUS", "ESCAPED_ROW_ID"]


def duplicate_catalog_wire(case: RowCase, fact: DuplicateFact) -> bytes:
    original = encode_json(payload(case, case.row))
    if fact == "SUCCESS":
        return original.replace(b'"retCode":0', b'"retCode":1,"retCode":0', 1)
    if fact == "CATEGORY":
        return original.replace(b'"category":"linear"', b'"category":"spot","category":"linear"', 1)
    if fact == "LIST":
        return (
            b'{"retCode":0,"result":{"category":"linear","list":['
            + encode_json(case.row)
            + b'],"list":[],"nextPageCursor":""}}'
        )
    if fact == "ROW_STATUS":
        return original.replace(b'"status":"Trading"', b'"status":"Trading","status":"Settling"', 1)
    return original.replace(
        b'"symbol":"AAAUSDT"', b'"' + bytes([92]) + b'u0073ymbol":"AAAUSDT","symbol":"AAAUSDT"', 1
    )


@pytest.mark.parametrize("fact", ["SUCCESS", "CATEGORY", "LIST", "ROW_STATUS", "ESCAPED_ROW_ID"])
def test_duplicate_raw_catalog_facts_cannot_be_collapsed_before_validation(fact: DuplicateFact) -> None:
    from tank.contracts.json import DuplicateJsonKeyError

    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    with pytest.raises(DuplicateJsonKeyError):
        parse_instrument_page(duplicate_catalog_wire(case, fact), case.feed())


@pytest.mark.asyncio
async def test_ambiguous_catalog_cannot_advance_snapshot_or_etag_and_retained_raw_survives(
    tmp_path: Path,
) -> None:
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    phase: Literal["BASELINE", "CHANGE", "AMBIGUOUS", "EMPTY"] = "BASELINE"
    conditionals: list[str | None] = []

    def respond(request: httpx.Request) -> httpx.Response:
        conditional = request.headers.get("If-None-Match")
        conditionals.append(conditional)
        if phase == "BASELINE":
            return httpx.Response(200, json=payload(case, case.row), headers={"ETag": '"base"'})
        if phase == "CHANGE":
            return httpx.Response(
                200, json=payload(case, {**case.row, "status": "Settling"}), headers={"ETag": '"changed"'}
            )
        if phase == "AMBIGUOUS":
            return httpx.Response(
                200, content=duplicate_catalog_wire(case, "LIST"), headers={"ETag": '"ambiguous"'}
            )
        if conditional == '"ambiguous"':
            return httpx.Response(304, headers={"ETag": '"ambiguous"'})
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {
                    "category": "linear",
                    "list": [],
                    "nextPageCursor": "",
                },
            },
            headers={"ETag": '"empty"'},
        )

    repo = MemoryRepository()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    configured = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": case.feed().url,
                    "venue": "bybit",
                    "market_type": "PERP_LINEAR",
                }
            ],
        }
    )
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
            service.sources["S14"] = configured
            try:
                assert (await service.poll_once("S14")).items == 0
                phase = "CHANGE"
                assert (await service.poll_once("S14")).items == 1
                [original] = await repo.list("ingest.source_items")
                raw_ref = original["raw_ref"]
                item_id = original["item_id"]
                assert isinstance(item_id, str)
                assert isinstance(raw_ref, str)
                original_raw = Path(raw_ref).read_bytes()
                baseline = await repo.get("ops.source_checkpoints", "S14:0:instruments")
                phase = "AMBIGUOUS"
                refused = await service.poll_once("S14")
                assert refused.status == "DEGRADED" and refused.items == 0
                assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == baseline
                assert await repo.list("ingest.source_items") == [original]
                assert Path(raw_ref).read_bytes() == original_raw
                failed_checkpoint = await repo.get("ops.source_checkpoints", "S14:0")
                assert failed_checkpoint is not None and "etag" not in failed_checkpoint
                phase = "EMPTY"
                accepted = await service.poll_once("S14")
                assert accepted.status == "HEALTHY" and accepted.items == 1
                assert conditionals[-1] is None
                assert (await service.poll_once("S14")).items == 0
                records = await repo.list("ingest.source_items")
                assert len(records) == 2 and sum(row["change"] == "REMOVED" for row in records) == 1
                assert await repo.get("ingest.source_items", item_id) == original
                assert Path(raw_ref).read_bytes() == original_raw
            finally:
                await service.close()
    finally:
        await budgets.shutdown()


@pytest.mark.parametrize("invalid", [" AAAUSDT", "AAAUSDT ", "AAA USDT", "AAA\tUSDT", "AAA\u200bUSDT"])
def test_protocol_identity_whitespace_or_control_cannot_create_a_different_symbol(invalid: str) -> None:
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    with pytest.raises(ValidationError, match="identity cannot contain"):
        parse_instrument_page(payload(case, {**case.row, "symbol": invalid}), case.feed())


def test_native_kraken_key_is_an_actual_identity_not_a_string_coercion() -> None:
    case = next(case for case in cases() if case.venue == "kraken")
    with pytest.raises(ValueError, match="identity cannot contain"):
        parse_instrument_page({"error": [], "result": {" XAAAZUSD ": case.row}}, case.feed())


@pytest.mark.parametrize(
    ("code", "kind"),
    [
        ("PERPETUAL", "PERPETUAL"),
        ("PERPETUAL_DELIVERING", "PERPETUAL"),
        ("CURRENT_MONTH", "DATED_FUTURE"),
        ("NEXT_MONTH", "DATED_FUTURE"),
        ("CURRENT_QUARTER", "DATED_FUTURE"),
        ("NEXT_QUARTER", "DATED_FUTURE"),
    ],
)
def test_binance_observed_contract_family_is_not_the_requested_acquisition_lane(
    code: str,
    kind: str,
) -> None:
    from tank.contracts.source_checkpoints import BinanceContractObservation, VenueInstrument

    case = next(case for case in cases() if case.venue == "binance" and case.market_type == "PERP_LINEAR")
    row = {**case.row, "contractType": code, "marginAsset": "USDC"}
    observed = next(iter(parse_instrument_page(payload(case, row), case.feed()).snapshot.root.values()))
    assert isinstance(observed, VenueInstrument)
    assert observed.acquisition_market_type == "PERP_LINEAR"
    assert isinstance(observed.reported_contract, BinanceContractObservation)
    assert observed.reported_contract.kind == kind
    assert observed.reported_contract.contract_type == code
    assert observed.reported_contract.margin_asset == "USDC"


@pytest.mark.parametrize(
    ("code", "kind", "mode", "settle"),
    [
        ("LinearPerpetual", "PERPETUAL", "LINEAR", "USDT"),
        ("InversePerpetual", "PERPETUAL", "INVERSE", "AAA"),
        ("LinearFutures", "DATED_FUTURE", "LINEAR", "USDC"),
        ("InverseFutures", "DATED_FUTURE", "INVERSE", "AAA"),
    ],
)
def test_bybit_reported_contract_and_settlement_survive_a_linear_query(
    code: str,
    kind: str,
    mode: str,
    settle: str,
) -> None:
    from tank.contracts.source_checkpoints import BybitContractObservation, VenueInstrument

    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    observed = next(
        iter(
            parse_instrument_page(
                payload(case, {**case.row, "contractType": code, "settleCoin": settle}),
                case.feed(),
            ).snapshot.root.values()
        )
    )
    assert isinstance(observed, VenueInstrument)
    assert observed.acquisition_market_type == "PERP_LINEAR"
    assert isinstance(observed.reported_contract, BybitContractObservation)
    assert observed.reported_contract.kind == kind and observed.reported_contract.settlement_mode == mode
    assert observed.reported_contract.settle_coin == settle


def test_legacy_acquisition_alias_preserves_snapshot_digest_and_external_identity() -> None:
    from tank.contracts.source_checkpoints import (
        InstrumentAcquisitionScope,
        ScopedInstrumentBaseline,
        VenueInstrument,
    )
    from tank.util import sha256

    legacy: dict[str, JsonValue] = {
        "venue": "bybit",
        "market_type": "PERP_LINEAR",
        "symbol": "AAAUSDT",
        "base": "AAA",
        "quote": "USDT",
        "status": "Settling",
        "contract_type": "LinearPerpetual",
    }
    named = VenueInstrument(
        venue="bybit",
        acquisition_market_type="PERP_LINEAR",
        symbol="AAAUSDT",
        base="AAA",
        quote="USDT",
        status="Settling",
        contract_type="LinearPerpetual",
    )
    snapshot = InstrumentSnapshot(root={"AAAUSDT": named})
    legacy_snapshot: dict[str, JsonValue] = {"AAAUSDT": legacy}
    assert snapshot.storage_record() == legacy_snapshot
    assert named.reported_contract is None
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    configured = source_feed(
        {
            "kind": "instruments",
            "url": case.feed().url,
            "venue": "bybit",
            "market_type": "PERP_LINEAR",
        }
    )
    stored = ScopedInstrumentBaseline.model_validate(
        {
            "source_id": "S14",
            "acquisition_scope": InstrumentAcquisitionScope.from_feed(configured),
            "instruments": legacy_snapshot,
            "sha256": sha256(legacy_snapshot),
            **fixture_versions().model_dump(mode="json"),
        }
    )
    assert stored.instruments.storage_record() == legacy_snapshot
    previous = InstrumentSnapshot.model_validate({"AAAUSDT": {**legacy, "status": "Trading"}})
    rows, _ = instrument_changes(
        snapshot,
        previous,
        configured,
        [InstrumentPage(payload=encode_json(payload(case, case.row)), observed_at=CLOCK)],
        {"AAAUSDT": 0},
        CLOCK,
    )
    assert rows[0].discovery.external_id == "bybit|PERP_LINEAR|AAAUSDT|" + sha256(legacy)
    evidence = decode_unique_json(rows[0].discovery.text)
    assert isinstance(evidence, dict)
    after = evidence["after"]
    assert isinstance(after, dict)
    assert "acquisition_market_type" in after and "market_type" not in after


async def assert_mixed_bybit_contract_restart(repo: Repository, tmp_path: Path) -> None:
    from tank.contracts.source_checkpoints import (
        BybitContractObservation,
        ScopedInstrumentBaseline,
        VenueInstrument,
    )

    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    variants: tuple[tuple[str, str, str], ...] = (
        ("AAAUSDT", "LinearPerpetual", "USDT"),
        ("AAAUSDT_FUT", "LinearFutures", "USDT"),
        ("AAAUSD", "InversePerpetual", "AAA"),
        ("AAAUSD_FUT", "InverseFutures", "AAA"),
    )
    phase: Literal["BASELINE", "INVALID", "CHANGE"] = "BASELINE"

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.params["category"] == "linear" and request.url.params["baseCoin"] == "AAA"
        rows: list[JsonValue] = []
        for symbol, code, settlement in variants:
            row = {**case.row, "symbol": symbol, "contractType": code, "settleCoin": settlement}
            if phase == "INVALID" and symbol == "AAAUSD_FUT":
                row["contractType"] = "UNREPORTED_NEW_CONTRACT"
            if phase == "CHANGE" and symbol in {"AAAUSDT_FUT", "AAAUSD_FUT"}:
                row["status"] = "Delivering"
            rows.append(row)
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {"category": "linear", "list": rows, "nextPageCursor": ""},
            },
        )

    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    configured = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": case.feed().url + "?category=linear&baseCoin=AAA",
                    "venue": "bybit",
                    "market_type": "PERP_LINEAR",
                }
            ],
        }
    )
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
            service.sources["S14"] = configured
            assert (await service.poll_once("S14")).items == 0
            baseline = await repo.get("ops.source_checkpoints", "S14:0:instruments")
            assert baseline is not None
            observed = ScopedInstrumentBaseline.from_document(baseline)
            assert set(observed.instruments.root) == {symbol for symbol, _, _ in variants}
            inverse = observed.instruments.root["AAAUSD_FUT"]
            assert isinstance(inverse, VenueInstrument) and isinstance(
                inverse.reported_contract, BybitContractObservation
            )
            assert (
                inverse.reported_contract.kind == "DATED_FUTURE"
                and inverse.reported_contract.settlement_mode == "INVERSE"
            )
            await service.close()
            restarted = SourceService(repo, settings, http=http, versions=fixture_versions(), budgets=budgets)
            restarted.sources["S14"] = configured
            try:
                assert (await restarted.poll_once("S14")).items == 0
                phase = "INVALID"
                invalid = await restarted.poll_once("S14")
                assert invalid.status == "DEGRADED" and invalid.items == 0
                assert await repo.get("ops.source_checkpoints", "S14:0:instruments") == baseline
                assert not await repo.list("ingest.source_items")
                phase = "CHANGE"
                assert (await restarted.poll_once("S14")).items == 1
                [changed] = await repo.list("ingest.source_items")
                assert changed["change"] == "STATE_UPDATE"
                title = changed["title"]
                assert isinstance(title, str)
                assert "Bybit instrument catalog" in title and "PERP" not in title
                assert (await restarted.poll_once("S14")).items == 0
            finally:
                await restarted.close()
    finally:
        await budgets.shutdown()


@pytest.mark.asyncio
async def test_mixed_bybit_contracts_restart_without_filtering_or_false_removal(tmp_path: Path) -> None:
    await assert_mixed_bybit_contract_restart(MemoryRepository(), tmp_path)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_mixed_bybit_contracts_preserve_scope_and_actual_kind(
    tmp_path: Path,
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await assert_mixed_bybit_contract_restart(repo, tmp_path)


def test_reported_contract_cannot_be_relabelled_or_promoted_from_unknown_history() -> None:
    from tank.contracts.source_checkpoints import (
        BybitContractObservation,
        InstrumentAcquisitionScope,
        InstrumentSnapshotUpdate,
    )
    from tank.util import sha256

    with pytest.raises(ValidationError, match="observed family/mode"):
        BybitContractObservation(
            provider="bybit",
            kind="PERPETUAL",
            contract_type="LinearFutures",
            settlement_mode="LINEAR",
            settle_coin="USDT",
        )
    legacy = InstrumentSnapshot.model_validate(
        {
            "AAAUSDT": {
                "venue": "bybit",
                "market_type": "PERP_LINEAR",
                "symbol": "AAAUSDT",
                "base": "AAA",
                "quote": "USDT",
                "status": "Trading",
                "contract_type": "LinearPerpetual",
            }
        }
    )
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    configured = source_feed(
        {
            "kind": "instruments",
            "url": case.feed().url,
            "venue": "bybit",
            "market_type": "PERP_LINEAR",
        }
    )
    with pytest.raises(ValidationError, match="requires reported contract observations"):
        InstrumentSnapshotUpdate(
            source_id="S14",
            checkpoint_key="S14:0:instruments",
            snapshot=legacy,
            acquisition_scope=InstrumentAcquisitionScope.from_feed(configured),
            archives=[],
            sha256=sha256(legacy.storage_record()),
            versions=fixture_versions(),
        )


def test_known_reported_settlement_change_remains_a_material_catalog_transition() -> None:
    case = next(case for case in cases() if case.venue == "bybit" and case.market_type == "PERP_LINEAR")
    before = parse_instrument_page(payload(case, case.row), case.feed()).snapshot
    changed = {**case.row, "settleCoin": "USDC"}
    after = parse_instrument_page(payload(case, changed), case.feed()).snapshot
    configured = source_feed(
        {
            "kind": "instruments",
            "url": case.feed().url,
            "venue": "bybit",
            "market_type": "PERP_LINEAR",
        }
    )
    changes, _ = instrument_changes(
        after,
        before,
        configured,
        [
            InstrumentPage(
                payload=encode_json(payload(case, changed)),
                observed_at=CLOCK,
            )
        ],
        {"AAAUSDT": 0},
        CLOCK,
    )
    assert len(changes) == 1 and changes[0].discovery.change == "STATE_UPDATE"
    assert '"settle_coin": "USDC"' in changes[0].discovery.text


@pytest.mark.parametrize(
    ("venue", "market_type", "field"),
    [
        ("hyperliquid", "PERP_LINEAR", "quote"),
        ("okx", "PERP_LINEAR", "baseCcy"),
        ("okx", "PERP_LINEAR", "quoteCcy"),
    ],
)
def test_present_optional_currency_identity_is_not_whitespace(
    venue: Venue,
    market_type: MarketType,
    field: str,
) -> None:
    case = next(case for case in cases() if case.venue == venue and case.market_type == market_type)
    with pytest.raises(ValidationError, match="identity cannot contain"):
        parse_instrument_page(payload(case, {**case.row, field: " AAA "}), case.feed())


def test_okx_spot_cannot_claim_a_derivative_contract_mode() -> None:
    case = next(case for case in cases() if case.venue == "okx" and case.market_type == "SPOT")
    with pytest.raises(ValidationError):
        parse_instrument_page(payload(case, {**case.row, "ctType": "linear"}), case.feed())
    parsed = parse_instrument_page(payload(case, {**case.row, "ctType": ""}), case.feed())
    assert next(iter(parsed.snapshot.root.values())).contract_type == ""
