"""Instrument comparisons require the recorded acquisition scope, independent of config versions."""

import os
from pathlib import Path
from typing import Literal

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, source_definition

from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.repository import Repository as TypedRepository
from tank.contracts.source_checkpoints import (
    INSTRUMENT_BASELINE_ARCHIVES,
    InstrumentAcquisitionScope,
    InstrumentRecord,
    ScopedInstrumentBaseline,
)
from tank.contracts.source_items import InstrumentSourceItem, captured_item, parse_source_item
from tank.contracts.sources import SourceDefinition
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import MemoryRepository, Repository
from tank.util import sha256

ALL_URL = "https://synthetic.example/instruments?category=linear"
FILTERED_URL = ALL_URL + "&symbol=AAAUSDT"
POINTER = "S14:0:instruments"


class SyntheticSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class Catalog:
    def __init__(self) -> None:
        self.statuses = {"AAAUSDT": "Trading", "BBBUSDT": "Trading"}
        self.requests: list[httpx.Request] = []
        self.force_304 = False

    def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.force_304:
            return httpx.Response(304)
        selected = request.url.params.get("symbol")
        entries: list[JsonValue] = [
            {
                "symbol": symbol,
                "baseCoin": symbol.removesuffix("USDT"),
                "quoteCoin": "USDT",
                "status": status,
                "settleCoin": "USDT",
                "contractType": "LinearPerpetual",
            }
            for symbol, status in self.statuses.items()
            if selected is None or selected == symbol
        ]
        payload: dict[str, JsonValue] = {
            "retCode": 0,
            "result": {"category": "linear", "list": entries, "nextPageCursor": ""},
        }
        return httpx.Response(200, json=payload, headers={"etag": sha256(payload)})


class ChangeEvidence(DomainModel):
    before: InstrumentRecord | None
    after: InstrumentRecord | None


def assert_change(item: InstrumentSourceItem, symbol: str) -> None:
    assert item.change == "STATE_UPDATE"
    evidence = ChangeEvidence.model_validate_json(item.text)
    assert evidence.after is not None and evidence.after.symbol == symbol


def versions(number: int) -> ResolvedVersionIdentity:
    return ResolvedVersionIdentity(
        config_version=f"synthetic-scope-config-{number}", code_version="synthetic-scope-code"
    )


def bind(service: SourceService, url: str) -> SourceDefinition:
    source = source_definition(
        {
            "source_id": "S14",
            "feeds": [{"kind": "instruments", "url": url, "venue": "bybit", "market_type": "PERP_LINEAR"}],
        }
    )
    service.sources["S14"] = source
    return source


async def assert_scope_restart_and_unrelated_version_change(repository: Repository, tmp_path: Path) -> None:
    catalog = Catalog()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(catalog.respond)) as http:
        first = SourceService(repository, settings, http=http, versions=versions(1), budgets=budgets)
        original = bind(first, ALL_URL)
        scope = InstrumentAcquisitionScope.from_feed(original.feeds[0])
        assert (await first.poll_once("S14")).items == 0
        baseline = await repository.get("ops.source_checkpoints", POINTER)
        assert baseline is not None
        await first.close()

        filtered = SourceService(repository, settings, http=http, versions=versions(2), budgets=budgets)
        narrowed = bind(filtered, FILTERED_URL)
        try:
            result = await filtered.poll_once("S14")
            assert result.status == "HEALTHY" and result.items == 0
            assert not await repository.list("ingest.source_items")
            assert "if-none-match" not in catalog.requests[-1].headers
            scoped = await repository.get("ops.source_checkpoints", POINTER + ":scope:" + scope.digest)
            assert scoped is not None
            assert ScopedInstrumentBaseline.from_document(scoped) == ScopedInstrumentBaseline.from_document(
                baseline
            )
            archives = TypedRepository(repository, INSTRUMENT_BASELINE_ARCHIVES)
            records = [
                record
                async for record in archives.iter_all(
                    filters={"checkpoint_kind": "INSTRUMENT_BASELINE_ARCHIVE"}
                )
            ]
            assert len(records) == 1
            assert records[0].original_record == baseline and records[0].original_scope == scope
            assert InstrumentAcquisitionScope.from_feed(narrowed.feeds[0]) != scope
        finally:
            await filtered.close()

        returned = SourceService(repository, settings, http=http, versions=versions(3), budgets=budgets)
        bind(returned, ALL_URL)
        try:
            assert (await returned.poll_once("S14")).items == 0
            assert not await repository.list("ingest.source_items")
        finally:
            await returned.close()
        changed_config = SourceService(repository, settings, http=http, versions=versions(4), budgets=budgets)
        bind(changed_config, ALL_URL)
        catalog.statuses["BBBUSDT"] = "Settling"
        try:
            outcome = await changed_config.poll_once("S14")
            assert outcome.status == "HEALTHY" and outcome.items == 1
            [wire] = await repository.list("ingest.source_items")
            item = parse_source_item(wire)
            assert isinstance(item, InstrumentSourceItem)
            assert_change(item, "BBBUSDT")
            assert item.discovery_config_version == versions(4).config_version
        finally:
            await changed_config.close()
    await budgets.shutdown()


@pytest.mark.asyncio
async def test_scope_restart_does_not_invent_removal_or_reset_on_global_versions(tmp_path: Path) -> None:
    await assert_scope_restart_and_unrelated_version_change(MemoryRepository(), tmp_path)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_scope_restart_does_not_invent_removal_or_reset_on_global_versions(
    tmp_path: Path, postgres_template: PostgresTemplate
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repository:
        await assert_scope_restart_and_unrelated_version_change(repository, tmp_path)


@pytest.mark.asyncio
async def test_old_scope_pending_capture_retains_its_actual_feed_raw_and_versions(tmp_path: Path) -> None:
    repository = MemoryRepository()
    catalog = Catalog()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(catalog.respond)) as http:
        first = SourceService(repository, settings, http=http, versions=versions(1), budgets=budgets)
        source = bind(first, ALL_URL)
        assert (await first.poll_once("S14")).items == 0
        catalog.statuses["BBBUSDT"] = "Settling"
        await first.poller.acquire(source, 0)
        queued = await repository.get("ops.source_checkpoints", "S14:0")
        assert queued is not None
        batches = queued["pending_batches"]
        assert isinstance(batches, list) and len(batches) == 1
        batch = batches[0]
        assert isinstance(batch, dict)
        rows = batch["rows"]
        assert isinstance(rows, list) and len(rows) == 1
        row = rows[0]
        assert isinstance(row, dict)
        capture = captured_item("S14", row).capture
        raw_file = Path(capture.raw_ref)
        raw_bytes = raw_file.read_bytes()
        await first.close()
        restarted = SourceService(repository, settings, http=http, versions=versions(2), budgets=budgets)
        bind(restarted, FILTERED_URL)
        try:
            outcome = await restarted.poll_once("S14")
            assert outcome.status == "HEALTHY" and outcome.items == 1
            [wire] = await repository.list("ingest.source_items")
            item = parse_source_item(wire)
            assert isinstance(item, InstrumentSourceItem)
            assert_change(item, "BBBUSDT")
            assert item.item_id == capture.item_id and item.feed == ALL_URL
            assert item.raw_ref == capture.raw_ref and raw_file.read_bytes() == raw_bytes
            assert item.observed_at == capture.observed_at
            assert item.discovery_config_version == versions(1).config_version
        finally:
            await restarted.close()
    await budgets.shutdown()


@pytest.mark.asyncio
async def test_unattributed_historical_baseline_is_archived_without_inferred_scope(tmp_path: Path) -> None:
    repository = MemoryRepository()
    previous: dict[str, JsonValue] = {
        "source_id": "S14",
        "config_version": "historical-config",
        "code_version": "historical-code",
        "instruments": {
            symbol: {
                "venue": "bybit",
                "market_type": "PERP_LINEAR",
                "symbol": symbol,
                "base": symbol.removesuffix("USDT"),
                "quote": "USDT",
                "status": "Trading",
                "contract_type": "LinearPerpetual",
            }
            for symbol in ("AAAUSDT", "BBBUSDT")
        },
    }
    previous["sha256"] = sha256(previous["instruments"])
    await repository.put("ops.source_checkpoints", POINTER, previous)
    catalog = Catalog()
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(catalog.respond)) as http:
        service = SourceService(
            repository,
            SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None),
            http=http,
            versions=versions(2),
            budgets=budgets,
        )
        bind(service, FILTERED_URL)
        try:
            assert (await service.poll_once("S14")).items == 0
            assert not await repository.list("ingest.source_items")
            archives = TypedRepository(repository, INSTRUMENT_BASELINE_ARCHIVES)
            records = [
                record
                async for record in archives.iter_all(
                    filters={"checkpoint_kind": "INSTRUMENT_BASELINE_ARCHIVE"}
                )
            ]
            assert len(records) == 1
            assert records[0].scope_declaration == "NOT_RECORDED"
            assert records[0].original_scope is None and records[0].original_record == previous
        finally:
            await service.close()
    await budgets.shutdown()


@pytest.mark.asyncio
async def test_304_cannot_authorize_a_new_instrument_scope(tmp_path: Path) -> None:
    repository = MemoryRepository()
    catalog = Catalog()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(catalog.respond)) as http:
        service = SourceService(repository, settings, http=http, versions=versions(1), budgets=budgets)
        bind(service, ALL_URL)
        assert (await service.poll_once("S14")).items == 0
        prior = await repository.get("ops.source_checkpoints", POINTER)
        await service.close()
        catalog.force_304 = True
        restarted = SourceService(repository, settings, http=http, versions=versions(2), budgets=budgets)
        bind(restarted, FILTERED_URL)
        try:
            outcome = await restarted.poll_once("S14")
            assert outcome.status == "DEGRADED" and outcome.items == 0
            assert "if-none-match" not in catalog.requests[-1].headers
            assert await repository.get("ops.source_checkpoints", POINTER) == prior
            assert not await repository.list("ingest.source_items")
        finally:
            await restarted.close()
    await budgets.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["BODY", "HEADER"])
async def test_declared_body_or_header_filter_cannot_manufacture_a_removal(
    tmp_path: Path, selector: Literal["BODY", "HEADER"]
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        body = decode_json(request.content)
        assert isinstance(body, dict)
        selection = body["dex"] if selector == "BODY" else request.headers["X-Synthetic-Catalog"]
        names = ["AAA", "BBB"] if selection == "all" else ["AAA"]
        return httpx.Response(200, json={"universe": [{"name": name} for name in names]})

    def definition(selection: str) -> SourceDefinition:
        body: dict[str, JsonValue] = {"type": "meta"}
        headers: dict[str, JsonValue] = {}
        if selector == "BODY":
            body["dex"] = selection
        else:
            headers["X-Synthetic-Catalog"] = selection
        return source_definition(
            {
                "source_id": "S14",
                "feeds": [
                    {
                        "kind": "instruments",
                        "url": "https://synthetic.example/info",
                        "method": "POST",
                        "body": body,
                        "headers": headers,
                        "venue": "hyperliquid",
                        "market_type": "PERP_LINEAR",
                    }
                ],
            }
        )

    repository = MemoryRepository()
    settings = SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        for number, selection in enumerate(("all", "one"), start=1):
            service = SourceService(
                repository, settings, http=http, versions=versions(number), budgets=budgets
            )
            service.sources["S14"] = definition(selection)
            try:
                result = await service.poll_once("S14")
                assert result.status == "HEALTHY" and result.items == 0
                assert not await repository.list("ingest.source_items")
            finally:
                await service.close()
    await budgets.shutdown()


@pytest.mark.asyncio
async def test_pagination_preserves_the_declared_query_scope_including_blank_values(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params.get_list("retained") == ["one", "two"]
        assert request.url.params["settleCoin"] == ""
        next_page = "cursor" in request.url.params
        symbol = "BBBUSDT" if next_page else "AAAUSDT"
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {
                    "category": "linear",
                    "list": [
                        {
                            "symbol": symbol,
                            "baseCoin": symbol.removesuffix("USDT"),
                            "quoteCoin": "USDT",
                            "settleCoin": "USDT",
                            "contractType": "LinearPerpetual",
                            "status": "Trading",
                        }
                    ],
                    "nextPageCursor": "" if next_page else "next",
                },
            },
        )

    repository = MemoryRepository()
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(
            repository,
            SyntheticSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None),
            http=http,
            versions=versions(1),
            budgets=budgets,
        )
        bind(service, ALL_URL + "&settleCoin=&retained=one&retained=two")
        try:
            outcome = await service.poll_once("S14")
            assert outcome.status == "HEALTHY" and outcome.items == 0
            baseline = await repository.get("ops.source_checkpoints", POINTER)
            assert baseline is not None
            observed = ScopedInstrumentBaseline.from_document(baseline)
            assert set(observed.instruments.root) == {"AAAUSDT", "BBBUSDT"}
            assert len(requests) == 2 and requests[-1].url.params["cursor"] == "next"
        finally:
            await service.close()
    await budgets.shutdown()
