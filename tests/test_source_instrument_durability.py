"""Instrument baselines cannot outrun durable raw capture and pending queue state."""

import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions, source_definition

from tank.contracts import DomainModel
from tank.contracts.source_checkpoints import InstrumentRecord
from tank.contracts.source_items import InstrumentSourceItem, parse_source_item
from tank.contracts.sources import SourceDefinition
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import MemoryRepository, Repository
from tank.util import JsonInput, sha256


class FixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class VenueResponse:
    def __init__(self, status: str) -> None:
        self.status = status
        self.sent: list[bytes] = []

    def payload(self) -> dict[str, JsonValue]:
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "contractType": "LinearPerpetual",
                        "status": self.status,
                    }
                ],
                "nextPageCursor": "",
            },
        }

    def respond(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "synthetic.example"
        response = httpx.Response(200, json=self.payload())
        self.sent.append(response.content)
        return response


class RawFailureSource(SourceService):
    fail_next_raw: bool = False

    def raw(
        self, payload: bytes, *, source_id: str, item_id: str, observed_at: str | datetime
    ) -> tuple[str, str]:
        if self.fail_next_raw:
            self.fail_next_raw = False
            raise OSError("synthetic raw durability failure")
        return super().raw(payload, source_id=source_id, item_id=item_id, observed_at=observed_at)


class LaterIngestFailureSource(SourceService):
    fail_next_ingest: bool = False

    async def ingest(self, source: SourceDefinition, item: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        if self.fail_next_ingest:
            self.fail_next_ingest = False
            raise OSError("synthetic later ingest failure")
        return await super().ingest(source, item)


def has_pending_rows(record: Mapping[str, JsonInput]) -> bool:
    pending = record.get("pending_batches")
    return isinstance(pending, list) and any(
        isinstance(batch, Mapping) and isinstance(batch.get("rows"), list) and bool(batch["rows"])
        for batch in pending
    )


class PostWriteFailureRepository(MemoryRepository):
    fail_after_put: str | None = None

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        result = await super().put(collection, key, record, immutable=immutable)
        if (
            collection == "ops.source_checkpoints"
            and self.fail_after_put == key
            and (key.endswith(":instruments") or has_pending_rows(record))
        ):
            self.fail_after_put = None
            raise OSError("synthetic after checkpoint put")
        return result


def bind_instrument_feed(service: SourceService) -> None:
    service.sources["S14"] = source_definition(
        {
            "source_id": "S14",
            "feeds": [
                {
                    "kind": "instruments",
                    "url": "https://synthetic.example/instruments",
                    "venue": "bybit",
                    "market_type": "PERP_LINEAR",
                }
            ],
        }
    )


class InstrumentChangeEvidence(DomainModel):
    before: InstrumentRecord | None
    after: InstrumentRecord | None


async def assert_recovered_change(repository: Repository, response: VenueResponse) -> None:
    records = await repository.list("ingest.source_items")
    assert len(records) == 1
    item = parse_source_item(records[0])
    assert isinstance(item, InstrumentSourceItem)
    assert item.change == "STATE_UPDATE"
    assert item.published_at is None and item.published_prec == "COLLECTION_ONLY"
    assert item.published_prec_reason == "INCOMPLETE"
    checkpoint = await repository.get("ops.source_checkpoints", "S14:0")
    assert checkpoint is not None and item.observed_at == checkpoint["fetched_at"]
    transition = InstrumentChangeEvidence.model_validate_json(item.text)
    assert transition.before is not None and transition.before.status == "Trading"
    assert transition.after is not None and transition.after.status == "Settling"
    raw = zstandard.ZstdDecompressor().decompress(Path(item.raw_ref).read_bytes())
    assert sha256(raw) == item.raw_sha256
    assert raw == response.sent[-1]


@pytest.mark.asyncio
async def test_raw_failure_does_not_advance_snapshot_and_restart_recovers_change(tmp_path: Path) -> None:
    repository = MemoryRepository()
    response = VenueResponse("Trading")
    settings = FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response.respond)) as http:
        service = RawFailureSource(
            repository, settings, http=http, versions=fixture_versions(), budgets=budgets
        )
        bind_instrument_feed(service)
        assert (await service.poll_once("S14")).items == 0
        before = await repository.get("ops.source_checkpoints", "S14:0:instruments")
        assert before is not None
        response.status = "Settling"
        service.fail_next_raw = True
        failed = await service.poll_once("S14")
        assert failed.status == "DEGRADED" and failed.items == 0
        assert await repository.get("ops.source_checkpoints", "S14:0:instruments") == before
        checkpoint = await repository.get("ops.source_checkpoints", "S14:0")
        assert checkpoint is not None and checkpoint["pending_batches"] == []
        assert not await repository.list("ingest.source_items")
        await service.close()

        restarted = SourceService(
            repository, settings, http=http, versions=fixture_versions(), budgets=budgets
        )
        bind_instrument_feed(restarted)
        try:
            recovered = await restarted.poll_once("S14")
            assert recovered.status == "HEALTHY" and recovered.items == 1
            await assert_recovered_change(repository, response)
        finally:
            await restarted.close()
    await budgets.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_key", ["S14:0", "S14:0:instruments"])
async def test_actual_transaction_rolls_back_pending_and_snapshot_after_either_put(
    tmp_path: Path, failure_key: Literal["S14:0", "S14:0:instruments"]
) -> None:
    repository = PostWriteFailureRepository()
    response = VenueResponse("Trading")
    settings = FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
    budgets = fixture_request_budgets()
    async with httpx.AsyncClient(transport=httpx.MockTransport(response.respond)) as http:
        service = SourceService(repository, settings, http=http, versions=fixture_versions(), budgets=budgets)
        bind_instrument_feed(service)
        assert (await service.poll_once("S14")).items == 0
        before = await repository.get("ops.source_checkpoints", "S14:0:instruments")
        assert before is not None
        response.status = "Settling"
        repository.fail_after_put = failure_key
        failed = await service.poll_once("S14")
        assert failed.status == "DEGRADED" and failed.items == 0
        assert await repository.get("ops.source_checkpoints", "S14:0:instruments") == before
        checkpoint = await repository.get("ops.source_checkpoints", "S14:0")
        assert checkpoint is not None and checkpoint["pending_batches"] == []
        assert not await repository.list("ingest.source_items")
        await service.close()

        restarted = SourceService(
            repository, settings, http=http, versions=fixture_versions(), budgets=budgets
        )
        bind_instrument_feed(restarted)
        try:
            assert (await restarted.poll_once("S14")).items == 1
            await assert_recovered_change(repository, response)
        finally:
            await restarted.close()
    await budgets.shutdown()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("failure_key", ["S14:0", "S14:0:instruments"])
async def test_postgres_commit_failure_preserves_prior_baseline_and_restart(
    tmp_path: Path,
    postgres_template: PostgresTemplate,
    monkeypatch: pytest.MonkeyPatch,
    failure_key: Literal["S14:0", "S14:0:instruments"],
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repository:
        original_put = repository.put
        fail_armed = False

        async def fail_after_actual_put(
            collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
        ) -> dict[str, JsonValue]:
            nonlocal fail_armed
            result = await original_put(collection, key, record, immutable=immutable)
            if (
                fail_armed
                and collection == "ops.source_checkpoints"
                and key == failure_key
                and (key.endswith(":instruments") or has_pending_rows(record))
            ):
                fail_armed = False
                raise OSError("synthetic after actual PostgreSQL checkpoint put")
            return result

        monkeypatch.setattr(repository, "put", fail_after_actual_put)
        response = VenueResponse("Trading")
        settings = FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
        budgets = fixture_request_budgets()
        async with httpx.AsyncClient(transport=httpx.MockTransport(response.respond)) as http:
            service = SourceService(
                repository,
                settings,
                http=http,
                versions=fixture_versions(),
                budgets=budgets,
            )
            bind_instrument_feed(service)
            assert (await service.poll_once("S14")).items == 0
            before = await repository.get("ops.source_checkpoints", "S14:0:instruments")
            assert before is not None
            response.status = "Settling"
            fail_armed = True
            failed = await service.poll_once("S14")
            assert failed.status == "DEGRADED" and failed.items == 0
            assert not fail_armed
            assert await repository.get("ops.source_checkpoints", "S14:0:instruments") == before
            checkpoint = await repository.get("ops.source_checkpoints", "S14:0")
            assert checkpoint is not None and checkpoint["pending_batches"] == []
            assert not await repository.list("ingest.source_items")
            await service.close()

            restarted = SourceService(
                repository,
                settings,
                http=http,
                versions=fixture_versions(),
                budgets=budgets,
            )
            bind_instrument_feed(restarted)
            try:
                assert (await restarted.poll_once("S14")).items == 1
                await assert_recovered_change(repository, response)
            finally:
                await restarted.close()
        await budgets.shutdown()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_later_ingest_failure_reuses_original_durable_pending_capture(
    tmp_path: Path, postgres_template: PostgresTemplate
) -> None:
    database_url = os.environ["TANK_SOURCE_TEST_DATABASE_URL"]
    async with postgres_template.repository(database_url) as repository:
        response = VenueResponse("Trading")
        settings = FixtureSettings(data_dir=tmp_path, ops_progress_enabled=False, apify_api_token=None)
        budgets = fixture_request_budgets()
        async with httpx.AsyncClient(transport=httpx.MockTransport(response.respond)) as http:
            service = LaterIngestFailureSource(
                repository,
                settings,
                http=http,
                versions=fixture_versions(),
                budgets=budgets,
            )
            bind_instrument_feed(service)
            assert (await service.poll_once("S14")).items == 0
            before = await repository.get("ops.source_checkpoints", "S14:0:instruments")
            response.status = "Settling"
            service.fail_next_ingest = True
            failed = await service.poll_once("S14")
            assert failed.status == "DEGRADED" and failed.items == 0
            assert await repository.get("ops.source_checkpoints", "S14:0:instruments") != before
            state = await repository.get("ops.source_checkpoints", "S14:0")
            assert state is not None
            batches = state["pending_batches"]
            assert isinstance(batches, list) and len(batches) == 1
            batch = batches[0]
            assert isinstance(batch, dict)
            rows = batch["rows"]
            assert isinstance(rows, list) and len(rows) == 1
            row = rows[0]
            assert isinstance(row, dict)
            original_id, observed_at, raw_ref = row["item_id"], row["observed_at"], row["raw_ref"]
            assert isinstance(original_id, str) and isinstance(observed_at, str) and isinstance(raw_ref, str)
            raw_bytes = Path(raw_ref).read_bytes()
            assert not await repository.list("ingest.source_items")
            await service.close()

            restarted = SourceService(
                repository,
                settings,
                http=http,
                versions=fixture_versions(),
                budgets=budgets,
            )
            bind_instrument_feed(restarted)
            try:
                assert (await restarted.poll_once("S14")).items == 1
                [record] = await repository.list("ingest.source_items")
                item = parse_source_item(record)
                assert isinstance(item, InstrumentSourceItem)
                assert item.item_id == original_id and item.observed_at == observed_at
                assert item.raw_ref == raw_ref and Path(raw_ref).read_bytes() == raw_bytes
                assert item.published_at is None and item.published_prec == "COLLECTION_ONLY"
                latest = await repository.get("ops.source_checkpoints", "S14:0")
                assert latest is not None and latest["pending_batches"] == []
            finally:
                await restarted.close()
        await budgets.shutdown()
