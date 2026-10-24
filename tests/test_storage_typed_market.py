"""§14 typed market storage: ClickHouse behind the database repository equals the memory double."""

import os
from collections.abc import AsyncIterator

import pytest
from foundation_helpers import object_field, present, text_field
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.models import Record
from tank.storage import PostgresRepository, StorageBackend

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def typed_repo(postgres_template: PostgresTemplate) -> AsyncIterator[PostgresRepository]:
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn or not os.getenv("CLICKHOUSE_TEST_URL"):
        pytest.fail(
            "TANK_TEST_DATABASE_URL and CLICKHOUSE_TEST_URL required; creates owned disposable databases"
        )
    async with postgres_template.repository(dsn) as repo:
        yield repo


def flow(at: str = "2026-10-18T00:00:00Z") -> Record:
    return {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "minute_start": at,
        "buy_qty": "1.00000000000000000001",
        "sell_qty": "0",
        "buy_usd": "10",
        "sell_usd": "0",
        "size_buckets": {},
        "n_buy": 1,
        "n_sell": 0,
        "sum_sq_qty": "1",
        "complete": True,
        "recorded_at": "2026-10-18T00:01:00Z",
        "recorded_at_prec": "COLLECTION_ONLY",
        "first_trade": {"price": "123.456789012345678901"},
    }


async def test_ticker_partial_point_merge_and_read_queries(typed_repo: StorageBackend) -> None:
    repo = typed_repo
    await repo.migrate()
    a: Record = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "ts": "2026-10-18T00:00:00Z",
        "mark_price": "10",
        "field_sources": {"mark_price": {"as_of": "2026-10-18T00:00:00Z", "source_path": "ws"}},
    }
    await repo.put_market_batch("mkt.tickers_24h", [a], keys=["ws"])
    b: Record = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "ts": a["ts"],
        "index_price": "11",
        "field_sources": {"index_price": {"as_of": a["ts"], "source_path": "rest"}},
    }
    rows = await repo.put_market_batch("mkt.tickers_24h", [b], keys=["rest"])
    assert len(rows) == 1 and rows[0]["mark_price"] == "10" and rows[0]["index_price"] == "11"
    assert set(object_field(rows[0], "field_sources")) == {"mark_price", "index_price"}
    assert len(await repo.latest_market_records("mkt.tickers_24h")) == 1
    assert (
        present(
            await repo.latest_before(
                "mkt.tickers_24h", "ts", "2026-10-18T00:01:00Z", filters={"token_id": "BTC"}
            )
        )
    )["mark_price"] == "10"


async def test_funding_source_merge_survives_partial_restart(typed_repo: StorageBackend) -> None:
    repo = typed_repo
    await repo.migrate()
    base: Record = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "kind": "CURRENT_ESTIMATE",
        "as_of": "2026-10-18T00:00:00Z",
        "metric_key": "funding",
        "rate": "0.01",
        "interval_h": "8",
        "rate_8h_eq": "0.01",
        "capture_source": "WEBSOCKET",
    }
    ws: Record = dict(base, source_path="ws", received_ts="2026-10-18T00:00:01Z")
    await repo.put_market_batch(
        "mkt.funding_obs", [dict(ws, source_observations={"WEBSOCKET": ws})], keys=["ws"]
    )
    rest: Record = dict(
        base,
        rate="0.02",
        rate_8h_eq="0.02",
        capture_source="PUBLIC_REST",
        source_path="rest",
        received_ts="2026-10-18T00:00:02Z",
    )
    result = (
        await repo.put_market_batch(
            "mkt.funding_obs", [dict(rest, source_observations={"PUBLIC_REST": rest})], keys=["rest"]
        )
    )[0]
    assert result["rate"] == "0.01" and result["capture_source"] == "WEBSOCKET"
    assert set(object_field(result, "source_observations")) == {"WEBSOCKET", "PUBLIC_REST"}
    assert (
        object_field(object_field(result, "source_observations"), "WEBSOCKET")["received_ts"]
        == ws["received_ts"]
    )
    assert object_field(result, "source_disagreement")["present"] is True


async def test_memory_market_batch_uses_same_typed_contract() -> None:
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    rows = await repo.put_market_batch("mkt.flow_bars_1m", [flow(), flow()], keys=["a", "b"])
    assert len(rows) == 1
    assert (present(await repo.get("mkt.flow_bars_1m", "b")))["first_trade"] == flow()["first_trade"]
    assert await repo.put_market_batch("mkt.flow_bars_1m", [flow()], keys=["b"]) == []


@pytest.mark.parametrize("reverse", [False, True])
async def test_same_batch_funding_and_ticker_equal_sequential(
    typed_repo: StorageBackend, reverse: bool
) -> None:
    repo = typed_repo
    await repo.migrate()
    base: Record = {
        "instrument_id": "batch:BTC",
        "token_id": "BTC",
        "kind": "CURRENT_ESTIMATE",
        "as_of": "2026-10-18T00:00:00Z",
        "metric_key": "funding",
        "interval_h": "8",
    }
    rest: Record = dict(
        base,
        rate="0.02",
        rate_8h_eq="0.02",
        capture_source="PUBLIC_REST",
        source_path="rest",
        received_ts="2026-10-18T00:00:01Z",
    )
    ws: Record = dict(
        base,
        rate="0.01",
        rate_8h_eq="0.01",
        capture_source="WEBSOCKET",
        source_path="ws",
        received_ts="2026-10-18T00:00:02Z",
    )
    packets: list[Record] = [
        dict(rest, source_observations={"PUBLIC_REST": rest}),
        dict(ws, source_observations={"WEBSOCKET": ws}),
    ]
    if reverse:
        packets.reverse()
    batched = (await repo.put_market_batch("mkt.funding_obs", packets, keys=["one", "two"]))[0]
    assert batched["rate"] == "0.01"
    assert set(object_field(batched, "source_observations")) == {"PUBLIC_REST", "WEBSOCKET"}
    assert object_field(batched, "source_disagreement")["present"] is True
    await repo.delete("mkt.funding_obs", "two")
    for i, packet in enumerate(packets):
        await repo.put_market_batch("mkt.funding_obs", [packet], keys=[str(i)])
    sequential = (
        await repo.latest_market_records(
            "mkt.funding_obs", group_by=("instrument_id", "kind"), time_field="as_of"
        )
    )[0]
    for field in (
        "rate",
        "rate_8h_eq",
        "capture_source",
        "source_observations",
        "source_disagreement",
        "received_ts",
    ):
        assert batched[field] == sequential[field]
    a: Record = {
        "instrument_id": "batch:BTC",
        "token_id": "BTC",
        "ts": base["as_of"],
        "mark_price": "10",
        "field_sources": {
            "mark_price": {
                "as_of": "2026-10-18T00:00:02Z",
                "capture_source": "WEBSOCKET",
                "source_path": "ws",
            }
        },
    }
    b: Record = {
        "instrument_id": "batch:BTC",
        "token_id": "BTC",
        "ts": base["as_of"],
        "mark_price": "9",
        "index_price": "11",
        "field_sources": {
            "mark_price": {
                "as_of": "2026-10-18T00:00:01Z",
                "capture_source": "PUBLIC_REST",
                "source_path": "rest",
            },
            "index_price": {
                "as_of": "2026-10-18T00:00:01Z",
                "capture_source": "PUBLIC_REST",
                "source_path": "rest",
            },
        },
    }
    ticker_packets: list[Record] = [a, b]
    if reverse:
        ticker_packets.reverse()
    batched = (
        await repo.put_market_batch("mkt.tickers_24h", ticker_packets, keys=["ticker_one", "ticker_two"])
    )[0]
    assert batched["mark_price"] == "10" and batched["index_price"] == "11"
    await repo.delete("mkt.tickers_24h", "ticker_two")
    for i, packet in enumerate(ticker_packets):
        await repo.put_market_batch("mkt.tickers_24h", [packet], keys=["ticker_seq" + str(i)])
    sequential = (await repo.latest_market_records("mkt.tickers_24h"))[0]
    for field in (
        "mark_price",
        "index_price",
        "field_sources",
        "mark_as_of",
        "index_as_of",
        "capture_sources",
    ):
        assert batched[field] == sequential[field]


@pytest.mark.parametrize("reverse", [False, True])
async def test_same_batch_point_fold_memory(reverse: bool) -> None:
    from tank.storage import MemoryRepository

    await test_same_batch_funding_and_ticker_equal_sequential(MemoryRepository(), reverse)


async def test_typed_retention_preserves_major_day_and_prunes_expired(typed_repo: StorageBackend) -> None:
    from datetime import timedelta

    from test_storage import complete_fixture_record

    from tank.util import iso, utcnow

    repo = typed_repo
    await repo.migrate()
    old = utcnow() - timedelta(days=120)
    event = complete_fixture_record(
        "evt.events", {"event_id": "protected-event", "public_at": iso(old), "tokens_mentioned": ["BTC"]}
    )
    await repo.put("evt.events", "protected-event", event)
    ems = complete_fixture_record(
        "evt.ems_assessments",
        {
            "ems_id": "protected-major",
            "event_id": "protected-event",
            "version": 1,
            "class": "STRUCTURAL_SHOCK",
        },
    )
    await repo.put("evt.ems_assessments", "protected-major", ems)
    preserved = flow(iso(old.replace(hour=23)))
    expired = flow(iso(old - timedelta(days=10)))
    await repo.put_market_batch("mkt.flow_bars_1m", [preserved, expired], keys=["protected", "expired"])
    assert any(token == "BTC" for token, _, _ in await repo.preserved_market_windows())
    counts = await repo.prune_market(utcnow() - timedelta(days=90))
    partitions = counts["clickhouse_partitions_dropped"]
    assert isinstance(partitions, dict)
    assert partitions["mkt.flow_bars_1m"] == 1
    assert await repo.get("mkt.flow_bars_1m", "protected") is not None
    assert await repo.get("mkt.flow_bars_1m", "expired") is None


def test_typed_timestamp_alias_validation_and_unknown_policy() -> None:
    from tank.market_codec import decode, encode

    sample: Record = {
        "instrument_id": "x",
        "kind": "SETTLED",
        "as_of": "2026-10-18T00:00:00Z",
        "settlement_time": "2026-10-18T00:00:00Z",
        "settlement_time_prec": "BUCKET(1h)",
        "settlement_time_prec_reason": None,
    }
    result = decode("mkt.funding_obs", encode("mkt.funding_obs", sample))
    assert result["funding_time_prec"] == "BUCKET(1h)" and result["settlement_time_prec"] == "BUCKET(1h)"
    assert result["as_of_prec"] is None and result["as_of_prec_reason"] == "INCOMPLETE"
    with pytest.raises(ValueError, match="Conflicting timestamp precision"):
        encode("mkt.funding_obs", dict(sample, funding_time_prec="EXACT"))
    with pytest.raises(ValueError, match="Unsupported timestamp precision"):
        encode("mkt.funding_obs", dict(sample, settlement_time_prec="INVENTED_EXACT"))
    oi = encode(
        "mkt.oi_obs",
        {
            "ts": sample["as_of"],
            "ts_prec": "BUCKET",
            "timestamp_precision": "BUCKET(5m)",
            "ts_bucket_size_s": 300,
        },
    )
    assert oi["ts_prec"] == "BUCKET(300s)"
    nested = encode("mkt.funding_obs", {"as_of": {"value": sample["as_of"], "precision": "BUCKET(1h)"}})
    assert nested["as_of_prec"] == "BUCKET(1h)"
    assert (
        encode("mkt.funding_obs", dict(sample, as_of_precision="COLLECTION_ONLY"))["as_of_prec"]
        == "COLLECTION_ONLY"
    )


async def exercise_dependency_head(repo: StorageBackend) -> None:
    await repo.migrate()
    source: Record = dict(
        flow("2026-10-18T00:00:00Z"),
        recorded_at="2026-10-18T00:01:00Z",
        boundary_segments=[{"evidence": "x" * 10000}],
    )
    newer: Record = dict(flow("2026-10-18T01:00:00Z"), recorded_at="2026-10-18T01:01:00Z")
    unrelated: Record = dict(
        flow("2026-10-18T00:00:00Z"), instrument_id="unowned:BTC", recorded_at="2026-10-18T04:00:00Z"
    )
    future: Record = dict(flow("2026-10-18T03:00:00Z"), recorded_at="2026-10-18T05:00:00Z")
    await repo.put_market_batch(
        "mkt.flow_bars_1m", [source, newer, unrelated, future], keys=["older", "newer", "unowned", "future"]
    )
    before = present(
        await repo.market_dependency_head(
            "mkt.flow_bars_1m",
            "minute_start",
            "2026-10-18T02:00:00Z",
            start="2026-10-18T00:00:00Z",
            filters={"token_id": "BTC"},
            instrument_ids=["test:BTC", "other:BTC"],
            assembled_at="2026-10-18T06:00:00Z",
        )
    )
    assert before["storage_key"] == "newer"
    correction: Record = dict(source, buy_qty="2", recorded_at="2026-10-18T02:30:00Z")
    await repo.put_market_batch("mkt.flow_bars_1m", [correction], keys=["older"])
    after = present(
        await repo.market_dependency_head(
            "mkt.flow_bars_1m",
            "minute_start",
            "2026-10-18T02:00:00Z",
            start="2026-10-18T00:00:00Z",
            filters={"token_id": "BTC"},
            instrument_ids=["test:BTC", "other:BTC"],
            assembled_at="2026-10-18T06:00:00Z",
        )
    )
    assert after["storage_key"] == "older" and after != before
    assert set(after) == {"storage_key", "recorded_at", "semantic_fingerprint", "minute_start"}
    frozen = await repo.market_dependency_head(
        "mkt.flow_bars_1m",
        "minute_start",
        "2026-10-18T02:00:00Z",
        start="2026-10-18T00:00:00Z",
        filters={"token_id": "BTC"},
        instrument_ids=["test:BTC", "other:BTC"],
        assembled_at="2026-10-18T02:00:00Z",
    )
    assert frozen == before
    assert (
        await repo.market_dependency_head(
            "mkt.flow_bars_1m", "minute_start", "2026-10-18T02:00:00Z", instrument_ids=[]
        )
        is None
    )
    scoped = await repo.market_dependency_head(
        "mkt.flow_bars_1m",
        "minute_start",
        "2026-10-18T02:00:00Z",
        start="2026-10-18T00:30:00Z",
        filters={"token_id": "BTC"},
        instrument_ids=["test:BTC", "other:BTC"],
        assembled_at="2026-10-18T06:00:00Z",
    )
    assert scoped == before
    with pytest.raises(ValueError):
        await repo.market_dependency_head("mkt.flow_bars_1m", "not_a_clock", "2026-10-18T02:00:00Z")


async def test_market_dependency_head_clickhouse(typed_repo: StorageBackend) -> None:
    await exercise_dependency_head(typed_repo)


async def test_market_dependency_head_memory() -> None:
    from tank.storage import MemoryRepository

    await exercise_dependency_head(MemoryRepository())


async def exercise_bounded_latest_market_records(repo: StorageBackend) -> None:
    await repo.migrate()
    rows = [
        dict(flow("2026-10-18T00:00:00Z"), recorded_at="2026-10-18T00:01:00Z"),
        dict(flow("2026-10-18T01:00:00Z"), recorded_at="2026-10-18T01:01:00Z"),
        dict(flow("2026-10-18T01:30:00Z"), recorded_at="2026-10-18T02:01:00Z"),
        dict(flow("2026-10-18T01:45:00Z"), recorded_at="2026-10-18T01:46:00Z", complete=False),
        dict(flow("2026-10-18T03:00:00Z"), recorded_at="2026-10-18T03:01:00Z"),
        dict(
            flow("2026-10-18T01:45:00Z"),
            instrument_id="legacy:BTC",
            recorded_at=None,
            recorded_at_prec=None,
            recorded_at_prec_reason="INCOMPLETE",
        ),
        dict(flow("2026-10-17T01:00:00Z"), instrument_id="expired:BTC", recorded_at="2026-10-17T01:01:00Z"),
    ]
    await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=[str(i) for i in range(len(rows))])
    result = {
        r["instrument_id"]: r
        for r in await repo.latest_market_records(
            "mkt.flow_bars_1m",
            time_field="minute_start",
            group_by=("instrument_id",),
            filters={"complete": True},
            cutoff="2026-10-18T02:00:00Z",
            assembled_at="2026-10-18T01:30:00Z",
            start="2026-10-18T00:00:00Z",
        )
    }
    assert set(result) == {"test:BTC", "legacy:BTC"}
    assert result["test:BTC"]["minute_start"] == "2026-10-18T01:00:00Z"
    assert result["legacy:BTC"]["recorded_at"] is None
    later = {
        r["instrument_id"]: r
        for r in await repo.latest_market_records(
            "mkt.flow_bars_1m",
            time_field="minute_start",
            group_by=("instrument_id",),
            filters={"complete": True},
            cutoff="2026-10-18T02:00:00Z",
            assembled_at="2026-10-18T06:00:00Z",
            start="2026-10-18T00:00:00Z",
        )
    }
    assert later["test:BTC"]["minute_start"] == "2026-10-18T01:30:00Z"
    unbounded = {
        r["instrument_id"]: r
        for r in await repo.latest_market_records(
            "mkt.flow_bars_1m", time_field="minute_start", filters={"complete": True}
        )
    }
    assert unbounded["test:BTC"]["minute_start"] == "2026-10-18T03:00:00Z"
    scoped = await repo.latest_market_records(
        "mkt.flow_bars_1m",
        time_field="minute_start",
        group_by=("instrument_id",),
        filters={"complete": True},
        cutoff="2026-10-18T02:00:00Z",
        assembled_at="2026-10-18T01:30:00Z",
        start="2026-10-18T00:00:00Z",
        instrument_ids=["test:BTC", "test:BTC", "missing"],
    )
    assert scoped == [result["test:BTC"]]
    assert (
        await repo.latest_market_records(
            "mkt.flow_bars_1m",
            time_field="minute_start",
            group_by=("instrument_id",),
            filters={"complete": True},
            cutoff="2026-10-18T02:00:00Z",
            assembled_at="2026-10-18T01:30:00Z",
            start="2026-10-18T00:00:00Z",
            instrument_ids=[],
        )
        == []
    )
    scoped_groups = await repo.latest_market_records(
        "mkt.flow_bars_1m",
        time_field="minute_start",
        group_by=("instrument_id", "token_id"),
        filters={"complete": True},
        cutoff="2026-10-18T02:00:00Z",
        assembled_at="2026-10-18T01:30:00Z",
        start="2026-10-18T00:00:00Z",
        instrument_ids=["test:BTC"],
    )
    assert scoped_groups == scoped
    exact = await repo.latest_market_records(
        "mkt.flow_bars_1m",
        time_field="minute_start",
        group_by=("instrument_id",),
        filters={
            "complete": True,
            "quote_volume": None,
            "buy_qty": "1.00000000000000000001",
            "size_buckets": {},
        },
        cutoff="2026-10-18T02:00:00Z",
        assembled_at="2026-10-18T01:30:00Z",
        start="2026-10-18T00:00:00Z",
        instrument_ids=["test:BTC"],
    )
    assert exact == scoped
    for invalid in ("test:BTC",):
        with pytest.raises(ValueError, match="instrument_ids"):
            await repo.latest_market_records(
                "mkt.flow_bars_1m",
                time_field="minute_start",
                group_by=("instrument_id",),
                filters={"complete": True},
                cutoff="2026-10-18T02:00:00Z",
                assembled_at="2026-10-18T01:30:00Z",
                start="2026-10-18T00:00:00Z",
                instrument_ids=invalid,
            )


async def test_latest_market_records_bounds_clickhouse(typed_repo: StorageBackend) -> None:
    await exercise_bounded_latest_market_records(typed_repo)


async def test_latest_market_records_bounds_memory() -> None:
    from tank.storage import MemoryRepository

    await exercise_bounded_latest_market_records(MemoryRepository())


async def test_dependency_heads_cover_advancing_semantic_boundaries() -> None:
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    rows = [
        dict(flow("2026-10-18T00:00:00Z"), recorded_at="2026-10-18T05:00:00Z"),
        dict(flow("2026-10-18T01:00:00Z"), recorded_at="2026-10-18T01:01:00Z"),
        dict(flow("2026-10-18T02:00:00Z"), recorded_at="2026-10-18T02:01:00Z"),
    ]
    await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=["old-corrected", "middle", "future-eligible"])

    async def head(cutoff: str, order: str, start: str = "2026-10-18T00:00:00Z") -> Record | None:
        return await repo.market_dependency_head(
            "mkt.flow_bars_1m",
            "minute_start",
            cutoff,
            start=start,
            instrument_ids=["test:BTC"],
            assembled_at="2026-10-18T06:00:00Z",
            order=order,
        )

    assert await head("2026-10-18T01:30:00Z", "availability") == await head(
        "2026-10-18T02:30:00Z", "availability"
    )
    assert (present(await head("2026-10-18T01:30:00Z", "semantic_latest")))["storage_key"] == "middle"
    assert (present(await head("2026-10-18T02:30:00Z", "semantic_latest")))[
        "storage_key"
    ] == "future-eligible"
    assert (present(await head("2026-10-18T02:30:00Z", "semantic_earliest")))[
        "storage_key"
    ] == "old-corrected"
    assert (present(await head("2026-10-18T02:30:00Z", "semantic_earliest", start="2026-10-18T00:30:00Z")))[
        "storage_key"
    ] == "middle"
    with pytest.raises(ValueError, match="Unsupported"):
        await head("2026-10-18T02:30:00Z", "arbitrary SQL")


async def exercise_compact_market_projection(repo: StorageBackend) -> None:
    await repo.migrate()

    def candle(
        iid: str, minute: int, close: str = "1.2345678901234567890123456789", **updates: JsonValue
    ) -> Record:
        record: Record = {
            "instrument_id": iid,
            "token_id": "BTC",
            "open_time": f"2026-10-18T00:{minute:02d}:00Z",
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "o": "1",
            "h": "2",
            "l": "1",
            "c": close,
            "volume_native": "3",
            "complete": True,
            "recorded_at": "2026-10-18T00:06:00Z",
            "recorded_at_prec": "COLLECTION_ONLY",
        }
        return record | updates

    rows = [
        candle("A", 0),
        candle("A", 1),
        candle("A", 2, "0"),
        candle("A", 3, complete=False),
        candle("A", 4, "NaN"),
        candle("A", 5, recorded_at="2026-10-18T00:07:00Z"),
        candle("A", 6),
        candle("B", 0, recorded_at=None, recorded_at_prec=None, recorded_at_prec_reason="INCOMPLETE"),
        candle("C", 0),
    ]
    await repo.put_market_batch("mkt.klines_1m", rows, keys=[str(i) for i in range(len(rows))])
    fields = ["instrument_id", "open_time", "c"]
    projection = [
        row
        async for row in repo.iter_market_projection(
            "mkt.klines_1m",
            fields,
            "open_time",
            start="2026-10-18T00:00:00Z",
            end="2026-10-18T00:02:00Z",
            filters={"complete": True},
            instrument_ids=["A"],
            assembled_at="2026-10-18T00:06:00Z",
            batch_size=1,
        )
    ]
    assert len(projection) == 2 and all(set(row) == set(fields) for row in projection)
    assert projection[0]["c"] == "1.2345678901234567890123456789"
    assert [row["open_time"] for row in projection] == ["2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z"]
    counts = await repo.market_series_counts(
        ["A", "B", "missing"], "2026-10-18T00:00:00Z", "2026-10-18T00:06:00Z", "2026-10-18T00:06:00Z"
    )
    assert counts == {"A": 2, "B": 1, "missing": 0}
    assert await repo.market_series_counts([], "2026-10-18T00:00:00Z", "2026-10-18T00:06:00Z") == {}
    assert [
        row
        async for row in repo.iter_market_projection("mkt.klines_1m", fields, "open_time", instrument_ids=[])
    ] == []


async def test_compact_market_projection_clickhouse(typed_repo: StorageBackend) -> None:
    await exercise_compact_market_projection(typed_repo)


async def test_compact_market_projection_memory() -> None:
    from tank.storage import MemoryRepository

    await exercise_compact_market_projection(MemoryRepository())


def test_oi_anchor_is_named_and_scoped_not_generic_extra() -> None:
    from tank.market_codec import decode, encode

    sample: Record = {"ts": "2026-10-18T10:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_anchor": "END"}
    assert decode("mkt.oi_obs", encode("mkt.oi_obs", sample))["ts_bucket_anchor"] == "END"
    with pytest.raises(ValueError, match="Unknown typed market field"):
        encode("mkt.liq_obs", sample)
    with pytest.raises(ValueError, match="anchor"):
        encode("mkt.oi_obs", dict(sample, ts_bucket_anchor="GUESSED"))


async def _unproven_writes_keep_complete_bars(repo: StorageBackend) -> None:
    proven = flow()
    await repo.put_market_batch("mkt.flow_bars_1m", [proven], keys=["bar"])

    for reason in (None, "STREAM_COVERAGE_UNPROVEN"):
        assert (
            await repo.put_market_batch(
                "mkt.flow_bars_1m", [dict(proven, complete=False, gap_reason=reason)], keys=["bar"]
            )
            == []
        )
        assert (present(await repo.get("mkt.flow_bars_1m", "bar")))["complete"] is True

    lost: Record = dict(proven, complete=False, gap_reason="STREAM_DISCONNECTED: keepalive ping timeout")
    assert len(await repo.put_market_batch("mkt.flow_bars_1m", [lost], keys=["bar"])) == 1
    stored = present(await repo.get("mkt.flow_bars_1m", "bar"))
    assert stored["complete"] is False and text_field(stored, "gap_reason").startswith("STREAM_DISCONNECTED")

    assert len(await repo.put_market_batch("mkt.flow_bars_1m", [proven], keys=["bar"])) == 1


async def test_unproven_flow_bar_never_replaces_a_complete_one_memory() -> None:
    from tank.storage import MemoryRepository

    await _unproven_writes_keep_complete_bars(MemoryRepository())


async def test_unproven_flow_bar_never_replaces_a_complete_one_clickhouse(typed_repo: StorageBackend) -> None:
    await typed_repo.migrate()
    await _unproven_writes_keep_complete_bars(typed_repo)


async def test_dependency_heads_in_one_round_trip_equal_single_heads(typed_repo: StorageBackend) -> None:
    from tank.market_storage import market_dependency_head

    await typed_repo.migrate()
    rows = [flow(at) for at in ("2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:02:00Z")]
    rows[1]["recorded_at"] = "2026-10-18T00:05:00Z"
    await typed_repo.put_market_batch("mkt.flow_bars_1m", rows)
    args = ("mkt.flow_bars_1m", "minute_start", "2026-10-18T00:03:00Z")
    orders = ("availability", "semantic_latest", "semantic_earliest")
    batched = await market_dependency_head(
        typed_repo,
        *args,
        start="2026-10-18T00:00:00Z",
        filters={"instrument_id": "test:BTC"},
        assembled_at="2026-10-18T01:00:00Z",
        orders=orders,
    )
    assert batched == {
        o: await market_dependency_head(
            typed_repo,
            *args,
            start="2026-10-18T00:00:00Z",
            filters={"instrument_id": "test:BTC"},
            assembled_at="2026-10-18T01:00:00Z",
            order=o,
        )
        for o in orders
    }
    assert present(batched["availability"])["minute_start"] == "2026-10-18T00:01:00Z"


async def test_batched_range_read_gives_the_loop_turns_while_it_decodes(typed_repo: StorageBackend) -> None:
    import asyncio
    from datetime import timedelta

    from tank.market_storage import market_range_by_instrument
    from tank.util import iso, parse_time

    await typed_repo.migrate()
    base = parse_time("2026-10-18T00:00:00Z")
    rows = [flow(iso(base + timedelta(minutes=n))) for n in range(2500)]
    await typed_repo.put_market_batch("mkt.flow_bars_1m", rows)
    turns = [0]
    done = asyncio.Event()

    async def other_task() -> None:
        while not done.is_set():
            turns[0] += 1
            await asyncio.sleep(0)

    other = asyncio.create_task(other_task())
    before = None
    await asyncio.sleep(0)
    before = turns[0]
    out = await market_range_by_instrument(
        typed_repo,
        "mkt.flow_bars_1m",
        "minute_start",
        ["test:BTC"],
        base,
        base + timedelta(days=3),
        batch_size=5000,
    )
    done.set()
    await other
    assert len(out["test:BTC"]) == 2500
    assert turns[0] - before >= 2


async def test_kline_days_recorded_since_match_memory_on_clickhouse(typed_repo: StorageBackend) -> None:
    from tank.market_storage import market_kline_days_recorded_since
    from tank.storage import MemoryRepository
    from tank.util import parse_time

    await typed_repo.migrate()

    def candle(iid: str, opened: str, recorded_at: str) -> Record:
        return {
            "instrument_id": iid,
            "token_id": "BTC",
            "open_time": opened,
            "open_time_prec": "BUCKET(60s)",
            "interval_s": 60,
            "o": "1",
            "h": "2",
            "l": "1",
            "c": "1",
            "volume_native": "3",
            "complete": True,
            "recorded_at": recorded_at,
            "recorded_at_prec": "COLLECTION_ONLY",
        }

    rows = [
        candle("A", "2026-10-16T23:59:00Z", "2026-10-18T00:10:00Z"),
        candle("A", "2026-10-17T00:00:00Z", "2026-10-18T00:01:00Z"),
        candle("B", "2026-10-17T12:00:00Z", "2026-10-18T00:20:00Z"),
        candle("B", "2026-10-18T00:00:00Z", "2026-10-18T00:20:00Z"),
    ]
    memory = MemoryRepository()
    for repo in (typed_repo, memory):
        await repo.put_market_batch("mkt.klines_1m", rows, keys=[str(i) for i in range(len(rows))])
    args = ("2026-10-18T00:05:00Z", "2026-10-18T00:00:00Z")
    found = await market_kline_days_recorded_since(typed_repo, *args)
    assert found == await market_kline_days_recorded_since(memory, *args)
    assert found == {("A", parse_time("2026-10-16T00:00:00Z")), ("B", parse_time("2026-10-17T00:00:00Z"))}


async def test_sampled_history_decodes_json_rows_off_the_loop_on_clickhouse(
    typed_repo: StorageBackend,
) -> None:
    from tank.market_storage import sampled_market_history
    from tank.storage import MemoryRepository

    await typed_repo.migrate()
    rows = [flow(at) for at in ("2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:06:00Z")]
    memory = MemoryRepository()
    for repo in (typed_repo, memory):
        await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=[str(i) for i in range(len(rows))])
    args: tuple[str, str, list[str], list[str], str, str, int] = (
        "mkt.flow_bars_1m",
        "minute_start",
        [],
        ["test:BTC"],
        "2026-10-18T00:00:00Z",
        "2026-10-18T01:00:00Z",
        300,
    )
    sampled = await sampled_market_history(typed_repo, *args)
    assert [r["minute_start"] for r in sampled] == [
        r["minute_start"] for r in await sampled_market_history(memory, *args)
    ]
    assert [r["minute_start"] for r in sampled] == ["2026-10-18T00:01:00Z", "2026-10-18T00:06:00Z"]
    assert sampled[0]["first_trade"] == {"price": "123.456789012345678901"}


async def test_rows_recorded_since_match_memory_on_clickhouse(typed_repo: StorageBackend) -> None:
    from tank.market_storage import market_rows_recorded_since
    from tank.storage import MemoryRepository

    await typed_repo.migrate()
    rows = [flow(at) for at in ("2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:02:00Z")]
    rows[1]["recorded_at"] = "2026-10-18T00:09:00Z"
    rows[2]["recorded_at"] = "2026-10-18T00:09:30Z"
    memory = MemoryRepository()
    for repo in (typed_repo, memory):
        await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=[str(i) for i in range(len(rows))])
    args = ("mkt.flow_bars_1m", "minute_start", ["test:BTC"], "2026-10-18T00:05:00Z")
    window = ("2026-10-18T00:00:00Z", "2026-10-18T00:02:00Z")
    found = await market_rows_recorded_since(typed_repo, *args, *window)
    expected = await market_rows_recorded_since(memory, *args, *window)
    assert [(i, r["minute_start"], r["first_trade"]) for i, r, _ in found] == [
        (i, r["minute_start"], r["first_trade"]) for i, r, _ in expected
    ]
    assert [r["minute_start"] for _, r, _ in found] == ["2026-10-18T00:01:00Z"]
