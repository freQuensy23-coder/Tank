"""Disposable PostgreSQL proof of §14 typed market storage."""

import os
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest

from tank.storage import PostgresRepository


@pytest.fixture
async def typed_repo():
    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TANK_TEST_DATABASE_URL required; creates only an owned disposable database")
    import psycopg
    from psycopg import sql

    parsed = urlsplit(dsn)
    name = "tank_precision_conformance_" + uuid.uuid4().hex[:12]
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    target = urlunsplit(parsed._replace(path="/" + name))
    repo = await PostgresRepository(target).open()
    try:
        yield repo
    finally:
        await repo.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def flow(at="2026-10-18T00:00:00Z"):
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


async def test_typed_batch_and_negative_constraints(typed_repo):
    import psycopg

    repo = typed_repo
    await repo.migrate()
    row = flow()
    result = await repo.put_market_batch("mkt.flow_bars_1m", [row, row], keys=["legacy", "canonical"])
    assert len(result) == 1
    assert result[0]["buy_qty"] == row["buy_qty"]
    assert result[0]["first_trade"] == row["first_trade"]
    assert await repo.put_market_batch("mkt.flow_bars_1m", [row], keys=["canonical"]) == []
    assert await repo.get("mkt.flow_bars_1m", "canonical") == result[0]
    late = [
        r
        async for r in repo.iter_late_arrivals(
            "mkt.flow_bars_1m",
            "recorded_at",
            "minute_start",
            since="2026-10-18T00:00:00Z",
            end="2026-10-18T00:02:00Z",
            market_end="2026-10-18T00:00:00Z",
            filters={"token_id": "BTC"},
            batch_size=1,
        )
    ]
    assert late == result
    await repo.maintain_partitions()
    assert await repo.get("mkt.flow_bars_1m", "canonical") == result[0]
    assert (
        len(
            [
                r
                async for r in repo.iter_range(
                    "mkt.flow_bars_1m", "minute_start", filters={"token_id": "BTC"}, batch_size=1
                )
            ]
        )
        == 1
    )
    async with repo.transaction():
        cols = await (
            await repo._conn().execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='mkt'"
            )
        ).fetchall()
        assert not {"document", "immutable", "extras"} & {r["column_name"] for r in cols}
        assert (await (await repo._conn().execute("SELECT to_regclass('mkt.trades') AS name")).fetchone())[
            "name"
        ] is None
    with pytest.raises(ValueError, match="identity"):
        await repo.put_market_batch("mkt.flow_bars_1m", [dict(row, instrument_id=None)])
    with pytest.raises(ValueError, match="Unknown typed"):
        await repo.put_market_batch("mkt.flow_bars_1m", [dict(row, unregistered="lost")])
    with pytest.raises(psycopg.errors.NotNullViolation):
        async with repo.transaction():
            await repo._conn().execute(
                "INSERT INTO mkt.flow_bars_1m(storage_key,minute_start) VALUES ('bad',now())"
            )


async def test_ticker_partial_point_merge_and_read_queries(typed_repo):
    repo = typed_repo
    await repo.migrate()
    a = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "ts": "2026-10-18T00:00:00Z",
        "mark_price": "10",
        "field_sources": {"mark_price": {"as_of": "2026-10-18T00:00:00Z", "source_path": "ws"}},
    }
    await repo.put_market_batch("mkt.tickers_24h", [a], keys=["ws"])
    b = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "ts": a["ts"],
        "index_price": "11",
        "field_sources": {"index_price": {"as_of": a["ts"], "source_path": "rest"}},
    }
    rows = await repo.put_market_batch("mkt.tickers_24h", [b], keys=["rest"])
    assert len(rows) == 1 and rows[0]["mark_price"] == "10" and rows[0]["index_price"] == "11"
    assert set(rows[0]["field_sources"]) == {"mark_price", "index_price"}
    assert len(await repo.latest_market_records("mkt.tickers_24h")) == 1
    assert (
        await repo.latest_before("mkt.tickers_24h", "ts", "2026-10-18T00:01:00Z", filters={"token_id": "BTC"})
    )["mark_price"] == "10"


async def test_upgrade_exports_raw_and_preserves_immutable_documents(typed_repo, tmp_path, monkeypatch):
    import asyncio
    import hashlib
    import json

    import pyarrow.parquet as pq
    from click.testing import CliRunner
    from psycopg.types.json import Jsonb

    from tank.cli import main

    repo = typed_repo
    await repo.migrate(target="026_timestamp_precision_metadata.sql")
    old = {
        "instrument_id": "test:BTC",
        "token_id": "BTC",
        "trade_id": "1",
        "ts": "2026-10-18T00:00:01Z",
        "price": "10.123456789012345678901",
        "qty": "1",
    }
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.trades(storage_key,document,immutable,instrument_id,trade_id,token_id,ts) VALUES ('t',%s,true,'test:BTC','1','BTC','2026-10-18T00:00:01Z')",
            [Jsonb(old)],
        )
    with pytest.raises(RuntimeError, match="market_export_dir"):
        await repo.migrate()
    async with repo.transaction():
        assert (
            await (
                await repo._conn().execute("SELECT document FROM mkt.trades WHERE storage_key='t'")
            ).fetchone()
        )["document"] == old
    monkeypatch.setenv("TANK_DATABASE_URL", repo.database_url)
    result = await asyncio.to_thread(
        CliRunner().invoke, main, ["db", "migrate", "--market-export-dir", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    manifest = json.loads(next(tmp_path.glob("legacy-raw-*.json")).read_text())
    assert manifest["row_count"] == 1
    exports = await repo.list("ops.market_migration_exports")
    assert len(exports) == 1 and exports[0]["manifest_path"] == str(next(tmp_path.glob("legacy-raw-*.json")))
    file = manifest["files"][0]
    from pathlib import Path

    assert hashlib.sha256(Path(file["path"]).read_bytes()).hexdigest() == file["sha256"]
    row = pq.read_table(file["path"]).to_pylist()[0]
    assert json.loads(row["legacy_row_json"])["document"] == old
    from tank.market_migration import read_verified_legacy_raw

    assert next(iter(read_verified_legacy_raw(next(tmp_path.glob("legacy-raw-*.json")))))["document"] == old
    broken = dict(manifest, row_count=2)
    with pytest.raises(ValueError, match="total count"):
        list(read_verified_legacy_raw(broken))
    await repo.migrate(market_export_dir=tmp_path)  # idempotent restart


async def test_funding_source_merge_survives_partial_restart(typed_repo):
    repo = typed_repo
    await repo.migrate()
    base = {
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
    ws = dict(base, source_path="ws", received_ts="2026-10-18T00:00:01Z")
    await repo.put_market_batch(
        "mkt.funding_obs", [dict(ws, source_observations={"WEBSOCKET": ws})], keys=["ws"]
    )
    rest = dict(
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
    assert set(result["source_observations"]) == {"WEBSOCKET", "PUBLIC_REST"}
    assert result["source_observations"]["WEBSOCKET"]["received_ts"] == ws["received_ts"]
    assert result["source_disagreement"]["present"] is True


async def test_typed_market_agent_select_only(typed_repo):
    import psycopg

    repo = typed_repo
    await repo.migrate()
    await repo.put_market_batch("mkt.flow_bars_1m", [flow()], keys=["readable"])
    async with repo.transaction():
        role = await (
            await repo._conn().execute("SELECT rolconfig FROM pg_roles WHERE rolname='tank_agent'")
        ).fetchone()
        if role is None:
            pytest.skip("isolated cluster lacks provisioned tank_agent role")
        assert "default_transaction_read_only=on" in role["rolconfig"]
        await repo._conn().execute("GRANT USAGE ON SCHEMA mkt TO tank_agent")
        await repo._conn().execute("GRANT SELECT ON ALL TABLES IN SCHEMA mkt TO tank_agent")
        await repo._conn().execute("SET LOCAL ROLE tank_agent")
        assert (await (await repo._conn().execute("SELECT buy_qty FROM mkt.flow_bars_1m")).fetchone())[
            "buy_qty"
        ] > 0
        for statement in (
            "DELETE FROM mkt.flow_bars_1m",
            "INSERT INTO mkt.flow_bars_1m(storage_key,instrument_id,minute_start) VALUES ('forbidden','x',now())",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with repo._conn().transaction():
                    await repo._conn().execute(statement)
        await repo._conn().execute("RESET ROLE")


async def test_memory_market_batch_uses_same_typed_contract():
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    rows = await repo.put_market_batch("mkt.flow_bars_1m", [flow(), flow()], keys=["a", "b"])
    assert len(rows) == 1
    assert (await repo.get("mkt.flow_bars_1m", "b"))["first_trade"] == flow()["first_trade"]
    assert await repo.put_market_batch("mkt.flow_bars_1m", [flow()], keys=["b"]) == []
    with pytest.raises(ValueError, match="Unknown collection"):
        await repo.get("mkt.trades", "raw")


@pytest.mark.parametrize("reverse", [False, True])
async def test_same_batch_funding_and_ticker_equal_sequential(typed_repo, reverse):
    repo = typed_repo
    await repo.migrate()
    base = {
        "instrument_id": "batch:BTC",
        "token_id": "BTC",
        "kind": "CURRENT_ESTIMATE",
        "as_of": "2026-10-18T00:00:00Z",
        "metric_key": "funding",
        "interval_h": "8",
    }
    rest = dict(
        base,
        rate="0.02",
        rate_8h_eq="0.02",
        capture_source="PUBLIC_REST",
        source_path="rest",
        received_ts="2026-10-18T00:00:01Z",
    )
    ws = dict(
        base,
        rate="0.01",
        rate_8h_eq="0.01",
        capture_source="WEBSOCKET",
        source_path="ws",
        received_ts="2026-10-18T00:00:02Z",
    )
    packets = [
        dict(rest, source_observations={"PUBLIC_REST": rest}),
        dict(ws, source_observations={"WEBSOCKET": ws}),
    ]
    if reverse:
        packets.reverse()
    batched = (await repo.put_market_batch("mkt.funding_obs", packets, keys=["one", "two"]))[0]
    assert batched["rate"] == "0.01"
    assert set(batched["source_observations"]) == {"PUBLIC_REST", "WEBSOCKET"}
    assert batched["source_disagreement"]["present"] is True
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
    a = {
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
    b = {
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
    ticker_packets = [a, b]
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
async def test_same_batch_point_fold_memory(reverse):
    from tank.storage import MemoryRepository

    await test_same_batch_funding_and_ticker_equal_sequential(MemoryRepository(), reverse)


async def test_typed_retention_preserves_major_day_and_prunes_expired(typed_repo):
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
    assert counts["mkt.flow_bars_1m"] == 1
    assert await repo.get("mkt.flow_bars_1m", "protected") is not None
    assert await repo.get("mkt.flow_bars_1m", "expired") is None


def test_typed_timestamp_alias_validation_and_unknown_policy():
    from tank.market_storage import decode, encode

    sample = {
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


async def test_manifest_truncate_and_settlement_companion_upgrade(typed_repo, tmp_path):
    import psycopg
    from psycopg.types.json import Jsonb

    repo = typed_repo
    await repo.migrate(target="026_timestamp_precision_metadata.sql")
    legacy = {
        "instrument_id": "legacy:X",
        "token_id": "X",
        "kind": "SETTLED",
        "rate": "0.01",
        "interval_h": "8",
        "rate_8h_eq": "0.01",
        "as_of": "2026-10-18T00:00:00Z",
        "as_of_prec": "EXACT",
        "metric_key": "funding",
        "settlement_time": "2026-10-18T00:00:00Z",
        "settlement_time_prec": "EXACT",
        "settlement_time_prec_reason": None,
    }
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.funding_obs(storage_key,document,instrument_id,kind,rate,interval_h,rate_8h_eq,as_of,metric_key) VALUES ('legacy',%s,'legacy:X','SETTLED',0.01,8,0.01,'2026-10-18T00:00:00Z','funding')",
            [Jsonb(legacy)],
        )
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.trades(storage_key,document,immutable,instrument_id,trade_id,token_id,ts) VALUES ('legacy-raw',%s,true,'legacy:X','1','X','2026-10-18T00:00:01Z')",
            [Jsonb({"trade_id": "1", "price": "10", "qty": "1", "ts": "2026-10-18T00:00:01Z"})],
        )
    await repo.migrate(market_export_dir=tmp_path, legacy_bundle_dir=tmp_path / "bundles")
    row = await repo.get("mkt.funding_obs", "legacy")
    assert row["funding_time_prec"] == "EXACT" and row["settlement_time_prec"] == "EXACT"
    with pytest.raises(psycopg.errors.RaiseException, match="immutable legacy market evidence"):
        async with repo.transaction():
            await repo._conn().execute("TRUNCATE ops.market_migration_exports")
    assert len(await repo.list("ops.market_migration_exports")) == 2


async def exercise_dependency_head(repo):
    await repo.migrate()
    source = dict(
        flow("2026-10-18T00:00:00Z"),
        recorded_at="2026-10-18T00:01:00Z",
        boundary_segments=[{"evidence": "x" * 10000}],
    )
    newer = dict(flow("2026-10-18T01:00:00Z"), recorded_at="2026-10-18T01:01:00Z")
    unrelated = dict(
        flow("2026-10-18T00:00:00Z"), instrument_id="unowned:BTC", recorded_at="2026-10-18T04:00:00Z"
    )
    future = dict(flow("2026-10-18T03:00:00Z"), recorded_at="2026-10-18T05:00:00Z")
    await repo.put_market_batch(
        "mkt.flow_bars_1m", [source, newer, unrelated, future], keys=["older", "newer", "unowned", "future"]
    )
    options = {
        "start": "2026-10-18T00:00:00Z",
        "filters": {"token_id": "BTC"},
        "instrument_ids": ["test:BTC", "other:BTC"],
        "assembled_at": "2026-10-18T06:00:00Z",
    }
    before = await repo.market_dependency_head(
        "mkt.flow_bars_1m", "minute_start", "2026-10-18T02:00:00Z", **options
    )
    assert before["storage_key"] == "newer"
    correction = dict(source, buy_qty="2", recorded_at="2026-10-18T02:30:00Z")
    await repo.put_market_batch("mkt.flow_bars_1m", [correction], keys=["older"])
    after = await repo.market_dependency_head(
        "mkt.flow_bars_1m", "minute_start", "2026-10-18T02:00:00Z", **options
    )
    assert after["storage_key"] == "older" and after != before
    assert set(after) == {"storage_key", "recorded_at", "semantic_fingerprint", "minute_start"}
    frozen = await repo.market_dependency_head(
        "mkt.flow_bars_1m",
        "minute_start",
        "2026-10-18T02:00:00Z",
        **(options | {"assembled_at": "2026-10-18T02:00:00Z"}),
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
        **(options | {"start": "2026-10-18T00:30:00Z"}),
    )
    assert scoped == before
    with pytest.raises(ValueError):
        await repo.market_dependency_head("mkt.flow_bars_1m", "not_a_clock", "2026-10-18T02:00:00Z")


async def test_market_dependency_head_actual_pg(typed_repo):
    await exercise_dependency_head(typed_repo)


async def test_market_dependency_head_memory():
    from tank.storage import MemoryRepository

    await exercise_dependency_head(MemoryRepository())


async def exercise_bounded_latest_market_records(repo):
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
    options = {
        "time_field": "minute_start",
        "group_by": ("instrument_id",),
        "filters": {"complete": True},
        "cutoff": "2026-10-18T02:00:00Z",
        "assembled_at": "2026-10-18T01:30:00Z",
        "start": "2026-10-18T00:00:00Z",
    }
    result = {r["instrument_id"]: r for r in await repo.latest_market_records("mkt.flow_bars_1m", **options)}
    assert set(result) == {"test:BTC", "legacy:BTC"}
    assert result["test:BTC"]["minute_start"] == "2026-10-18T01:00:00Z"
    assert result["legacy:BTC"]["recorded_at"] is None
    later = {
        r["instrument_id"]: r
        for r in await repo.latest_market_records(
            "mkt.flow_bars_1m", **(options | {"assembled_at": "2026-10-18T06:00:00Z"})
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
        "mkt.flow_bars_1m", **options, instrument_ids=["test:BTC", "test:BTC", "missing"]
    )
    assert scoped == [result["test:BTC"]]
    assert await repo.latest_market_records("mkt.flow_bars_1m", **options, instrument_ids=[]) == []
    scoped_groups = await repo.latest_market_records(
        "mkt.flow_bars_1m",
        **(options | {"group_by": ("instrument_id", "token_id")}),
        instrument_ids=["test:BTC"],
    )
    assert scoped_groups == scoped
    exact = await repo.latest_market_records(
        "mkt.flow_bars_1m",
        **(
            options
            | {
                "filters": {
                    "complete": True,
                    "quote_volume": None,
                    "buy_qty": "1.00000000000000000001",
                    "size_buckets": {},
                }
            }
        ),
        instrument_ids=["test:BTC"],
    )
    assert exact == scoped
    for invalid in ("test:BTC", [123]):
        with pytest.raises(ValueError, match="instrument_ids"):
            await repo.latest_market_records("mkt.flow_bars_1m", **options, instrument_ids=invalid)


async def test_latest_market_records_bounds_actual_pg(typed_repo):
    await exercise_bounded_latest_market_records(typed_repo)


async def test_latest_market_records_bounds_memory():
    from tank.storage import MemoryRepository

    await exercise_bounded_latest_market_records(MemoryRepository())


async def test_dependency_heads_cover_advancing_semantic_boundaries():
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    rows = [
        dict(flow("2026-10-18T00:00:00Z"), recorded_at="2026-10-18T05:00:00Z"),
        dict(flow("2026-10-18T01:00:00Z"), recorded_at="2026-10-18T01:01:00Z"),
        dict(flow("2026-10-18T02:00:00Z"), recorded_at="2026-10-18T02:01:00Z"),
    ]
    await repo.put_market_batch("mkt.flow_bars_1m", rows, keys=["old-corrected", "middle", "future-eligible"])

    async def head(cutoff, order, start="2026-10-18T00:00:00Z"):
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
    assert (await head("2026-10-18T01:30:00Z", "semantic_latest"))["storage_key"] == "middle"
    assert (await head("2026-10-18T02:30:00Z", "semantic_latest"))["storage_key"] == "future-eligible"
    assert (await head("2026-10-18T02:30:00Z", "semantic_earliest"))["storage_key"] == "old-corrected"
    assert (await head("2026-10-18T02:30:00Z", "semantic_earliest", start="2026-10-18T00:30:00Z"))[
        "storage_key"
    ] == "middle"
    with pytest.raises(ValueError, match="Unsupported"):
        await head("2026-10-18T02:30:00Z", "arbitrary SQL")


async def exercise_compact_market_projection(repo, monkeypatch=None):
    await repo.migrate()

    def candle(iid, minute, close="1.2345678901234567890123456789", **updates):
        return {
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
        } | updates

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
    statements = []
    if monkeypatch is not None:
        import psycopg

        original = psycopg.AsyncConnection.execute

        async def traced(conn, query, *args, **kwargs):
            statements.append(query.as_string(conn) if hasattr(query, "as_string") else query)
            return await original(conn, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.AsyncConnection, "execute", traced)
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
    if statements:
        assert all("SELECT *" not in statement.upper() for statement in statements)
        assert all("document" not in statement.lower() for statement in statements)
        select_parts = [statement.upper().split("FROM")[0] for statement in statements]
        assert all("VOLUME_NATIVE" not in part and "RECORDED_AT" not in part for part in select_parts)
        assert repo._connection.get() is None


async def test_compact_market_projection_actual_pg(typed_repo, monkeypatch):
    await exercise_compact_market_projection(typed_repo, monkeypatch)


async def test_compact_market_projection_memory():
    from tank.storage import MemoryRepository

    await exercise_compact_market_projection(MemoryRepository())


async def test_oi_bucket_anchor_nullable_upgrade_and_cutoff(typed_repo):
    import psycopg

    from tank.time_precision import interval_bounds, propagate_timestamp
    from tank.util import iso

    repo = typed_repo
    await repo.migrate(target="027_typed_market.sql")
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.oi_obs(storage_key,instrument_id,token_id,metric_key,ts,ts_prec,ts_bucket_size_s,received_ts,raw_value,raw_unit,sidedness,semantic_fingerprint) VALUES ('legacy','legacy:OI','T','test.oi','2026-10-18T09:50:00Z','BUCKET',300,'2026-10-18T09:50:01Z',100,'BASE','SINGLE','legacy-fingerprint')"
        )
        before = (
            await (
                await repo._conn().execute(
                    "SELECT to_jsonb(row) AS row FROM mkt.oi_obs AS row WHERE storage_key='legacy'"
                )
            ).fetchone()
        )["row"]
    await repo.migrate()
    await repo.migrate()
    async with repo.transaction():
        after = (
            await (
                await repo._conn().execute(
                    "SELECT to_jsonb(row) AS row FROM mkt.oi_obs AS row WHERE storage_key='legacy'"
                )
            ).fetchone()
        )["row"]
    assert after.pop("ts_bucket_anchor") is None and after == before
    legacy_stamp = propagate_timestamp(await repo.get("mkt.oi_obs", "legacy"), "ts")
    assert legacy_stamp.bucket_anchor == "UNSPECIFIED"
    assert interval_bounds(legacy_stamp) == (None, None)
    common = {
        "instrument_id": "new:OI",
        "token_id": "T",
        "metric_key": "test.oi",
        "ts_prec": "BUCKET",
        "ts_bucket_size_s": 300,
        "received_ts": "2026-10-18T10:00:01Z",
        "received_ts_prec": "COLLECTION_ONLY",
        "raw_value": "100",
        "raw_unit": "BASE",
        "sidedness": "SINGLE",
    }
    await repo.put_market_batch(
        "mkt.oi_obs",
        [
            dict(common, ts="2026-10-18T09:55:00Z", ts_bucket_anchor="END"),
            dict(common, ts="2026-10-18T10:00:00Z", ts_bucket_anchor="END"),
        ],
        keys=["prior", "period-end"],
    )
    rows = await repo.latest_market_records(
        "mkt.oi_obs", time_field="ts", filters={"instrument_id": "new:OI"}, cutoff="2026-10-18T09:59:00Z"
    )
    assert rows[0]["ts"] == "2026-10-18T09:55:00Z"
    current = await repo.get("mkt.oi_obs", "period-end")
    assert current["ts"] == "2026-10-18T10:00:00Z" and current["ts_bucket_anchor"] == "END"
    low, high = interval_bounds(propagate_timestamp(current, "ts"))
    assert (iso(low), iso(high)) == ("2026-10-18T09:55:00Z", "2026-10-18T10:00:00Z")
    projection = [
        row
        async for row in repo.iter_market_projection(
            "mkt.oi_obs",
            ["ts", "ts_bucket_anchor"],
            "ts",
            start="2026-10-18T09:55:00Z",
            end="2026-10-18T10:00:00Z",
            filters={"instrument_id": "new:OI"},
        )
    ]
    assert projection == [{"ts": "2026-10-18T09:55:00Z", "ts_bucket_anchor": "END"}]
    await repo.put_market_batch(
        "mkt.oi_obs",
        [dict(common, ts="2026-10-18T10:05:00Z", ts_bucket_anchor="UNSPECIFIED")],
        keys=["unknown-anchor"],
    )
    assert interval_bounds(propagate_timestamp(await repo.get("mkt.oi_obs", "unknown-anchor"), "ts")) == (
        None,
        None,
    )
    await repo.put_market_batch(
        "mkt.oi_obs",
        [dict(common, ts="2026-10-18T10:10:00Z", ts_bucket_anchor="START")],
        keys=["period-start"],
    )
    low, high = interval_bounds(propagate_timestamp(await repo.get("mkt.oi_obs", "period-start"), "ts"))
    assert (iso(low), iso(high)) == ("2026-10-18T10:10:00Z", "2026-10-18T10:15:00Z")
    with pytest.raises(ValueError, match="anchor"):
        await repo.put_market_batch(
            "mkt.oi_obs",
            [dict(common, ts="2026-10-18T10:10:00Z", ts_bucket_anchor="GUESSED")],
            keys=["invalid"],
        )
    with pytest.raises(psycopg.errors.CheckViolation):
        async with repo.transaction():
            await repo._conn().execute(
                "UPDATE mkt.oi_obs SET ts_bucket_anchor='GUESSED' WHERE storage_key='period-end'"
            )


async def test_pre027_oi_import_keeps_future_nullable_anchor_out_of_old_target(typed_repo, tmp_path):
    from psycopg.types.json import Jsonb

    repo = typed_repo
    await repo.migrate(target="026_timestamp_precision_metadata.sql")
    legacy = {
        "instrument_id": "legacy:OI",
        "token_id": "T",
        "metric_key": "test.oi",
        "ts": "2026-10-18T10:00:00Z",
        "ts_prec": "BUCKET",
        "ts_bucket_size_s": 300,
        "received_ts": "2026-10-18T10:00:01Z",
        "raw_value": "100",
        "raw_unit": "BASE",
        "sidedness": "SINGLE",
    }
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO mkt.oi_obs(storage_key,document,immutable,instrument_id,metric_key,ts,ts_prec,received_ts,raw_value,raw_unit,sidedness) VALUES ('legacy',%s,true,'legacy:OI','test.oi','2026-10-18T10:00:00Z','BUCKET','2026-10-18T10:00:01Z',100,'BASE','SINGLE')",
            [Jsonb(legacy)],
        )
    await repo.migrate(target="027_typed_market.sql")
    async with repo.transaction():
        anchor = await (
            await repo._conn().execute(
                "SELECT 1 FROM pg_attribute WHERE attrelid='mkt.oi_obs'::regclass AND attname='ts_bucket_anchor'"
            )
        ).fetchone()
        archived = await (
            await repo._conn().execute(
                "SELECT document,immutable FROM archive.oi_obs WHERE storage_key='legacy'"
            )
        ).fetchone()
    assert anchor is None
    assert archived == {"document": legacy, "immutable": True}
    before = await repo.get("mkt.oi_obs", "legacy")
    assert before["ts_bucket_anchor"] is None
    await repo.migrate(legacy_bundle_dir=tmp_path / "bundles")
    assert await repo.get("mkt.oi_obs", "legacy") == before


def test_oi_anchor_is_named_and_scoped_not_generic_extra():
    from tank.market_storage import decode, encode

    sample = {"ts": "2026-10-18T10:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_anchor": "END"}
    assert decode("mkt.oi_obs", encode("mkt.oi_obs", sample))["ts_bucket_anchor"] == "END"
    with pytest.raises(ValueError, match="Unknown typed market field"):
        encode("mkt.liq_obs", sample)
    with pytest.raises(ValueError, match="anchor"):
        encode("mkt.oi_obs", dict(sample, ts_bucket_anchor="GUESSED"))


async def test_partition_relocation_commits_each_day_and_preserves_failed_day(typed_repo, monkeypatch):
    repo = typed_repo
    await repo.migrate()
    rows = [flow("2020-01-01T00:00:00Z"), flow("2020-01-02T00:00:00Z")]
    for index, row in enumerate(rows):
        await repo.put("mkt.flow_bars_1m", str(index), row)
    before = [await repo.get("mkt.flow_bars_1m", str(index)) for index in range(2)]
    connection = repo._conn

    class FaultAtSecondAttach:
        def __init__(self, conn):
            self.conn = conn

        async def execute(self, query, *args, **kwargs):
            text = query if isinstance(query, str) else query.as_string(self.conn)
            if "ATTACH PARTITION" in text and "flow_bars_1m_20200102" in text:
                raise RuntimeError("Injected partition attachment failure")
            return await self.conn.execute(query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(repo, "_conn", lambda: FaultAtSecondAttach(connection()))
        with pytest.raises(RuntimeError, match="Injected partition"):
            await repo.maintain_partitions()
    assert [await repo.get("mkt.flow_bars_1m", str(index)) for index in range(2)] == before
    async with repo.transaction():
        physical = await (
            await repo._conn().execute(
                "SELECT storage_key,tableoid::regclass::text AS physical FROM mkt.flow_bars_1m ORDER BY storage_key"
            )
        ).fetchall()
    assert [r["physical"] for r in physical] == ["mkt.flow_bars_1m_20200101", "mkt.flow_bars_1m_historical"]
    await repo.maintain_partitions()
    assert [await repo.get("mkt.flow_bars_1m", str(index)) for index in range(2)] == before


async def _unproven_writes_keep_complete_bars(repo):
    proven = flow()
    await repo.put_market_batch("mkt.flow_bars_1m", [proven], keys=["bar"])
    # A writer without proof for the minute (restored bar, second process) cannot unprove it.
    for reason in (None, "STREAM_COVERAGE_UNPROVEN"):
        assert (
            await repo.put_market_batch(
                "mkt.flow_bars_1m", [dict(proven, complete=False, gap_reason=reason)], keys=["bar"]
            )
            == []
        )
        assert (await repo.get("mkt.flow_bars_1m", "bar"))["complete"] is True
    # Evidence of loss still invalidates it.
    lost = dict(proven, complete=False, gap_reason="STREAM_DISCONNECTED: keepalive ping timeout")
    assert len(await repo.put_market_batch("mkt.flow_bars_1m", [lost], keys=["bar"])) == 1
    stored = await repo.get("mkt.flow_bars_1m", "bar")
    assert stored["complete"] is False and stored["gap_reason"].startswith("STREAM_DISCONNECTED")
    # An unproven bar may be replaced by anything, including a proven one.
    assert len(await repo.put_market_batch("mkt.flow_bars_1m", [proven], keys=["bar"])) == 1


async def test_unproven_flow_bar_never_replaces_a_complete_one_memory():
    from tank.storage import MemoryRepository

    await _unproven_writes_keep_complete_bars(MemoryRepository())


async def test_unproven_flow_bar_never_replaces_a_complete_one_postgres(typed_repo):
    await typed_repo.migrate()
    await _unproven_writes_keep_complete_bars(typed_repo)


async def test_dependency_heads_in_one_round_trip_equal_single_heads(typed_repo):
    from tank.market_storage import market_dependency_head

    await typed_repo.migrate()
    rows = [flow(at) for at in ("2026-10-18T00:00:00Z", "2026-10-18T00:01:00Z", "2026-10-18T00:02:00Z")]
    rows[1]["recorded_at"] = "2026-10-18T00:05:00Z"
    await typed_repo.put_market_batch("mkt.flow_bars_1m", rows)
    args = ("mkt.flow_bars_1m", "minute_start", "2026-10-18T00:03:00Z")
    kwargs = {
        "start": "2026-10-18T00:00:00Z",
        "filters": {"instrument_id": "test:BTC"},
        "assembled_at": "2026-10-18T01:00:00Z",
    }
    orders = ("availability", "semantic_latest", "semantic_earliest")
    batched = await market_dependency_head(typed_repo, *args, **kwargs, orders=orders)
    assert batched == {o: await market_dependency_head(typed_repo, *args, **kwargs, order=o) for o in orders}
    assert batched["availability"]["minute_start"] == "2026-10-18T00:01:00Z"


async def test_batched_range_read_gives_the_loop_turns_while_it_decodes(typed_repo):
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

    async def other_task():
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
    assert turns[0] - before >= 2  # yields every YIELD_ROWS rows, not only between pages


async def test_book_pages_never_fetch_levels_outside_the_walked_rows(typed_repo):
    from datetime import timedelta

    from tank.market_storage import market_range_by_instrument
    from tank.util import iso, parse_time

    await typed_repo.migrate()
    base = parse_time("2026-10-18T00:00:00Z")
    levels = [["2", "1"], ["3", "1"]]
    rows = [
        {
            "instrument_id": "test:BTC",
            "token_id": "BTC",
            "exchange_ts": iso(base + timedelta(seconds=10 * n)),
            "integrity_ok": n == 0,
            "mid": "2",
            "bids": levels,
            "asks": levels,
            "recorded_at": iso(base + timedelta(seconds=10 * n + 1)),
            "recorded_at_prec": "COLLECTION_ONLY",
        }
        for n in range(5)
    ]
    await typed_repo.put_market_batch("mkt.book_summaries", rows)
    queries = []
    real_conn = typed_repo._conn

    class Recording:
        def __init__(self, conn):
            self.conn = conn

        async def execute(self, query, *args, **kwargs):
            queries.append(query.as_string(self.conn) if hasattr(query, "as_string") else str(query))
            return await self.conn.execute(query, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.conn, name)

    typed_repo._conn = lambda: Recording(real_conn())
    try:
        out = await market_range_by_instrument(
            typed_repo,
            "mkt.book_summaries",
            "exchange_ts",
            ["test:BTC"],
            base,
            base + timedelta(minutes=5),
            batch_size=2,
            level_rows=2,
        )
    finally:
        typed_repo._conn = real_conn
    got = out["test:BTC"]
    assert [("bids" in r) for r in got] == [True, False, False, True, True]  # newest 2 + newest checked
    assert got[0]["bids"] == levels and got[4]["asks"] == levels
    pages = [q for q in queries if "LIMIT" in q]
    assert len(pages) == 3 and not any('"bids"' in q for q in pages)  # levels never decoded per page
