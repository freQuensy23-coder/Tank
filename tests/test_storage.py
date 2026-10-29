import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta

import pytest
import pytest_asyncio
from deployment_postgres_helpers import deployment_repository

from tank.storage import ImmutableRecordError, MemoryRepository, PostgresRepository
from tank.util import iso, new_id, parse_time, utcnow


def complete_fixture_record(collection, record):
    """Explicit synthetic §14 fixtures, independent of production defaulting behavior."""
    at = iso()
    common = {
        "config_version": "test-config",
        "code_version": "test-code",
        "model_id": "test-model",
        "prompt_version": "test-prompt",
    }
    templates = {
        "ingest.source_items": {
            "feed": "test:fixture",
            "external_id": record.get("item_id"),
            "published_prec": "COLLECTION_ONLY",
            "observed_at": at,
            "raw_ref": "/test/raw",
            "raw_sha256": "a" * 64,
            "content_hash": "b" * 64,
            "source_class": "PRIMARY",
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
            "source_id": "TEST",
        },
        "evt.events": {
            "statement": "Synthetic storage fixture.",
            "theme_tags": [],
            "public_at": at,
            "public_at_precision": "EXACT",
            "public_at_basis": "SYNTHETIC_FIXTURE",
            "known_to_us_at": at,
            "freshness": "FRESH",
            "event_state": "RUMOR",
            "state_verified": False,
            "certainty": "HIGH",
            "timing_confidence": "HIGH",
            "best_source_class": "PRIMARY",
            "lifecycle": "DETECTED",
        },
        "evt.ems_assessments": dict(
            version=1,
            cutoff=at,
            dims={},
            structural_gates=[],
            criterion="MATERIAL",
            class_reasons=[],
            impact_horizon={},
            **{"class": "LOW"},
            **common,
        ),
        "ana.market_truth": dict(
            token_id="test:T",
            cutoff=at,
            primary_window="W1H",
            move_class="UNKNOWN",
            alternatives=[],
            divergence={},
            flags=[],
            decisive_facts=[],
            data_confidence={"class": "LOW"},
            class_confidence="LOW",
            detail={},
            **common,
        ),
        "ana.decisions": {
            "token_id": "test:T",
            "cutoff": at,
            "thesis_grade": "D",
            "thesis_rule": "D",
            "trade_grade": "D",
            "trade_reasons": ["TRADE_BASE_D"],
            "action": "WAIT",
            "action_rule": "A1",
            "tactical": False,
            "investigate": False,
            "outlook": {},
            "conditions": [],
        },
        "mkt.flow_bars_1m": {
            "buy_qty": "0",
            "sell_qty": "0",
            "buy_usd": record.get("buy_notional", "0"),
            "sell_usd": record.get("sell_notional", "0"),
            "n_buy": 0,
            "n_sell": 0,
            "sum_sq_qty": "0",
            "size_buckets": {},
            "complete": True,
        },
        "mkt.liq_obs": {"side": "LONG_LIQ", "qty_native": "1", "notional_usd": "10", "coverage": "COMPLETE"},
        "out.alerts": dict(
            kind="FULL",
            generated_at=at,
            card_text="Synthetic storage card.",
            card_struct={},
            decisions=[],
            evidence_bundle_id="a" * 64,
            **common,
        ),
        "out.ops_notices": {"text": "Synthetic notice.", "created_at": at, "created_by": "scheduler"},
    }
    defaults = templates.get(collection, {})
    for key, value in defaults.items():
        record.setdefault(key, value)
    return record


@pytest_asyncio.fixture(
    params=["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def repo(request, postgres_template):
    async with AsyncExitStack() as resources:
        if request.param == "postgres_deployment":
            result = await resources.enter_async_context(deployment_repository())
        elif request.param in {"postgres", "postgres_fresh"}:
            url = os.getenv("TANK_TEST_DATABASE_URL")
            if not url:
                pytest.skip(
                    "Real PostgreSQL requires TANK_TEST_DATABASE_URL pointing at an isolated test database"
                )
            fresh = request.param == "postgres_fresh"
            result = await resources.enter_async_context(
                postgres_template.repository(url, migrated=not fresh)
            )
            if fresh:
                await result.migrate()
        else:
            result = MemoryRepository()
            resources.push_async_callback(result.close)
            await result.open()
        original_put = result.put

        async def put_fixture(collection, key, record, **kwargs):
            return await original_put(collection, key, complete_fixture_record(collection, record), **kwargs)

        result.put = put_fixture
        yield result


async def test_nested_transaction_rollback_and_exact_filters(repo):
    async with repo.transaction():
        await repo.put("reg.tokens", "a", {"token_id": "a", "symbol": "A", "null": None})
        try:
            async with repo.transaction():
                await repo.put("reg.tokens", "b", {"token_id": "b"})
                raise ValueError("rollback nested")
        except ValueError:
            pass
        assert await repo.get("reg.tokens", "a")
        assert await repo.get("reg.tokens", "b") is None
    assert len(await repo.list("reg.tokens", filters={"null": None})) == 1
    assert await repo.list("reg.tokens", filters={"missing": None}) == []
    with pytest.raises(ValueError):
        async with repo.transaction():
            await repo.delete("reg.tokens", "a")
            raise ValueError("rollback outer")
    assert await repo.get("reg.tokens", "a")


async def test_append_only_and_immutable(repo):
    await repo.put("evt.events", "alert-fixture-event", {"event_id": "alert-fixture-event"})
    record = {"alert_id": "a", "version": 1, "event_id": "alert-fixture-event", "card_text": "original"}
    await repo.put("out.alerts", "a:1", record)
    assert await repo.put("out.alerts", "a:1", record) == record
    with pytest.raises(ImmutableRecordError):
        await repo.put("out.alerts", "a:1", {**record, "card_text": "changed"})
    with pytest.raises(ImmutableRecordError):
        await repo.delete("out.alerts", "a:1")
    await repo.put("reg.tokens", "sealed", {"token_id": "sealed"}, immutable=True)
    with pytest.raises(ImmutableRecordError):
        await repo.delete("reg.tokens", "sealed")


async def test_queue_namespaced_idempotency_concurrency_priority(repo):
    rows = await asyncio.gather(*[repo.enqueue("FILTER_ITEM", {"item_id": "i"}, "i") for _ in range(10)])
    assert len({r["job_id"] for r in rows}) == 1
    routed = await repo.enqueue("ROUTE_ITEM", {"item_id": "i"}, "i", priority=0)
    assert routed["job_id"] != rows[0]["job_id"]
    claimed = await asyncio.gather(*[repo.claim_job() for _ in range(5)])
    jobs = [j for j in claimed if j]
    assert len(jobs) == 2
    assert len({j["job_id"] for j in jobs}) == 2
    await repo.finish_job(routed["job_id"])
    assert (await repo.enqueue("ROUTE_ITEM", {}, "i"))["status"] == "DONE"


async def test_queue_expiry_retry_recovery(repo):
    expired = await repo.enqueue("X", {}, "expired", expires_at=iso(utcnow() - timedelta(seconds=1)))
    delayed = await repo.enqueue("X", {}, "delayed", run_after=iso(utcnow() + timedelta(hours=1)))
    retry = await repo.enqueue("X", {}, "retry", max_attempts=2)
    assert (await repo.claim_job())["job_id"] == retry["job_id"]
    await repo.fail_job(retry["job_id"], "temporary", retry_after=0)
    assert (await repo.claim_job())["attempts"] == 2
    await repo.recover_jobs()
    assert (await repo.get("ops.jobs", retry["job_id"]))["status"] == "QUEUED"
    recovered = await repo.claim_job()
    assert recovered["attempts"] == 2
    await repo.fail_job(retry["job_id"], "actual final failure")
    assert (await repo.get("ops.jobs", retry["job_id"]))["status"] == "FAILED"
    assert (await repo.get("ops.jobs", expired["job_id"]))["status"] == "EXPIRED"
    assert (await repo.get("ops.jobs", delayed["job_id"]))["status"] == "QUEUED"
    assert await repo.claim_job() is None


async def test_event_lock_serializes_increment(repo):
    await repo.put("evt.events", "event", {"event_id": "event", "counter": 0})

    async def increment():
        async with repo.lock("event:event"):
            row = await repo.get("evt.events", "event")
            await asyncio.sleep(0)
            row["counter"] += 1
            await repo.put("evt.events", "event", row)

    await asyncio.gather(*[increment() for _ in range(10)])
    assert (await repo.get("evt.events", "event"))["counter"] == 10


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_postgres_reopen_keeps_jobs_and_typed_columns(repo):
    if not isinstance(repo, PostgresRepository):
        pytest.skip("PostgreSQL restart/persistence requires the real PostgreSQL repository")
    job = await repo.enqueue("DELIVER", {"alert_id": "a"}, new_id("idem"))
    await repo.claim_job()
    await repo.close()
    await repo.open()
    await repo.recover_jobs()
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "QUEUED"
    async with repo.transaction():
        rows = await (
            await repo._conn().execute(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('reg','ingest','evt','mkt','ana','out','ops','scout','qa')"
            )
        ).fetchall()
    assert len(rows) >= 45


async def test_market_projection_update_and_retention(repo):
    token, instrument = new_id("tok"), new_id("ins")
    await repo.put("reg.instruments", instrument, {"instrument_id": instrument, "token_id": token})
    event = new_id("evt")
    old = utcnow() - timedelta(days=120)
    await repo.put("evt.events", event, {"event_id": event, "public_at": iso(old)})
    ems = new_id("ems")
    await repo.put(
        "evt.ems_assessments",
        ems,
        {"ems_id": ems, "event_id": event, "version": 1, "class": "STRUCTURAL_SHOCK"},
    )
    mta = new_id("mta")
    await repo.put("ana.market_truth", mta, {"mta_id": mta, "token_id": token, "event_id": event})
    key = instrument + ":protected"
    row = {
        "instrument_id": instrument,
        "minute_start": iso(old),
        "buy_qty": "12.3",
        "buy_notional": "24.6",
        "complete": True,
    }
    await repo.put("mkt.flow_bars_1m", key, row)
    await repo.put("mkt.flow_bars_1m", key, {**row, "buy_qty": "13.3"})
    stale_key = instrument + ":stale"
    await repo.put("mkt.flow_bars_1m", stale_key, {**row, "minute_start": iso(old - timedelta(days=10))})
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert (await repo.get("mkt.flow_bars_1m", key))["buy_qty"] == "13.3"
    assert await repo.get("mkt.flow_bars_1m", stale_key) is None
    assert (await repo.get("mkt.flow_bars_1m", key))["buy_usd"] == "24.6"


async def test_indexed_range_pages_equal_timestamps_exclusive_end(repo):
    instrument = new_id("ins")
    at = utcnow()
    for n in range(9):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            {"instrument_id": instrument, "ts": iso(at), "qty_native": str(n)},
        )
    await repo.put(
        "mkt.liq_obs",
        f"{instrument}:later",
        {"instrument_id": instrument, "ts": iso(at + timedelta(seconds=1))},
    )
    rows = [
        row
        async for row in repo.iter_range(
            "mkt.liq_obs",
            "ts",
            start=at,
            end=at + timedelta(seconds=1),
            filters={"instrument_id": instrument},
            batch_size=2,
        )
    ]
    assert len(rows) == 9
    assert len({r["qty_native"] for r in rows}) == 9
    with pytest.raises(ValueError):
        await repo.list_range("mkt.liq_obs", "no_such_field")


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", [pytest.param("postgres_fresh", id="postgres")], indirect=True)
async def test_postgres_migrations_repeat_and_nested_temporal_projection(repo):
    if not isinstance(repo, PostgresRepository):
        pytest.skip("Migration identity and SQL projections require real PostgreSQL")
    await repo.migrate()
    await repo.migrate()
    eid = new_id("evt")
    at = iso()
    record = {"event_id": eid, "event_time": {"value": at, "precision": "EXACT", "quote": "evidence"}}
    await repo.put("evt.events", eid, record)
    assert await repo.get("evt.events", eid) == record
    async with repo.transaction():
        row = await (
            await repo._conn().execute(
                "SELECT event_time,event_time_prec FROM evt.events WHERE storage_key=%s", (eid,)
            )
        ).fetchone()
    assert iso(row["event_time"]) == at
    assert row["event_time_prec"] == "EXACT"


async def test_decision_structured_condition_projection(repo):
    eid, mid, did = new_id("evt"), new_id("mta"), new_id("dec")
    await repo.put("evt.events", eid, {"event_id": eid})
    await repo.put("ana.market_truth", mid, {"mta_id": mid, "event_id": eid})
    conditions = [{"cond_id": "cond-a", "role": "UPGRADE", "expr": {"metric": "dc.class", "value": "HIGH"}}]
    row = {
        "dec_id": did,
        "event_id": eid,
        "mta_id": mid,
        "conditions": conditions,
        "trade_reasons": ["DATA_LOW"],
    }
    await repo.put("ana.decisions", did, row)
    assert (await repo.get("ana.decisions", did))["conditions"] == conditions
    if isinstance(repo, PostgresRepository):
        async with repo.transaction():
            result = await (
                await repo._conn().execute(
                    "SELECT conditions,trade_reasons FROM ana.decisions WHERE storage_key=%s", (did,)
                )
            ).fetchone()
        assert result["conditions"] == ["cond-a"]
        assert result["trade_reasons"] == ["DATA_LOW"]


async def test_expired_source_escalations_follow_job_retention(repo):
    expired, active = new_id("esc"), new_id("esc")
    for key, expires in ((expired, utcnow() - timedelta(days=31)), (active, utcnow() + timedelta(hours=1))):
        await repo.put(
            "ops.source_escalations",
            key,
            {"escalation_id": key, "source_id": "S01", "status": "ACTIVE", "expires_at": iso(expires)},
        )
    await repo.prune_jobs(utcnow() - timedelta(days=30))
    assert await repo.get("ops.source_escalations", expired) is None
    assert (await repo.get("ops.source_escalations", active))["source_id"] == "S01"


async def test_terminal_job_retention_keeps_dependencies_and_pending(repo):
    old = iso(utcnow() - timedelta(days=40))
    removable = await repo.enqueue("RETENTION_TEST", {}, new_id("idem"))
    needed = await repo.enqueue("RETENTION_TEST", {}, new_id("idem"))
    active = await repo.enqueue("RETENTION_TEST", {"depends_on": needed["job_id"]}, new_id("idem"))
    for row in (removable, needed):
        row.update(status="DONE", finished_at=old)
        await repo.put("ops.jobs", row["job_id"], row)
    active["run_after"] = old
    await repo.put("ops.jobs", active["job_id"], active)
    assert await repo.prune_jobs(utcnow() - timedelta(days=30)) == 1
    assert await repo.get("ops.jobs", removable["job_id"]) is None
    assert await repo.get("ops.jobs", needed["job_id"])
    assert (await repo.get("ops.jobs", active["job_id"]))["status"] == "QUEUED"


async def test_preservation_before_market_truth_includes_beneficiary_and_benchmarks(repo):
    eid = new_id("evt")
    now = iso()
    await repo.put("evt.events", eid, {"event_id": eid, "public_at": now, "tokens_mentioned": ["world:WLD"]})
    ems = new_id("ems")
    await repo.put("evt.ems_assessments", ems, {"ems_id": ems, "event_id": eid, "class": "MAJOR"})
    await repo.put(
        "evt.beneficiary_maps", eid + ":1", {"event_id": eid, "map_version": 1, "analysis_set": ["ondo:ONDO"]}
    )
    tokens = {row[0] for row in await repo.preserved_market_windows()}
    assert {"world:WLD", "ondo:ONDO", "bitcoin:BTC", "ethereum:ETH"} <= tokens


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", [pytest.param("postgres_deployment", id="postgres")], indirect=True)
async def test_deployed_agent_role_select_only(repo):
    assert isinstance(repo, PostgresRepository)
    from pathlib import Path

    import psycopg

    async with repo.transaction():
        database = await (await repo._conn().execute("SELECT current_database() AS db")).fetchone()
        assert database["db"] == "tank"
        db = await (await repo._conn().execute("SELECT 1 FROM pg_database WHERE datname='tank'")).fetchone()
        assert db is not None
        script = Path(__file__).resolve().parents[1] / "src" / "tank" / "agent_grants.sql"
        await repo._conn().execute(script.read_text(), prepare=False)
        await repo._conn().execute("GRANT USAGE,CREATE ON SCHEMA reg TO tank")
        await repo._conn().execute("SET LOCAL ROLE tank")
        probe = "role_probe_" + new_id("test").replace("-", "").lower()
        await repo._conn().execute("CREATE TABLE reg." + probe + "(value int)")
        await repo._conn().execute("RESET ROLE")
        await repo._conn().execute("SET LOCAL ROLE tank_agent")
        await repo._conn().execute("SELECT count(*) FROM reg." + probe)
        for table in (
            "reg.tokens",
            "ingest.source_items",
            "evt.events",
            "mkt.supply_obs",
            "ana.decisions",
            "out.alerts",
            "ops.jobs",
            "scout.expansion_proposals",
            "qa.reviews",
        ):
            await repo._conn().execute("SELECT count(*) FROM " + table)
        for statement in (
            "INSERT INTO reg.tokens(storage_key,document,token_id) VALUES ('forbidden','{}','forbidden')",
            "DELETE FROM evt.events WHERE false",
            "UPDATE ops.jobs SET status='DONE' WHERE false",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with repo.transaction():
                    await repo._conn().execute(statement)
        await repo._conn().execute("RESET ROLE")
        await repo._conn().execute("DROP TABLE reg." + probe)


async def test_latest_before_strict_cutoff_and_control_records(repo):
    instrument = new_id("ins")
    at = utcnow()
    for n, seconds in enumerate((-2, -1, 0, 1)):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            {
                "instrument_id": instrument,
                "qty_native": str(n),
                "ts": iso(at + timedelta(seconds=seconds)),
                "price": str(n),
            },
        )
    row = await repo.latest_before("mkt.liq_obs", "ts", at, filters={"instrument_id": instrument})
    assert row["qty_native"] == "1"
    assert (
        await repo.latest_before(
            "mkt.liq_obs", "ts", at - timedelta(seconds=10), filters={"instrument_id": instrument}
        )
        is None
    )
    bid = new_id("backfill")
    await repo.put(
        "mkt.backfill_runs",
        bid,
        {"backfill_id": bid, "status": "PENDING", "anchor": iso(), "result": {"instruments": []}},
    )
    assert (await repo.get("mkt.backfill_runs", bid))["result"] == {"instruments": []}


def test_decisive_fact_projection_requires_canonical_text():
    from tank.storage import _projection

    with pytest.raises(ValueError, match="requires text values"):
        _projection("ana.market_truth", {"decisive_facts": [{"metric": "spot.cvd", "text": "Positive"}]})
    assert _projection("ana.market_truth", {"decisive_facts": ["Positive"]})["decisive_facts"] == ["Positive"]


async def test_uncapped_keyset_and_latest_outside_first_page(repo):
    prefix = new_id("scan")
    at = utcnow()
    for n in range(9):
        key = prefix + ":" + str(n)
        await repo.put(
            "evt.events",
            key,
            {
                "event_id": key,
                "scan": prefix,
                "lifecycle": "TRACKED" if n % 2 == 0 else "CLOSED",
                "known_to_us_at": iso(at + timedelta(seconds=n)),
            },
        )
    all_rows = [r async for r in repo.iter_all("evt.events", filters={"scan": prefix}, batch_size=2)]
    assert len(all_rows) == 9 and len({r["event_id"] for r in all_rows}) == 9
    tracked = [
        r
        async for r in repo.iter_all(
            "evt.events", filters={"scan": prefix, "lifecycle": "TRACKED"}, batch_size=2
        )
    ]
    assert len(tracked) == 5
    latest = await repo.list_latest("evt.events", "known_to_us_at", filters={"scan": prefix}, limit=2)
    assert [r["event_id"] for r in latest] == [prefix + ":8", prefix + ":7"]
    bounded = await repo.list_latest(
        "evt.events", "known_to_us_at", filters={"scan": prefix}, limit=2, end=iso(at + timedelta(seconds=7))
    )
    assert [r["event_id"] for r in bounded] == [prefix + ":6", prefix + ":5"]
    with pytest.raises(ValueError):
        _ = [r async for r in repo.iter_all("evt.events", batch_size=0)]


async def test_normalized_retention_keeps_whole_event_day(repo):
    old = utcnow() - timedelta(days=120)
    event, ems = new_id("evt"), new_id("ems")
    await repo.put(
        "evt.events",
        event,
        {
            "event_id": event,
            "public_at": iso(old),
            "known_to_us_at": iso(old),
            "tokens_mentioned": ["token:major"],
        },
    )
    await repo.put("evt.ems_assessments", ems, {"ems_id": ems, "event_id": event, "class": "MAJOR"})
    expired, preserved, current = new_id("bar"), new_id("bar"), new_id("bar")
    for key, at in [
        (expired, old - timedelta(days=10)),
        (preserved, old.replace(hour=23)),
        (current, utcnow()),
    ]:
        await repo.put(
            "mkt.flow_bars_1m", key, {"instrument_id": "retention:" + key, "minute_start": iso(at)}
        )
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert await repo.get("mkt.flow_bars_1m", expired) is None
    assert await repo.get("mkt.flow_bars_1m", preserved)
    assert await repo.get("mkt.flow_bars_1m", current)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_sql_typed_completeness_and_serials(repo):
    if not isinstance(repo, PostgresRepository):
        pytest.skip("Actual typed SQL constraints require PostgreSQL")
    import psycopg
    from psycopg.types.json import Jsonb

    eid = new_id("evt")
    with pytest.raises(psycopg.errors.CheckViolation):
        async with repo.transaction():
            await repo._conn().execute(
                "INSERT INTO evt.events(storage_key,document,event_id) VALUES (%s,%s,%s)",
                (eid, Jsonb({"event_id": eid}), eid),
            )
    await repo.put("evt.events", eid, {"event_id": eid})
    ids = []
    for _ in range(2):
        key = new_id("state")
        await repo.put(
            "evt.state_changes",
            key,
            {"event_id": eid, "ladder": "EVENT", "to_state": "RUMOR", "evidence": [], "changed_at": iso()},
            immutable=True,
        )
        async with repo.transaction():
            row = await (
                await repo._conn().execute("SELECT id FROM evt.state_changes WHERE storage_key=%s", (key,))
            ).fetchone()
            ids.append(row["id"])
    assert all(i > 0 for i in ids) and len(set(ids)) == 2


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", [pytest.param("postgres_deployment", id="postgres")], indirect=True)
async def test_agent_new_connection_defaults_read_only(repo):
    assert isinstance(repo, PostgresRepository)
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    async with repo.transaction():
        database = await (await repo._conn().execute("SELECT current_database() AS db")).fetchone()
        assert database["db"] == "tank"
        await repo._conn().execute(
            (
                __import__("pathlib").Path(__file__).resolve().parents[1] / "src/tank/agent_grants.sql"
            ).read_text(),
            prepare=False,
        )
    parameters = conninfo_to_dict(repo.database_url)
    parameters["user"] = "tank_agent"
    parameters.pop("password", None)
    connection = await psycopg.AsyncConnection.connect(make_conninfo(**parameters), autocommit=True)
    try:
        row = await (await connection.execute("SHOW transaction_read_only")).fetchone()
        assert row[0] == "on"
        await connection.execute("SELECT count(*) FROM evt.events")
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            await connection.execute("DELETE FROM evt.events WHERE false")
    finally:
        await connection.close()


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.node
@pytest.mark.process
@pytest.mark.parametrize("repo", [pytest.param("postgres_deployment", id="postgres")], indirect=True)
async def test_agent_sandbox_url_resolves_bare_table_names_read_only(repo):
    assert isinstance(repo, PostgresRepository)
    import json
    import re
    import shutil
    import subprocess
    from pathlib import Path

    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node executable required for the actual sandbox database route")
    root = Path(__file__).resolve().parents[1]
    grants = (root / "src/tank/agent_grants.sql").read_text()
    granted = re.findall(r"'(\w+)'", re.search(r"ARRAY\[([^\]]+)\]\s+LOOP", grants).group(1))
    route = subprocess.run(
        [
            node,
            "--input-type=module",
            "-e",
            "import {sandboxDatabaseRoute,replayDatabaseRoute} from './vendor/openma/tank-deploy/openma_entrypoint.mjs';"
            "const replay=await replayDatabaseRoute(()=> 'synthetic',async()=>({address:'172.30.60.2'}));"
            "console.log(JSON.stringify([sandboxDatabaseRoute({},()=> 'synthetic'),replay.url]));",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=3,
    )
    production, replay = (conninfo_to_dict(url) for url in json.loads(route.stdout))
    async with repo.transaction():
        await repo._conn().execute(grants, prepare=False)
    parameters = conninfo_to_dict(repo.database_url)
    parameters["user"] = "tank_agent"
    parameters.pop("password", None)
    parameters["options"] = production.get("options")
    connection = await psycopg.AsyncConnection.connect(make_conninfo(**parameters), autocommit=True)
    try:
        path = (await (await connection.execute("SELECT current_schemas(false)")).fetchone())[0]
        assert set(granted) <= set(path)
        for table in (
            "tokens",
            "source_items",
            "events",
            "claims",
            "event_observations",
            "supply_obs",
            "decisions",
            "alerts",
            "jobs",
            "expansion_proposals",
            "reviews",
        ):
            await connection.execute("SELECT count(*) FROM " + table)
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            await connection.execute("DELETE FROM claims WHERE false")
    finally:
        await connection.close()
    assert replay.get("options") == production["options"]


async def test_late_arrivals_use_availability_not_original_receipt(repo):
    instrument = new_id("late")
    cutoff = utcnow() - timedelta(hours=2)
    arrival = utcnow()
    for n in range(7):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            {
                "instrument_id": instrument,
                "qty_native": str(n),
                "ts": iso(cutoff - timedelta(seconds=n)),
                "received_ts": iso(cutoff - timedelta(hours=1)),
                "recorded_at": iso(arrival),
                "price": "1.234567890123456789",
            },
        )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":future",
        {
            "instrument_id": instrument,
            "qty_native": "10",
            "ts": iso(cutoff + timedelta(microseconds=1)),
            "recorded_at": iso(arrival),
        },
    )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":legacy",
        {"instrument_id": instrument, "qty_native": "11", "ts": iso(cutoff), "received_ts": iso(arrival)},
    )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":end",
        {
            "instrument_id": instrument,
            "qty_native": "12",
            "ts": iso(cutoff),
            "recorded_at": iso(arrival + timedelta(seconds=1)),
        },
    )
    result = [
        r
        async for r in repo.iter_late_arrivals(
            "mkt.liq_obs",
            "recorded_at",
            "ts",
            since=arrival,
            end=arrival + timedelta(seconds=1),
            market_end=cutoff,
            filters={"instrument_id": instrument},
            batch_size=2,
        )
    ]
    assert len(result) == 7 and len({r["qty_native"] for r in result}) == 7
    assert all(parse_time(r["received_ts"]) < cutoff for r in result)
    assert any(parse_time(r["ts"]) == cutoff for r in result)
    assert all(r["price"] == "1.234567890123456789" for r in result)
    retried = [
        r
        async for r in repo.iter_late_arrivals(
            "mkt.liq_obs",
            "recorded_at",
            "ts",
            since=arrival,
            end=arrival + timedelta(seconds=1),
            market_end=cutoff,
            filters={"instrument_id": instrument},
            batch_size=3,
        )
    ]
    assert [r["qty_native"] for r in retried] == [r["qty_native"] for r in result]


async def test_metadata_pages_omit_large_document_payload(repo):
    prefix = new_id("pack")
    for n in range(5):
        await repo.put(
            "out.reaction_checkpoints",
            prefix + str(n),
            {
                "event_id": prefix,
                "token_id": "test:T",
                "revision": n + 1,
                "market_truth": {"large_payload": "x" * 10000},
            },
            immutable=True,
        )
    rows = [
        r
        async for r in repo.iter_metadata(
            "out.reaction_checkpoints",
            ["event_id", "token_id", "revision", "missing"],
            filters={"event_id": prefix},
            batch_size=2,
        )
    ]
    assert len(rows) == 5 and {r["revision"] for r in rows} == set(range(1, 6))
    assert all(
        set(r) == {"event_id", "token_id", "revision", "missing"} and r["missing"] is None for r in rows
    )


async def test_capture_tier_transitions_follow_market_retention(repo):
    old, recent = new_id("tier"), new_id("tier")
    for key, at in ((old, utcnow() - timedelta(days=120)), (recent, utcnow() - timedelta(days=1))):
        await repo.put(
            "ops.capture_tier_transitions",
            key,
            {
                "transition_id": key,
                "token_id": "retention:" + key,
                "effective_at": iso(at),
                "effective_at_prec": "COLLECTION_ONLY",
                "reason": "retention test",
                "config_version": "test",
                "code_version": "test",
                "tier": "BASE",
            },
        )
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert await repo.get("ops.capture_tier_transitions", old) is None
    assert await repo.get("ops.capture_tier_transitions", recent)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_lock_waiters_do_not_drain_the_pool(repo, monkeypatch):
    if not isinstance(repo, PostgresRepository):
        pytest.skip("Pool behavior requires PostgreSQL")
    key = new_id("contended")
    held, release = asyncio.Event(), asyncio.Event()
    ready = asyncio.Event()
    attempted = set()
    waiters = []
    transaction = repo.transaction

    @asynccontextmanager
    async def observed_transaction():
        async with transaction() as current:
            yield current

        task = asyncio.current_task()
        if task in waiters:
            attempted.add(task)
            if len(attempted) == len(waiters):
                ready.set()

    monkeypatch.setattr(repo, "transaction", observed_transaction)

    async def holder():
        async with repo.lock(key):
            held.set()
            await release.wait()

    order = []

    async def waiter(n):
        async with repo.lock(key):
            order.append(n)

    hold = asyncio.create_task(holder())
    try:
        await held.wait()
        waiters = [asyncio.create_task(waiter(n)) for n in range(40)]
        await asyncio.wait_for(ready.wait(), timeout=5)

        assert await asyncio.wait_for(repo.get("reg.tokens", "absent"), timeout=5) is None
        assert not order
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(hold, *waiters), timeout=60)
    assert sorted(order) == list(range(40))


async def test_one_reaction_capture_runs_at_a_time_and_other_jobs_do_not_wait(repo):
    await _clear_queue(repo)
    captures = [
        await repo.enqueue("REACTION_CAPTURE", {"n": n}, new_id("idem"), priority=0) for n in range(3)
    ]
    other = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)
    first = await repo.claim_job()
    assert first["job_id"] == captures[0]["job_id"]

    assert (await repo.claim_job())["job_id"] == other["job_id"]
    assert await repo.claim_job() is None
    await repo.finish_job(first["job_id"])
    assert (await repo.claim_job())["job_id"] == captures[1]["job_id"]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_concurrent_claims_respect_the_capture_cap(repo):
    if not isinstance(repo, PostgresRepository):
        pytest.skip("Concurrent claims require PostgreSQL")
    await _clear_queue(repo)
    for n in range(6):
        await repo.enqueue("REACTION_CAPTURE", {"n": n}, new_id("idem"), priority=0)
    for _ in range(3):
        await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)
    claimed = [job for job in await asyncio.gather(*(repo.claim_job() for _ in range(8))) if job]
    assert sorted(job["type"] for job in claimed) == ["DELIVER"] * 3 + ["REACTION_CAPTURE"]


async def test_due_captures_are_claimed_earliest_expiry_first(repo):
    await _clear_queue(repo)
    now = utcnow()

    slack = await repo.enqueue(
        "REACTION_CAPTURE",
        {},
        new_id("idem"),
        priority=2,
        run_after=iso(now - timedelta(minutes=30)),
        expires_at=iso(now + timedelta(hours=5)),
    )
    urgent = await repo.enqueue(
        "REACTION_CAPTURE",
        {},
        new_id("idem"),
        priority=2,
        run_after=iso(now - timedelta(minutes=5)),
        expires_at=iso(now + timedelta(minutes=20)),
    )
    first = await repo.claim_job()
    assert first["job_id"] == urgent["job_id"]
    await repo.finish_job(first["job_id"])
    assert (await repo.claim_job())["job_id"] == slack["job_id"]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_late_deliver_claim_is_logged_with_type_and_wait(repo, caplog):
    import logging

    if not isinstance(repo, PostgresRepository):
        pytest.skip("The claim timing log is the PostgreSQL claim path")
    await _clear_queue(repo)
    await repo.enqueue(
        "DELIVER", {}, new_id("idem"), priority=0, run_after=iso(utcnow() - timedelta(seconds=7))
    )
    with caplog.at_level(logging.INFO, logger="tank.storage"):
        job = await repo.claim_job()
    assert job["type"] == "DELIVER"
    assert "type DELIVER" in caplog.text and "after run_after" in caplog.text


async def _clear_queue(repo):
    if isinstance(repo, PostgresRepository):
        async with repo.transaction():
            await repo._conn().execute("DELETE FROM ops.jobs WHERE status IN ('QUEUED','RUNNING')")
            await repo._conn().execute(
                "DELETE FROM evt.events WHERE known_to_us_at > now() - interval '1 hour'"
                " AND NOT EXISTS (SELECT 1 FROM out.alerts a WHERE a.event_id = evt.events.event_id)"
            )


async def test_captures_yield_to_event_analysis_unless_near_expiry(repo):
    await _clear_queue(repo)
    now = utcnow()
    truth = await repo.enqueue("RUN_MARKET_TRUTH", {}, new_id("idem"), priority=1)
    assert (await repo.claim_job())["job_id"] == truth["job_id"]
    slack = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(now + timedelta(hours=5))
    )
    deliver = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)

    assert (await repo.claim_job())["job_id"] == deliver["job_id"]
    assert await repo.claim_job() is None
    urgent = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(now + timedelta(minutes=10))
    )
    assert (await repo.claim_job())["job_id"] == urgent["job_id"]
    await repo.finish_job(urgent["job_id"])
    await repo.finish_job(truth["job_id"])
    assert (await repo.claim_job())["job_id"] == slack["job_id"]


async def test_captures_wait_for_a_fresh_event_card(repo):
    await _clear_queue(repo)
    event = new_id("evt")
    await repo.put(
        "evt.events", event, {"event_id": event, "known_to_us_at": iso(utcnow() - timedelta(minutes=2))}
    )
    capture = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(utcnow() + timedelta(hours=5))
    )
    assert await repo.claim_job() is None
    await repo.put(
        "out.alerts",
        f"{event}:1",
        {"alert_id": new_id("alr"), "version": 1, "event_id": event, "kind": "FULL", "generated_at": iso()},
        immutable=True,
    )
    assert (await repo.claim_job())["job_id"] == capture["job_id"]


def _plan_nodes(node):
    yield node
    for child in node.get("Plans", ()):
        yield from _plan_nodes(child)


async def _clone_job(repo, job, copies, types):
    """Insert copies of job under new ids, cycling through types."""
    async with repo.transaction():
        await repo._conn().execute(
            "INSERT INTO ops.jobs SELECT (jsonb_populate_record(j, jsonb_build_object("
            "'storage_key', j.storage_key || '-' || g, 'job_id', j.job_id || '-' || g,"
            " 'idem_key', j.idem_key || '-' || g, 'type', (%(types)s::text[])[g %% %(n)s + 1],"
            " 'document', j.document || jsonb_build_object('job_id', j.job_id || '-' || g,"
            " 'idem_key', j.idem_key || '-' || g, 'type', (%(types)s::text[])[g %% %(n)s + 1])))).*"
            " FROM ops.jobs j, generate_series(1, %(copies)s) g WHERE j.storage_key = %(key)s",
            {"types": types, "n": len(types), "copies": copies, "key": job["job_id"]},
        )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_claim_reads_ops_jobs_through_indexes_once_history_accumulates(repo):
    """A claim poll must not scan the whole job history.

    Production had ~29k finished jobs of mixed types and ~250 deferred captures; the
    analysis gate then read every ops.jobs row on each poll."""
    from tank.storage import CLAIM_SQL, RUNNING_CAPS, claim_params

    await _clear_queue(repo)
    done = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)
    await repo.claim_job()
    await repo.finish_job(done["job_id"])
    await _clone_job(
        repo, done, 10000, ["DELIVER", "EVAL_CONDITIONS", "FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE"]
    )
    truth = await repo.enqueue("RUN_MARKET_TRUTH", {}, new_id("idem"), priority=1)
    assert (await repo.claim_job())["job_id"] == truth["job_id"]
    capture = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(utcnow() + timedelta(hours=5))
    )
    await _clone_job(repo, capture, 250, ["REACTION_CAPTURE"])
    async with repo.transaction():
        conn = repo._conn()
        await conn.execute("ANALYZE ops.jobs")
        explained = await conn.execute("EXPLAIN (FORMAT JSON) " + CLAIM_SQL, claim_params(list(RUNNING_CAPS)))
        plan = (await explained.fetchone())["QUERY PLAN"][0]["Plan"]
    scans = [node for node in _plan_nodes(plan) if node.get("Relation Name") == "jobs"]
    assert len(scans) >= 6
    assert [node["Alias"] for node in scans if node["Node Type"] == "Seq Scan"] == []
    assert await repo.claim_job() is None
    await repo.finish_job(truth["job_id"])
    assert (await repo.claim_job())["type"] == "REACTION_CAPTURE"


async def test_memory_reads_return_isolated_values_and_see_own_writes_before_rollback():
    repo = MemoryRepository()
    original = {"token_id": "a", "nested": {"values": [1]}}
    await repo.put("reg.tokens", "a", original)
    fetched = await repo.get("reg.tokens", "a")
    listed = await repo.list("reg.tokens")
    scanned = [row async for row in repo.iter_all("reg.tokens")]
    fetched["nested"]["values"].append(2)
    listed[0]["nested"]["values"].append(3)
    scanned[0]["nested"]["values"].append(4)
    assert await repo.get("reg.tokens", "a") == original
    with pytest.raises(ValueError, match="rollback"):
        async with repo.transaction():
            await repo.put("reg.tokens", "a", {"token_id": "a", "nested": {"values": [9]}})
            assert (await repo.get("reg.tokens", "a"))["nested"]["values"] == [9]
            assert (await repo.list("reg.tokens"))[0]["nested"]["values"] == [9]
            assert [row["nested"]["values"] async for row in repo.iter_all("reg.tokens")] == [[9]]
            raise ValueError("rollback")
    assert await repo.get("reg.tokens", "a") == original


async def test_memory_cancelled_reader_keeps_writer_exclusive_and_releases_waiter():
    repo = MemoryRepository()
    await repo.put("reg.tokens", "a", {"token_id": "a", "value": "before"})
    held, release, attempted, next_attempted = (asyncio.Event() for _ in range(4))

    async def writer():
        async with repo.transaction():
            await repo.put("reg.tokens", "a", {"token_id": "a", "value": "committed"})
            held.set()
            await release.wait()

    async def read_one():
        attempted.set()
        return await repo.get("reg.tokens", "a")

    async def read_list():
        next_attempted.set()
        return await repo.list("reg.tokens")

    owner = asyncio.create_task(writer())
    readers = []
    try:
        await asyncio.wait_for(held.wait(), 1)
        cancelled = asyncio.create_task(read_one())
        readers.append(cancelled)
        await asyncio.wait_for(attempted.wait(), 1)
        assert not cancelled.done()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        waiting = asyncio.create_task(read_list())
        readers.append(waiting)
        await asyncio.wait_for(next_attempted.wait(), 1)
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(owner, 1)
        assert await asyncio.wait_for(waiting, 1) == [{"token_id": "a", "value": "committed"}]
        assert (await asyncio.wait_for(repo.get("reg.tokens", "a"), 1))["value"] == "committed"
    finally:
        release.set()
        for task in [owner, *readers]:
            if not task.done():
                task.cancel()
        await asyncio.gather(owner, *readers, return_exceptions=True)
