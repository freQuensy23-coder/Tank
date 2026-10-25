import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from deployment_postgres_helpers import deployment_repository
from foundation_helpers import present, text_field
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import InvalidJobClaim
from tank.models import Record
from tank.storage import ImmutableRecordError, MemoryRepository, PostgresRepository, StorageBackend
from tank.util import clock_iso, iso, new_id, parse_time, sha256, utcnow


def nested_values(record: Record) -> list[JsonValue]:
    """Return the actual stored list so isolation tests mutate the returned reference."""
    nested = record["nested"]
    assert isinstance(nested, dict)
    values = nested["values"]
    assert isinstance(values, list)
    return values


def complete_fixture_record(collection: str, record: Record) -> Record:
    """Explicit synthetic §14 fixtures, independent of production defaulting behavior."""
    at = clock_iso()
    common = {
        "config_version": "test-config",
        "code_version": "test-code",
        "model_id": "test-model",
        "prompt_version": "test-prompt",
    }
    templates: dict[str, Record] = {
        "ingest.source_items": {
            "feed": "test:fixture",
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
    if collection not in templates:
        return record
    defaults = templates[collection]
    if collection == "ingest.source_items":
        defaults["external_id"] = text_field(record, "item_id")
    for key, value in defaults.items():
        record.setdefault(key, value)
    return record


@pytest_asyncio.fixture(
    params=["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def repo(
    request: pytest.FixtureRequest, postgres_template: PostgresTemplate
) -> AsyncIterator[StorageBackend]:
    async with AsyncExitStack() as resources:
        result: StorageBackend
        if request.param == "postgres_deployment":
            result = await resources.enter_async_context(deployment_repository())
        elif request.param in {"postgres", "postgres_fresh"}:
            url = os.getenv("TANK_TEST_DATABASE_URL")
            if not url:
                pytest.fail("Integration requires the isolated TANK_TEST_DATABASE_URL")
            fresh = request.param == "postgres_fresh"
            result = await resources.enter_async_context(
                postgres_template.repository(url, migrated=not fresh)
            )
            if fresh:
                await result.migrate()
        elif request.param == "memory":
            result = MemoryRepository()
            resources.push_async_callback(result.close)
            await result.open()
        else:
            raise ValueError("Unknown storage test backend")
        yield result


async def test_nested_transaction_rollback_and_exact_filters(repo: StorageBackend) -> None:
    async with repo.transaction():
        await repo.put(
            "reg.tokens",
            "a",
            complete_fixture_record("reg.tokens", {"token_id": "a", "symbol": "A", "null": None}),
        )
        try:
            async with repo.transaction():
                await repo.put("reg.tokens", "b", complete_fixture_record("reg.tokens", {"token_id": "b"}))
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


async def test_append_only_and_immutable(repo: StorageBackend) -> None:
    await repo.put(
        "evt.events",
        "alert-fixture-event",
        complete_fixture_record("evt.events", {"event_id": "alert-fixture-event"}),
    )
    record: Record = {
        "alert_id": "a",
        "version": 1,
        "event_id": "alert-fixture-event",
        "card_text": "original",
    }
    await repo.put("out.alerts", "a:1", complete_fixture_record("out.alerts", record))
    assert await repo.put("out.alerts", "a:1", complete_fixture_record("out.alerts", record)) == record
    with pytest.raises(ImmutableRecordError):
        await repo.put(
            "out.alerts", "a:1", complete_fixture_record("out.alerts", {**record, "card_text": "changed"})
        )
    with pytest.raises(ImmutableRecordError):
        await repo.delete("out.alerts", "a:1")
    await repo.put(
        "reg.tokens", "sealed", complete_fixture_record("reg.tokens", {"token_id": "sealed"}), immutable=True
    )
    with pytest.raises(ImmutableRecordError):
        await repo.delete("reg.tokens", "sealed")


async def test_queue_namespaced_idempotency_concurrency_priority(repo: StorageBackend) -> None:
    rows = await asyncio.gather(*[repo.enqueue("FILTER_ITEM", {"item_id": "i"}, "i") for _ in range(10)])
    assert len({text_field(present(r), "job_id") for r in rows}) == 1
    routed = await repo.enqueue("ROUTE_ITEM", {"item_id": "i"}, "i", priority=0)
    assert text_field(present(routed), "job_id") != text_field(present(rows[0]), "job_id")
    claimed = await asyncio.gather(*[repo.claim_job() for _ in range(5)])
    jobs = [j for j in claimed if j]
    assert len(jobs) == 2
    assert len({text_field(present(j), "job_id") for j in jobs}) == 2
    await repo.finish_job(text_field(present(routed), "job_id"))
    assert (await repo.enqueue("ROUTE_ITEM", {}, "i"))["status"] == "DONE"


async def test_queue_expiry_retry_recovery(repo: StorageBackend) -> None:
    expired = await repo.enqueue("X", {}, "expired", expires_at=iso(utcnow() - timedelta(seconds=1)))
    delayed = await repo.enqueue("X", {}, "delayed", run_after=iso(utcnow() + timedelta(hours=1)))
    retry = await repo.enqueue("X", {}, "retry", max_attempts=2)
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(retry), "job_id"
    )
    await repo.fail_job(text_field(present(retry), "job_id"), "temporary", retry_after=0)
    assert (present(await repo.claim_job()))["attempts"] == 2
    await repo.recover_jobs()
    assert (present(await repo.get("ops.jobs", text_field(present(retry), "job_id"))))["status"] == "QUEUED"
    recovered = present(await repo.claim_job())
    assert recovered["attempts"] == 2
    await repo.fail_job(text_field(present(retry), "job_id"), "actual final failure")
    assert (present(await repo.get("ops.jobs", text_field(present(retry), "job_id"))))["status"] == "FAILED"
    assert (present(await repo.get("ops.jobs", text_field(present(expired), "job_id"))))[
        "status"
    ] == "EXPIRED"
    assert (present(await repo.get("ops.jobs", text_field(present(delayed), "job_id"))))["status"] == "QUEUED"
    assert await repo.claim_job() is None


async def test_event_lock_serializes_increment(repo: StorageBackend) -> None:
    await repo.put(
        "evt.events", "event", complete_fixture_record("evt.events", {"event_id": "event", "counter": 0})
    )

    async def increment() -> None:
        async with repo.lock("event:event"):
            row = present(await repo.get("evt.events", "event"))
            counter = row["counter"]
            assert isinstance(counter, int)
            await asyncio.sleep(0)
            row["counter"] = counter + 1
            await repo.put("evt.events", "event", complete_fixture_record("evt.events", row))

    await asyncio.gather(*[increment() for _ in range(10)])
    assert (present(await repo.get("evt.events", "event")))["counter"] == 10


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_postgres_reopen_keeps_jobs_and_typed_columns(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    job = await repo.enqueue("DELIVER", {"alert_id": "a"}, new_id("idem"))
    await repo.claim_job()
    await repo.close()
    await repo.open()
    await repo.recover_jobs()
    assert (present(await repo.get("ops.jobs", text_field(present(job), "job_id"))))["status"] == "QUEUED"
    async with repo.transaction():
        rows = await (
            await repo._conn().execute(
                "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('reg','ingest','evt','mkt','ana','out','ops','scout','qa')"
            )
        ).fetchall()
    assert len(rows) >= 45


async def test_market_projection_update_and_retention(repo: StorageBackend) -> None:
    token, instrument = new_id("tok"), new_id("ins")
    await repo.put(
        "reg.instruments",
        instrument,
        complete_fixture_record("reg.instruments", {"instrument_id": instrument, "token_id": token}),
    )
    event = new_id("evt")
    old = utcnow() - timedelta(days=120)
    await repo.put(
        "evt.events", event, complete_fixture_record("evt.events", {"event_id": event, "public_at": iso(old)})
    )
    ems = new_id("ems")
    await repo.put(
        "evt.ems_assessments",
        ems,
        complete_fixture_record(
            "evt.ems_assessments",
            {"ems_id": ems, "event_id": event, "version": 1, "class": "STRUCTURAL_SHOCK"},
        ),
    )
    mta = new_id("mta")
    await repo.put(
        "ana.market_truth",
        mta,
        complete_fixture_record("ana.market_truth", {"mta_id": mta, "token_id": token, "event_id": event}),
    )
    key = instrument + ":protected"
    row: Record = {
        "instrument_id": instrument,
        "minute_start": iso(old),
        "buy_qty": "12.3",
        "buy_notional": "24.6",
        "complete": True,
    }
    await repo.put("mkt.flow_bars_1m", key, complete_fixture_record("mkt.flow_bars_1m", row))
    await repo.put(
        "mkt.flow_bars_1m", key, complete_fixture_record("mkt.flow_bars_1m", {**row, "buy_qty": "13.3"})
    )
    stale_key = instrument + ":stale"
    await repo.put(
        "mkt.flow_bars_1m",
        stale_key,
        complete_fixture_record("mkt.flow_bars_1m", {**row, "minute_start": iso(old - timedelta(days=10))}),
    )
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert (present(await repo.get("mkt.flow_bars_1m", key)))["buy_qty"] == "13.3"
    assert await repo.get("mkt.flow_bars_1m", stale_key) is None
    assert (present(await repo.get("mkt.flow_bars_1m", key)))["buy_usd"] == "24.6"


async def test_indexed_range_pages_equal_timestamps_exclusive_end(repo: StorageBackend) -> None:
    instrument = new_id("ins")
    at = utcnow()
    for n in range(9):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            complete_fixture_record(
                "mkt.liq_obs", {"instrument_id": instrument, "ts": iso(at), "qty_native": str(n)}
            ),
        )
    await repo.put(
        "mkt.liq_obs",
        f"{instrument}:later",
        complete_fixture_record(
            "mkt.liq_obs", {"instrument_id": instrument, "ts": iso(at + timedelta(seconds=1))}
        ),
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
async def test_postgres_migrations_repeat_and_nested_temporal_projection(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    await repo.migrate()
    await repo.migrate()
    eid = new_id("evt")
    at = clock_iso()
    record: Record = {"event_id": eid, "event_time": {"value": at, "precision": "EXACT", "quote": "evidence"}}
    await repo.put("evt.events", eid, complete_fixture_record("evt.events", record))
    assert await repo.get("evt.events", eid) == record
    async with repo.transaction():
        row = present(
            await (
                await repo._conn().execute(
                    "SELECT event_time,event_time_prec FROM evt.events WHERE storage_key=%s", (eid,)
                )
            ).fetchone()
        )
    event_time = row["event_time"]
    assert isinstance(event_time, datetime)
    assert iso(event_time) == at
    assert row["event_time_prec"] == "EXACT"


async def test_decision_structured_condition_projection(repo: StorageBackend) -> None:
    eid, mid, did = new_id("evt"), new_id("mta"), new_id("dec")
    await repo.put("evt.events", eid, complete_fixture_record("evt.events", {"event_id": eid}))
    await repo.put(
        "ana.market_truth", mid, complete_fixture_record("ana.market_truth", {"mta_id": mid, "event_id": eid})
    )
    conditions: list[JsonValue] = [
        {"cond_id": "cond-a", "role": "UPGRADE", "expr": {"metric": "dc.class", "value": "HIGH"}}
    ]
    row: Record = {
        "dec_id": did,
        "event_id": eid,
        "mta_id": mid,
        "conditions": conditions,
        "trade_reasons": ["DATA_LOW"],
    }
    await repo.put("ana.decisions", did, complete_fixture_record("ana.decisions", row))
    assert (present(await repo.get("ana.decisions", did)))["conditions"] == conditions
    if isinstance(repo, PostgresRepository):
        async with repo.transaction():
            result = present(
                await (
                    await repo._conn().execute(
                        "SELECT conditions,trade_reasons FROM ana.decisions WHERE storage_key=%s", (did,)
                    )
                ).fetchone()
            )
        assert result["conditions"] == ["cond-a"]
        assert result["trade_reasons"] == ["DATA_LOW"]


async def test_expired_source_escalations_follow_job_retention(repo: StorageBackend) -> None:
    expired, active = new_id("esc"), new_id("esc")
    for key, expires in ((expired, utcnow() - timedelta(days=31)), (active, utcnow() + timedelta(hours=1))):
        await repo.put(
            "ops.source_escalations",
            key,
            complete_fixture_record(
                "ops.source_escalations",
                {"escalation_id": key, "source_id": "S01", "status": "ACTIVE", "expires_at": iso(expires)},
            ),
        )
    await repo.prune_jobs(utcnow() - timedelta(days=30))
    assert await repo.get("ops.source_escalations", expired) is None
    assert (present(await repo.get("ops.source_escalations", active)))["source_id"] == "S01"


async def test_terminal_job_retention_keeps_dependencies_and_pending(repo: StorageBackend) -> None:
    old = iso(utcnow() - timedelta(days=40))
    removable = await repo.enqueue("RETENTION_TEST", {}, new_id("idem"))
    needed = await repo.enqueue("RETENTION_TEST", {}, new_id("idem"))
    active = await repo.enqueue(
        "RETENTION_TEST", {"depends_on": text_field(present(needed), "job_id")}, new_id("idem")
    )
    for row in (removable, needed):
        row.update(status="DONE", finished_at=old)
        await repo.put(
            "ops.jobs", text_field(present(row), "job_id"), complete_fixture_record("ops.jobs", row)
        )
    active["run_after"] = old
    await repo.put(
        "ops.jobs", text_field(present(active), "job_id"), complete_fixture_record("ops.jobs", active)
    )
    assert await repo.prune_jobs(utcnow() - timedelta(days=30)) == 1
    assert await repo.get("ops.jobs", text_field(present(removable), "job_id")) is None
    assert await repo.get("ops.jobs", text_field(present(needed), "job_id"))
    assert (present(await repo.get("ops.jobs", text_field(present(active), "job_id"))))["status"] == "QUEUED"


async def test_preservation_before_market_truth_includes_beneficiary_and_benchmarks(
    repo: StorageBackend,
) -> None:
    eid = new_id("evt")
    now = clock_iso()
    await repo.put(
        "evt.events",
        eid,
        complete_fixture_record(
            "evt.events", {"event_id": eid, "public_at": now, "tokens_mentioned": ["world:WLD"]}
        ),
    )
    ems = new_id("ems")
    await repo.put(
        "evt.ems_assessments",
        ems,
        complete_fixture_record("evt.ems_assessments", {"ems_id": ems, "event_id": eid, "class": "MAJOR"}),
    )
    await repo.put(
        "evt.beneficiary_maps",
        eid + ":1",
        complete_fixture_record(
            "evt.beneficiary_maps", {"event_id": eid, "map_version": 1, "analysis_set": ["ondo:ONDO"]}
        ),
    )
    tokens = {row[0] for row in await repo.preserved_market_windows()}
    assert {"world:WLD", "ondo:ONDO", "bitcoin:BTC", "ethereum:ETH"} <= tokens


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", [pytest.param("postgres_deployment", id="postgres")], indirect=True)
async def test_deployed_agent_role_select_only(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    from pathlib import Path

    import psycopg

    async with repo.transaction():
        database = present(await (await repo._conn().execute("SELECT current_database() AS db")).fetchone())
        assert database["db"] == "tank"
        db = present(
            await (await repo._conn().execute("SELECT 1 FROM pg_database WHERE datname='tank'")).fetchone()
        )
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


async def test_latest_before_strict_cutoff_and_control_records(repo: StorageBackend) -> None:
    instrument = new_id("ins")
    at = utcnow()
    for n, seconds in enumerate((-2, -1, 0, 1)):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            complete_fixture_record(
                "mkt.liq_obs",
                {
                    "instrument_id": instrument,
                    "qty_native": str(n),
                    "ts": iso(at + timedelta(seconds=seconds)),
                    "price": str(n),
                },
            ),
        )
    row = present(await repo.latest_before("mkt.liq_obs", "ts", at, filters={"instrument_id": instrument}))
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
        complete_fixture_record(
            "mkt.backfill_runs",
            {"backfill_id": bid, "status": "PENDING", "anchor": clock_iso(), "result": {"instruments": []}},
        ),
    )
    assert (present(await repo.get("mkt.backfill_runs", bid)))["result"] == {"instruments": []}


def test_decisive_fact_projection_requires_canonical_text() -> None:
    from tank.storage import _projection

    with pytest.raises(ValueError, match="requires text values"):
        _projection("ana.market_truth", {"decisive_facts": [{"metric": "spot.cvd", "text": "Positive"}]})
    assert _projection("ana.market_truth", {"decisive_facts": ["Positive"]})["decisive_facts"] == ["Positive"]


async def test_uncapped_keyset_and_latest_outside_first_page(repo: StorageBackend) -> None:
    prefix = new_id("scan")
    at = utcnow()
    for n in range(9):
        key = prefix + ":" + str(n)
        await repo.put(
            "evt.events",
            key,
            complete_fixture_record(
                "evt.events",
                {
                    "event_id": key,
                    "scan": prefix,
                    "lifecycle": "TRACKED" if n % 2 == 0 else "CLOSED",
                    "known_to_us_at": iso(at + timedelta(seconds=n)),
                },
            ),
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


async def test_normalized_retention_keeps_whole_event_day(repo: StorageBackend) -> None:
    old = utcnow() - timedelta(days=120)
    event, ems = new_id("evt"), new_id("ems")
    await repo.put(
        "evt.events",
        event,
        complete_fixture_record(
            "evt.events",
            {
                "event_id": event,
                "public_at": iso(old),
                "known_to_us_at": iso(old),
                "tokens_mentioned": ["token:major"],
            },
        ),
    )
    await repo.put(
        "evt.ems_assessments",
        ems,
        complete_fixture_record("evt.ems_assessments", {"ems_id": ems, "event_id": event, "class": "MAJOR"}),
    )
    expired, preserved, current = new_id("bar"), new_id("bar"), new_id("bar")
    for key, at in [
        (expired, old - timedelta(days=10)),
        (preserved, old.replace(hour=23)),
        (current, utcnow()),
    ]:
        await repo.put(
            "mkt.flow_bars_1m",
            key,
            complete_fixture_record(
                "mkt.flow_bars_1m", {"instrument_id": "retention:" + key, "minute_start": iso(at)}
            ),
        )
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert await repo.get("mkt.flow_bars_1m", expired) is None
    assert await repo.get("mkt.flow_bars_1m", preserved)
    assert await repo.get("mkt.flow_bars_1m", current)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_sql_typed_completeness_and_serials(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    import psycopg
    from psycopg.types.json import Jsonb

    eid = new_id("evt")
    with pytest.raises(psycopg.errors.CheckViolation):
        async with repo.transaction():
            await repo._conn().execute(
                "INSERT INTO evt.events(storage_key,document,event_id) VALUES (%s,%s,%s)",
                (eid, Jsonb({"event_id": eid}), eid),
            )
    await repo.put("evt.events", eid, complete_fixture_record("evt.events", {"event_id": eid}))
    ids: list[int] = []
    for _ in range(2):
        key = new_id("state")
        await repo.put(
            "evt.state_changes",
            key,
            complete_fixture_record(
                "evt.state_changes",
                {
                    "event_id": eid,
                    "ladder": "EVENT",
                    "to_state": "RUMOR",
                    "evidence": [],
                    "changed_at": clock_iso(),
                },
            ),
            immutable=True,
        )
        async with repo.transaction():
            row = present(
                await (
                    await repo._conn().execute(
                        "SELECT id FROM evt.state_changes WHERE storage_key=%s", (key,)
                    )
                ).fetchone()
            )
            identifier = row["id"]
            assert isinstance(identifier, int)
            ids.append(identifier)
    assert all(i > 0 for i in ids) and len(set(ids)) == 2


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", [pytest.param("postgres_deployment", id="postgres")], indirect=True)
async def test_agent_new_connection_defaults_read_only(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    async with repo.transaction():
        database = present(await (await repo._conn().execute("SELECT current_database() AS db")).fetchone())
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
    connection = await psycopg.AsyncConnection.connect(
        make_conninfo(**{key: str(value) for key, value in parameters.items()}), autocommit=True
    )
    try:
        row = present(await (await connection.execute("SHOW transaction_read_only")).fetchone())
        assert row is not None and row[0] == "on"
        await connection.execute("SELECT count(*) FROM evt.events")
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            await connection.execute("DELETE FROM evt.events WHERE false")
    finally:
        await connection.close()


async def test_late_arrivals_use_availability_not_original_receipt(repo: StorageBackend) -> None:
    instrument = new_id("late")
    cutoff = utcnow() - timedelta(hours=2)
    arrival = utcnow()
    for n in range(7):
        await repo.put(
            "mkt.liq_obs",
            f"{instrument}:{n}",
            complete_fixture_record(
                "mkt.liq_obs",
                {
                    "instrument_id": instrument,
                    "qty_native": str(n),
                    "ts": iso(cutoff - timedelta(seconds=n)),
                    "received_ts": iso(cutoff - timedelta(hours=1)),
                    "recorded_at": iso(arrival),
                    "price": "1.234567890123456789",
                },
            ),
        )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":future",
        complete_fixture_record(
            "mkt.liq_obs",
            {
                "instrument_id": instrument,
                "qty_native": "10",
                "ts": iso(cutoff + timedelta(microseconds=1)),
                "recorded_at": iso(arrival),
            },
        ),
    )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":legacy",
        complete_fixture_record(
            "mkt.liq_obs",
            {"instrument_id": instrument, "qty_native": "11", "ts": iso(cutoff), "received_ts": iso(arrival)},
        ),
    )
    await repo.put(
        "mkt.liq_obs",
        instrument + ":end",
        complete_fixture_record(
            "mkt.liq_obs",
            {
                "instrument_id": instrument,
                "qty_native": "12",
                "ts": iso(cutoff),
                "recorded_at": iso(arrival + timedelta(seconds=1)),
            },
        ),
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
    assert all(parse_time(text_field(r, "received_ts")) < cutoff for r in result)
    assert any(parse_time(text_field(r, "ts")) == cutoff for r in result)
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


async def test_metadata_pages_omit_large_document_payload(repo: StorageBackend) -> None:
    prefix = new_id("pack")
    for n in range(5):
        await repo.put(
            "out.reaction_checkpoints",
            prefix + str(n),
            complete_fixture_record(
                "out.reaction_checkpoints",
                {
                    "event_id": prefix,
                    "token_id": "test:T",
                    "revision": n + 1,
                    "market_truth": {"large_payload": "x" * 10000},
                },
            ),
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


async def test_capture_tier_transitions_follow_market_retention(repo: StorageBackend) -> None:
    old, recent = new_id("tier"), new_id("tier")
    for key, at in ((old, utcnow() - timedelta(days=120)), (recent, utcnow() - timedelta(days=1))):
        await repo.put(
            "ops.capture_tier_transitions",
            key,
            complete_fixture_record(
                "ops.capture_tier_transitions",
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
            ),
        )
    await repo.prune_market(utcnow() - timedelta(days=90))
    assert await repo.get("ops.capture_tier_transitions", old) is None
    assert await repo.get("ops.capture_tier_transitions", recent)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_lock_waiters_do_not_drain_the_pool(
    repo: StorageBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert isinstance(repo, PostgresRepository)
    key = new_id("contended")
    held, release = asyncio.Event(), asyncio.Event()
    ready = asyncio.Event()
    attempted: set[asyncio.Task[None]] = set()
    waiters: list[asyncio.Task[None]] = []
    transaction = repo.transaction

    @asynccontextmanager
    async def observed_transaction() -> AsyncIterator[PostgresRepository]:
        async with transaction() as current:
            yield current

        task = asyncio.current_task()
        if task in waiters:
            attempted.add(task)
            if len(attempted) == len(waiters):
                ready.set()

    monkeypatch.setattr(repo, "transaction", observed_transaction)

    async def holder() -> None:
        async with repo.lock(key):
            held.set()
            await release.wait()

    order: list[int] = []

    async def waiter(n: int) -> None:
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


async def test_one_reaction_capture_runs_at_a_time_and_other_jobs_do_not_wait(repo: StorageBackend) -> None:
    await _clear_queue(repo)
    captures = [
        await repo.enqueue("REACTION_CAPTURE", {"n": n}, new_id("idem"), priority=0) for n in range(3)
    ]
    other = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)
    first = present(await repo.claim_job())
    assert text_field(present(first), "job_id") == text_field(present(captures[0]), "job_id")

    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(other), "job_id"
    )
    assert await repo.claim_job() is None
    await repo.finish_job(text_field(present(first), "job_id"))
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(captures[1]), "job_id"
    )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_concurrent_claims_respect_the_capture_cap(repo: StorageBackend) -> None:
    assert isinstance(repo, PostgresRepository)
    await _clear_queue(repo)
    for n in range(6):
        await repo.enqueue("REACTION_CAPTURE", {"n": n}, new_id("idem"), priority=0)
    for _ in range(3):
        await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)
    claimed = [job for job in await asyncio.gather(*(repo.claim_job() for _ in range(8))) if job]
    assert sorted(text_field(job, "type") for job in claimed) == ["DELIVER"] * 3 + ["REACTION_CAPTURE"]


async def test_due_captures_are_claimed_earliest_expiry_first(repo: StorageBackend) -> None:
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
    first = present(await repo.claim_job())
    assert text_field(present(first), "job_id") == text_field(present(urgent), "job_id")
    await repo.finish_job(text_field(present(first), "job_id"))
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(slack), "job_id"
    )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
async def test_late_deliver_claim_is_logged_with_type_and_wait(
    repo: StorageBackend, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    assert isinstance(repo, PostgresRepository)
    await _clear_queue(repo)
    await repo.enqueue(
        "DELIVER", {}, new_id("idem"), priority=0, run_after=iso(utcnow() - timedelta(seconds=7))
    )
    with caplog.at_level(logging.INFO, logger="tank.storage"):
        job = present(await repo.claim_job())
    assert job["type"] == "DELIVER"
    assert "type DELIVER" in caplog.text and "after run_after" in caplog.text


async def _clear_queue(repo: StorageBackend) -> None:
    if isinstance(repo, PostgresRepository):
        async with repo.transaction():
            await repo._conn().execute("DELETE FROM ops.jobs WHERE status IN ('QUEUED','RUNNING')")
            await repo._conn().execute(
                "DELETE FROM evt.events WHERE known_to_us_at > now() - interval '1 hour'"
                " AND NOT EXISTS (SELECT 1 FROM out.alerts a WHERE a.event_id = evt.events.event_id)"
            )


async def test_captures_yield_to_event_analysis_unless_near_expiry(repo: StorageBackend) -> None:
    await _clear_queue(repo)
    now = utcnow()
    truth = await repo.enqueue("RUN_MARKET_TRUTH", {}, new_id("idem"), priority=1)
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(truth), "job_id"
    )
    slack = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(now + timedelta(hours=5))
    )
    deliver = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=3)

    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(deliver), "job_id"
    )
    assert await repo.claim_job() is None
    urgent = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(now + timedelta(minutes=10))
    )
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(urgent), "job_id"
    )
    await repo.finish_job(text_field(present(urgent), "job_id"))
    await repo.finish_job(text_field(present(truth), "job_id"))
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(slack), "job_id"
    )


async def test_captures_wait_for_a_fresh_event_card(repo: StorageBackend) -> None:
    await _clear_queue(repo)
    event = new_id("evt")
    await repo.put(
        "evt.events",
        event,
        complete_fixture_record(
            "evt.events", {"event_id": event, "known_to_us_at": iso(utcnow() - timedelta(minutes=2))}
        ),
    )
    capture = await repo.enqueue(
        "REACTION_CAPTURE", {}, new_id("idem"), priority=0, expires_at=iso(utcnow() + timedelta(hours=5))
    )
    assert await repo.claim_job() is None
    await repo.put(
        "out.alerts",
        f"{event}:1",
        complete_fixture_record(
            "out.alerts",
            {
                "alert_id": new_id("alr"),
                "version": 1,
                "event_id": event,
                "kind": "FULL",
                "generated_at": clock_iso(),
            },
        ),
        immutable=True,
    )
    assert text_field(present(present(await repo.claim_job())), "job_id") == text_field(
        present(capture), "job_id"
    )


async def test_memory_reads_return_isolated_values_and_see_own_writes_before_rollback() -> None:
    repo = MemoryRepository()
    original: Record = {"token_id": "a", "nested": {"values": [1]}}
    await repo.put("reg.tokens", "a", complete_fixture_record("reg.tokens", original))
    fetched = present(await repo.get("reg.tokens", "a"))
    listed = await repo.list("reg.tokens")
    scanned = [row async for row in repo.iter_all("reg.tokens")]
    nested_values(fetched).append(2)
    nested_values(listed[0]).append(3)
    nested_values(scanned[0]).append(4)
    assert await repo.get("reg.tokens", "a") == original
    with pytest.raises(ValueError, match="rollback"):
        async with repo.transaction():
            await repo.put(
                "reg.tokens",
                "a",
                complete_fixture_record("reg.tokens", {"token_id": "a", "nested": {"values": [9]}}),
            )
            assert nested_values(present(await repo.get("reg.tokens", "a"))) == [9]
            assert nested_values((await repo.list("reg.tokens"))[0]) == [9]
            assert [nested_values(row) async for row in repo.iter_all("reg.tokens")] == [[9]]
            raise ValueError("rollback")
    assert await repo.get("reg.tokens", "a") == original


async def test_memory_cancelled_reader_keeps_writer_exclusive_and_releases_waiter() -> None:
    repo = MemoryRepository()
    await repo.put(
        "reg.tokens", "a", complete_fixture_record("reg.tokens", {"token_id": "a", "value": "before"})
    )
    held, release, attempted, next_attempted = (asyncio.Event() for _ in range(4))

    async def writer() -> None:
        async with repo.transaction():
            await repo.put(
                "reg.tokens",
                "a",
                complete_fixture_record("reg.tokens", {"token_id": "a", "value": "committed"}),
            )
            held.set()
            await release.wait()

    async def read_one() -> Record | None:
        attempted.set()
        return await repo.get("reg.tokens", "a")

    async def read_list() -> list[Record]:
        next_attempted.set()
        return await repo.list("reg.tokens")

    owner = asyncio.create_task(writer())
    readers: list[asyncio.Task[Record | None] | asyncio.Task[list[Record]]] = []
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
        assert present(await asyncio.wait_for(repo.get("reg.tokens", "a"), 1))["value"] == "committed"
    finally:
        release.set()
        for task in [owner, *readers]:
            if not task.done():
                task.cancel()
        await asyncio.gather(owner, *readers, return_exceptions=True)


@pytest.mark.parametrize("grade", ["MAJOR", "STRUCTURAL", "STRUCTURAL_SHOCK", "LOW"])
async def test_retention_policy_preserves_exact_windows_days_and_bundle_references(
    repo: StorageBackend, grade: str
) -> None:
    anchor = (utcnow() - timedelta(days=120)).replace(hour=12, minute=0, second=0, microsecond=0)
    known = anchor + timedelta(hours=6)
    event_id, ems_id = new_id("evt"), new_id("ems")
    await repo.put(
        "evt.events",
        event_id,
        complete_fixture_record(
            "evt.events",
            {
                "event_id": event_id,
                "event_time": iso(anchor),
                "public_at": iso(anchor),
                "known_to_us_at": iso(known),
                "tokens_mentioned": ["retained:T"],
            },
        ),
    )
    await repo.put(
        "evt.ems_assessments",
        ems_id,
        complete_fixture_record(
            "evt.ems_assessments", {"ems_id": ems_id, "event_id": event_id, "class": grade}
        ),
    )
    start, end = anchor - timedelta(hours=24), known + timedelta(hours=72)
    protected = grade != "LOW"
    windows = [window for window in await repo.preserved_market_windows() if window[0] == "retained:T"]
    assert windows == ([("retained:T", start, end)] if protected else [])
    start_day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = (
        start_day - timedelta(microseconds=1),
        start_day,
        end_day + timedelta(days=1) - timedelta(microseconds=1),
        end_day + timedelta(days=1),
    )
    keys = [new_id("tier") for _ in timestamps]
    for key, at in zip(keys, timestamps, strict=True):
        await repo.put(
            "ops.capture_tier_transitions",
            key,
            complete_fixture_record(
                "ops.capture_tier_transitions",
                {
                    "transition_id": key,
                    "token_id": "retained:T",
                    "effective_at": iso(at),
                    "effective_at_prec": "COLLECTION_ONLY",
                    "reason": "retention boundary fixture",
                    "config_version": "test",
                    "code_version": "test",
                    "tier": "BASE",
                },
            ),
        )
    bundle_id = sha256(event_id)
    bundle: Record = {
        "bundle_id": bundle_id,
        "cutoff": iso(anchor),
        "cutoff_prec": "EXACT",
        "event_id": event_id,
        "capture_transition_refs": list(keys),
    }
    await repo.put("out.evidence_bundles", bundle_id, complete_fixture_record("out.evidence_bundles", bundle))
    counts = await repo.prune_market(utcnow() - timedelta(days=90))
    assert counts["ops.capture_tier_transitions"] == (2 if protected else 4)
    assert await repo.get("ops.capture_tier_transitions", keys[0]) is None
    assert await repo.get("ops.capture_tier_transitions", keys[3]) is None
    assert (await repo.get("ops.capture_tier_transitions", keys[1]) is not None) is protected
    assert (await repo.get("ops.capture_tier_transitions", keys[2]) is not None) is protected
    assert await repo.get("out.evidence_bundles", bundle_id) == bundle


async def test_legacy_wire_job_lifecycle_does_not_fabricate_provenance(repo: StorageBackend) -> None:
    job_id = new_id("job")
    idem_key = new_id("legacy")
    await repo.put(
        "ops.jobs",
        job_id,
        complete_fixture_record(
            "ops.jobs",
            {
                "job_id": job_id,
                "type": "LEGACY",
                "priority": 1,
                "idem_key": idem_key,
                "payload": {},
                "status": "QUEUED",
                "attempts": 0,
                "max_attempts": 3,
                "run_after": iso(utcnow() - timedelta(seconds=1)),
                "expires_at": None,
            },
        ),
    )
    snapshot = await repo.get("ops.jobs", job_id)
    assert snapshot is not None
    assert "run_after_prec" not in snapshot
    assert await repo.enqueue("LEGACY", {"ignored": True}, idem_key) == snapshot
    claimed = await repo.claim_job()
    assert claimed is not None
    assert text_field(present(claimed), "job_id") == job_id
    assert "run_after_prec" not in claimed
    await repo.fail_job(job_id, "retry", retry_after=0)
    retried = await repo.get("ops.jobs", job_id)
    assert retried is not None
    assert retried["status"] == "QUEUED"
    assert retried["run_after_prec"] == "COLLECTION_ONLY"


async def test_invalid_claim_rejection_is_atomic_and_sparse_on_real_backend(repo: StorageBackend) -> None:
    job_id, idem_key = new_id("job"), new_id("invalid")
    await repo.put(
        "ops.jobs",
        job_id,
        complete_fixture_record(
            "ops.jobs",
            {
                "job_id": job_id,
                "type": "LEGACY",
                "priority": 1,
                "idem_key": idem_key,
                "payload": {"retained": "wire"},
                "status": "QUEUED",
                "attempts": 0,
                "max_attempts": 3,
                "run_after": iso(utcnow() - timedelta(seconds=1)),
                "expires_at": None,
            },
        ),
    )
    jobs = JobRepository(repo)
    invalid = await jobs.claim()
    assert isinstance(invalid, InvalidJobClaim)
    assert await jobs.reject_invalid(invalid)
    assert not await jobs.reject_invalid(invalid)
    stored = await repo.get("ops.jobs", job_id)
    assert stored is not None
    assert stored["payload"] == {"retained": "wire"}
    assert stored["status"] == "FAILED"
    assert stored["last_error"] == "INVALID_QUEUE_ENVELOPE"
    assert stored["finished_at_prec"] == "COLLECTION_ONLY"
    assert "run_after_prec" not in stored
    assert await jobs.claim() is None
