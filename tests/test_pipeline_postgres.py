"""Routing concurrency with real transaction locks and no external calls."""

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Literal

import httpx
import pytest
from agent_source_fixtures import (
    declared_versions_fixture,
    pipeline_settings_fixture,
    source_item_fixture,
    source_record_fixture,
)
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.agents.jev import JevState
from tank.agents.pipeline import Pipeline
from tank.agents.pipeline_contracts import ClassificationName, JevScoreRecord, RoutingCandidate
from tank.contracts.repository import Collection
from tank.contracts.source_items import FeedSourceItem
from tank.ops_progress import DisabledSourceProgress
from tank.storage import PostgresRepository
from tank.util import clock_iso, iso, new_id

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def isolated_routing_repo(postgres_template: PostgresTemplate) -> AsyncIterator[PostgresRepository]:
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires an explicitly configured PostgreSQL test connection")
    async with postgres_template.repository(url) as repo:
        yield repo


@pytest.mark.asyncio
async def test_independent_routing_calls_overlap_preserve_source_updates_and_deduplicate(
    isolated_routing_repo: PostgresRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = isolated_routing_repo
    marker, at = new_id("ROUTING-TEST"), clock_iso()
    entity = marker + "-entity"
    config: dict[str, dict[str, JsonValue]] = {
        "registry": {"entities": [{"entity_id": entity, "aliases": [marker]}]},
        "thresholds": {"routing_cutoff": 0.8, "routing_window_days": 3},
    }
    opts = pipeline_settings_fixture(config)
    event = marker + "-event"
    await repo.put(
        "evt.events",
        event,
        {
            "event_id": event,
            "statement": marker,
            "entities": list[JsonValue]([entity]),
            "theme_tags": [],
            "public_at": at,
            "public_at_precision": "EXACT",
            "public_at_basis": "TEST",
            "known_to_us_at": at,
            "freshness": "FRESH",
            "event_state": "RUMOR",
            "state_verified": False,
            "certainty": "HIGH",
            "timing_confidence": "HIGH",
            "best_source_class": "PRIMARY",
            "lifecycle": "ANALYZING",
            "tokens_mentioned": [],
        },
    )
    identifiers = [
        marker + suffix for suffix in ("-first", "-second", "-duplicate", "-unrelated", "-new-duplicate")
    ]
    for item_id in identifiers:
        await repo.put(
            "ingest.source_items",
            item_id,
            source_record_fixture(
                {
                    "item_id": item_id,
                    "source_id": "S01",
                    "feed": "test",
                    "external_id": item_id,
                    "text": marker if item_id in identifiers[:3] else "Unrelated no matching identity",
                    "observed_at": at,
                    "published_prec": "COLLECTION_ONLY",
                    "raw_ref": "/test/raw",
                    "raw_sha256": "a" * 64,
                    "content_hash": "b" * 64,
                    "source_class": "PRIMARY",
                    "relevance": {"decision": "ALWAYS"},
                    "extraction_status": "PENDING",
                }
            ),
        )
    session = marker + "-session"
    await repo.put(
        "ops.agent_sessions",
        session,
        {
            "session_id": session,
            "event_id": event,
            "opened_by_item": identifiers[0],
            "agent_version": "test",
            "status": "OPEN",
            "opened_at": at,
            "last_item_at": at,
            "last_event_seq": 0,
        },
    )
    pipe = Pipeline(repo, opts, progress=DisabledSourceProgress(), versions=declared_versions_fixture())
    arrived, release, both_blocked = set(), asyncio.Event(), asyncio.Event()

    async def score(item: FeedSourceItem, name: ClassificationName, state: JevState) -> float:
        assert state.event is not None and state.event.statement == marker
        item_id = item.item_id
        assert isinstance(item_id, str)
        arrived.add(item_id)
        if len(arrived) == 2:
            both_blocked.set()
        await release.wait()
        return 0.95

    monkeypatch.setattr(pipe, "score", score)
    try:
        async with asyncio.timeout(5):
            tasks = [asyncio.create_task(pipe.route_item_wire({"item_id": i})) for i in identifiers[:2]]
            try:
                await both_blocked.wait()
                async with repo.lock("ingest-dedup"):
                    latest = await repo.get("ingest.source_items", identifiers[0])
                    assert latest is not None
                    await repo.put(
                        "ingest.source_items",
                        identifiers[0],
                        source_record_fixture({**latest, "extraction_reason": "preserved"}),
                    )

                unrelated = await pipe.route_item_wire({"item_id": identifiers[3]})
                assert unrelated["routing_message"] == {"kind": "NEW_ITEM", "item_id": identifiers[3]}
                new_one, new_two = await asyncio.gather(
                    *(pipe.route_item_wire({"item_id": identifiers[4]}) for _ in range(2))
                )
                assert new_one == new_two
                new_route = new_one["route"]
                assert isinstance(new_route, dict)
                assert "reservation_id" not in new_route
                release.set()
                first, second = await asyncio.gather(*tasks)
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)
        assert first["route_event_id"] == second["route_event_id"] == event
        assert first["extraction_reason"] == "preserved"
        score_calls = 0

        async def duplicate_score(item: FeedSourceItem, name: ClassificationName, state: JevState) -> float:
            nonlocal score_calls
            score_calls += 1
            await asyncio.sleep(0.02)
            return 0.95

        monkeypatch.setattr(pipe, "score", duplicate_score)
        one, two = await asyncio.gather(
            *(pipe.route_item_wire({"item_id": identifiers[2]}) for _ in range(2))
        )
        assert one == two and score_calls == 1
        jobs = await repo.list("ops.jobs", filters={"idem_key": identifiers[2] + ":ITEM_ADDED"})
        assert len(jobs) == 1
        new_jobs = await repo.list("ops.jobs", filters={"idem_key": identifiers[4] + ":NEW_ITEM"})
        assert len(new_jobs) == 1
        for row in (first, second, one, two):
            message = row["routing_message"]
            assert isinstance(message, dict)
            assert "reservation_id" not in message
    finally:
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["failure", "cancellation"])
async def test_parallel_candidate_scores_survive_retry_without_partial_winner(
    interruption: Literal["failure", "cancellation"],
    isolated_routing_repo: PostgresRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    repo = isolated_routing_repo
    marker, at = new_id("ROUTE-PAIRS"), clock_iso()
    item_id = marker + "-item"
    await repo.put(
        "ingest.source_items",
        item_id,
        source_record_fixture(
            {
                "item_id": item_id,
                "source_id": "S01",
                "feed": "test",
                "external_id": item_id,
                "text": marker,
                "observed_at": at,
                "published_prec": "COLLECTION_ONLY",
                "raw_ref": "/test/raw",
                "raw_sha256": "a" * 64,
                "content_hash": "b" * 64,
                "source_class": "PRIMARY",
                "relevance": {"decision": "ALWAYS"},
                "extraction_status": "PENDING",
            }
        ),
    )
    config: dict[str, dict[str, JsonValue]] = {"registry": {}, "thresholds": {"routing_cutoff": 0.8}}
    opts = pipeline_settings_fixture(config)
    candidates = [
        RoutingCandidate(
            event_id=marker + "-" + key,
            statement=key,
            lifecycle="ANALYZING",
            entities=[],
            tokens_mentioned=[],
            event_state="RUMOR",
        )
        for key in ("a", "b", "c")
    ]
    arrived: set[str] = set()
    calls: dict[str, int] = {}
    ready = asyncio.Event()
    retry = False

    async def transport(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        key = body["state"]["event"]["statement"]
        calls[key] = calls.get(key, 0) + 1
        assert list(body["questions"]) == ["same_event"]
        arrived.add(key)
        if len(arrived) == 3:
            ready.set()
        await ready.wait()
        if key == "b" and not retry:
            if interruption == "failure":
                return httpx.Response(503, json={"error": "synthetic failure"})
            await asyncio.Event().wait()
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "usage": {"input_tokens": 10},
                "answers": {"same_event": {"type": "noul", "noul": 0.9 if key == "c" else 0.95}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        pipe = Pipeline(
            repo, opts, progress=DisabledSourceProgress(), http=http, versions=declared_versions_fixture()
        )

        async def fixed_candidates(identities: set[str], since: str, now: datetime) -> list[RoutingCandidate]:
            return candidates

        monkeypatch.setattr(pipe, "_candidate_events", fixed_candidates)
        try:
            attempt = asyncio.create_task(pipe.route_item_wire({"item_id": item_id}))
            async with asyncio.timeout(5):
                await ready.wait()
                if interruption == "cancellation":
                    while len(await repo.list("ops.llm_usage", filters={"item_id": item_id})) < 2:
                        await asyncio.sleep(0.01)
                    attempt.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await attempt
                else:
                    with pytest.raises(httpx.HTTPStatusError):
                        await attempt
            current = await repo.get("ingest.source_items", item_id)
            assert current is not None and "routing_message" not in current
            assert not await repo.list("ops.jobs", filters={"idem_key": item_id + ":ITEM_ADDED"})
            assert len(await repo.list("ops.llm_usage", filters={"item_id": item_id})) == 2
            retry = True
            async with asyncio.timeout(5):
                result, duplicate = await asyncio.gather(
                    *(pipe.route_item_wire({"item_id": item_id}) for _ in range(2))
                )
            assert result == duplicate and result["route_event_id"] == marker + "-a"
            routing_scores = result["routing_scores"]
            assert isinstance(routing_scores, dict)
            assert list(routing_scores) == [event.event_id for event in candidates]
            assert calls == {"a": 1, "b": 2, "c": 1}
            assert len(await repo.list("ops.jobs", filters={"idem_key": item_id + ":ITEM_ADDED"})) == 1
        finally:
            await repo.close()


@pytest.mark.asyncio
async def test_candidate_window_equal_batch_clock_is_inclusive_without_future_rows(
    isolated_routing_repo: PostgresRepository,
) -> None:
    from datetime import timedelta

    from tank.util import utcnow

    repo = isolated_routing_repo
    marker, now = new_id("ROUTE-BOUNDARY"), utcnow()
    since = now - timedelta(days=3)
    try:
        for suffix, at in (
            ("z_equal", now),
            ("a_lower", since),
            ("future", now + timedelta(microseconds=1)),
            ("old", since - timedelta(microseconds=1)),
        ):
            key = marker + suffix
            await repo.put(
                "ingest.source_items",
                key,
                source_record_fixture(
                    {
                        "item_id": key,
                        "source_id": "S01",
                        "feed": "test",
                        "external_id": key,
                        "text": marker,
                        "observed_at": iso(at),
                        "published_prec": "COLLECTION_ONLY",
                        "raw_ref": "/test/raw",
                        "raw_sha256": "a" * 64,
                        "content_hash": "b" * 64,
                        "source_class": "PRIMARY",
                        "relevance": {"decision": "ALWAYS"},
                        "extraction_status": "PENDING",
                    }
                ),
            )
            await repo.put(
                "evt.events",
                key,
                {
                    "event_id": key,
                    "statement": key,
                    "entities": list[JsonValue]([marker]),
                    "theme_tags": [],
                    "public_at": iso(at),
                    "public_at_precision": "EXACT",
                    "public_at_basis": "TEST",
                    "known_to_us_at": iso(at),
                    "freshness": "FRESH",
                    "event_state": "RUMOR",
                    "state_verified": False,
                    "certainty": "HIGH",
                    "timing_confidence": "HIGH",
                    "best_source_class": "PRIMARY",
                    "lifecycle": "ANALYZING",
                    "tokens_mentioned": [],
                },
            )
            await repo.put(
                "ops.agent_sessions",
                key,
                {
                    "session_id": key,
                    "event_id": key,
                    "opened_by_item": key,
                    "agent_version": "test",
                    "status": "OPEN",
                    "opened_at": iso(at),
                    "last_item_at": iso(at),
                    "last_event_seq": 0,
                },
            )
        opts = pipeline_settings_fixture({"registry": {}, "thresholds": {}})
        events = await Pipeline(
            repo, opts, progress=DisabledSourceProgress(), versions=declared_versions_fixture()
        )._candidate_events({marker}, iso(since), now)
        assert [event.event_id for event in events] == [marker + "a_lower", marker + "z_equal"]
    finally:
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reported_cost", [None, 0, 0.123456789])
async def test_jev_billed_cost_or_explicit_unknown_persists_under_sql_contract(
    reported_cost: float | None, isolated_routing_repo: PostgresRepository
) -> None:
    """Real SQL constraints accept unknown billing only with its canonical reason."""

    repo = isolated_routing_repo
    opts = pipeline_settings_fixture({"registry": {}, "thresholds": {}})
    item = source_item_fixture(new_id("COST-TEST"), source_id="S30", text="Ethereum news")
    calls = []

    def transport(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        usage: dict[str, JsonValue] = {"input_tokens": 10}
        if reported_cost is not None:
            usage["cost"] = reported_cost
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13-free",
                "usage": usage,
                "answers": {"relevant": {"type": "noul", "noul": 0.9}},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        pipeline = Pipeline(
            repo, opts, progress=DisabledSourceProgress(), http=http, versions=declared_versions_fixture()
        )
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.9
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.9
    assert len(calls) == 1
    records = await repo.list("ops.llm_usage", filters={"item_id": item.item_id})
    assert len(records) == 1
    row = Collection("ops.llm_usage", JevScoreRecord).decode(records[0])
    assert row.billing_basis == "OPENCODE_REPORTED"
    if reported_cost is None:
        assert row.cost_usd is None
        assert row.missing_fields.cost_usd == row.cost_reason == "OpenCode does not report usage.cost"
    else:
        assert row.cost_usd == Decimal(str(reported_cost))
        assert row.cost_reason is None
        assert row.missing_fields.cost_usd is None
