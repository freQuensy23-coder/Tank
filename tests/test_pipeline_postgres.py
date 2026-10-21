"""Routing concurrency with real transaction locks and no external calls."""

import asyncio
import os
from types import SimpleNamespace

import pytest

from tank.agents.pipeline import Pipeline
from tank.util import iso, new_id

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def isolated_routing_repo(postgres_template):
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires an explicitly configured PostgreSQL test connection")
    async with postgres_template.repository(url) as repo:
        yield repo


@pytest.mark.asyncio
async def test_independent_routing_calls_overlap_preserve_source_updates_and_deduplicate(
    isolated_routing_repo,
):
    repo = isolated_routing_repo
    marker, at = new_id("ROUTING-TEST"), iso()
    entity = marker + "-entity"
    config = {
        "registry": {"entities": [{"entity_id": entity, "aliases": [marker]}]},
        "thresholds": {"routing_cutoff": 0.8, "routing_window_days": 3},
    }
    opts = SimpleNamespace(load_config=config.__getitem__, config_version="test", code_version="test")
    event = marker + "-event"
    await repo.put(
        "evt.events",
        event,
        {
            "event_id": event,
            "statement": marker,
            "entities": [entity],
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
        },
    )
    identifiers = [
        marker + suffix for suffix in ("-first", "-second", "-duplicate", "-unrelated", "-new-duplicate")
    ]
    for item_id in identifiers:
        await repo.put(
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "source_id": "TEST",
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
            },
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
    pipe = Pipeline(repo, opts, http=object())
    arrived, release, both_blocked = set(), asyncio.Event(), asyncio.Event()

    async def score(item, name, state):
        assert state["event"]["statement"] == marker
        arrived.add(item["item_id"])
        if len(arrived) == 2:
            both_blocked.set()
        await release.wait()
        return 0.95

    pipe.score = score
    try:
        async with asyncio.timeout(5):
            tasks = [asyncio.create_task(pipe.route_item({"item_id": i})) for i in identifiers[:2]]
            try:
                await both_blocked.wait()
                async with repo.lock("ingest-dedup"):
                    latest = await repo.get("ingest.source_items", identifiers[0])
                    await repo.put(
                        "ingest.source_items", identifiers[0], {**latest, "new_observation": "preserved"}
                    )
                # These independent jobs run while both Jev requests wait.
                unrelated = await pipe.route_item({"item_id": identifiers[3]})
                assert unrelated["routing_message"] == {"kind": "NEW_ITEM", "item_id": identifiers[3]}
                new_one, new_two = await asyncio.gather(
                    *(pipe.route_item({"item_id": identifiers[4]}) for _ in range(2))
                )
                assert new_one == new_two
                assert "reservation_id" not in new_one["route"]
                release.set()
                first, second = await asyncio.gather(*tasks)
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)
        assert first["route_event_id"] == second["route_event_id"] == event
        assert first["new_observation"] == "preserved"
        score_calls = 0

        async def duplicate_score(*args):
            nonlocal score_calls
            score_calls += 1
            await asyncio.sleep(0.02)
            return 0.95

        pipe.score = duplicate_score
        one, two = await asyncio.gather(*(pipe.route_item({"item_id": identifiers[2]}) for _ in range(2)))
        assert one == two and score_calls == 1
        jobs = await repo.list("ops.jobs", filters={"idem_key": identifiers[2] + ":ITEM_ADDED"})
        assert len(jobs) == 1
        new_jobs = await repo.list("ops.jobs", filters={"idem_key": identifiers[4] + ":NEW_ITEM"})
        assert len(new_jobs) == 1
        assert not any("reservation_id" in row["routing_message"] for row in (first, second, one, two))
    finally:
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["failure", "cancellation"])
async def test_parallel_candidate_scores_survive_retry_without_partial_winner(
    interruption, isolated_routing_repo
):
    import json

    import httpx

    repo = isolated_routing_repo
    marker, at = new_id("ROUTE-PAIRS"), iso()
    item_id = marker + "-item"
    await repo.put(
        "ingest.source_items",
        item_id,
        {
            "item_id": item_id,
            "source_id": "TEST",
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
        },
    )
    config = {"registry": {}, "thresholds": {"routing_cutoff": 0.8}}
    opts = SimpleNamespace(
        load_config=config.__getitem__,
        config_version="test",
        code_version="test",
        openrouter_api_key="FAKE-offline-key",
        jev_provider="openrouter",
    )
    candidates = tuple({"event_id": marker + "-" + key, "statement": key} for key in ("a", "b", "c"))
    arrived, calls, ready = set(), {}, asyncio.Event()
    retry = False

    async def transport(request):
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
        pipe = Pipeline(repo, opts, http=http)

        async def fixed_candidates(*args):
            return candidates

        pipe._candidate_events = fixed_candidates
        try:
            attempt = asyncio.create_task(pipe.route_item({"item_id": item_id}))
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
            assert "routing_message" not in await repo.get("ingest.source_items", item_id)
            assert not await repo.list("ops.jobs", filters={"idem_key": item_id + ":ITEM_ADDED"})
            assert len(await repo.list("ops.llm_usage", filters={"item_id": item_id})) == 2
            retry = True
            async with asyncio.timeout(5):
                result, duplicate = await asyncio.gather(
                    *(pipe.route_item({"item_id": item_id}) for _ in range(2))
                )
            assert result == duplicate and result["route_event_id"] == marker + "-a"
            assert list(result["routing_scores"]) == [event["event_id"] for event in candidates]
            assert calls == {"a": 1, "b": 2, "c": 1}
            assert len(await repo.list("ops.jobs", filters={"idem_key": item_id + ":ITEM_ADDED"})) == 1
        finally:
            await repo.close()


@pytest.mark.asyncio
async def test_candidate_window_equal_batch_clock_is_inclusive_without_future_rows(isolated_routing_repo):
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
                {
                    "item_id": key,
                    "source_id": "TEST",
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
                },
            )
            await repo.put(
                "evt.events",
                key,
                {
                    "event_id": key,
                    "statement": key,
                    "entities": [marker],
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
        opts = SimpleNamespace()
        events = await Pipeline(repo, opts, http=object())._candidate_events({marker}, iso(since), now)
        assert [event["event_id"] for event in events] == [marker + "a_lower", marker + "z_equal"]
    finally:
        await repo.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reported_cost", [None, 0, 0.123456789])
async def test_jev_billed_cost_or_explicit_unknown_persists_under_sql_contract(
    reported_cost, isolated_routing_repo
):
    """Real SQL constraints accept unknown billing only with its canonical reason."""
    import httpx

    repo = isolated_routing_repo
    opts = SimpleNamespace(
        openrouter_api_key="FAKE_TEST_KEY",
        config_version="test-config",
        code_version="test-code",
    )
    item = {"item_id": new_id("COST-TEST"), "source_id": "S30", "text": "Ethereum news"}
    calls = []

    def transport(request):
        calls.append(request)
        usage = {"input_tokens": 10}
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
        pipeline = Pipeline(repo, opts, http=http)
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.9
        assert await pipeline.score(item, "relevant", pipeline.state(item)) == 0.9
    assert len(calls) == 1
    records = await repo.list("ops.llm_usage", filters={"item_id": item["item_id"]})
    assert len(records) == 1
    row = records[0]
    assert row["billing_basis"] == "OPENCODE_REPORTED"
    if reported_cost is None:
        assert row["cost_usd"] is None
        assert (
            row["missing_fields"]["cost_usd"] == row["cost_reason"] == "OpenCode does not report usage.cost"
        )
    else:
        assert row["cost_usd"] == str(reported_cost)
        assert row["cost_reason"] is None
        assert "cost_usd" not in row["missing_fields"]
