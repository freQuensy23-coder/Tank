"""FILTER_ITEM and ROUTE_ITEM survive a Decision-1 outage by retrying across their job window."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import Pipeline
from tank.runtime import Runtime
from tank.storage import DECISION_MAX_ATTEMPTS, MemoryRepository, retry_delay
from tank.time_precision import collection_clock
from tank.util import parse_time, utcnow


def settings():
    return SimpleNamespace(
        foundry_api_key="FAKE_FOUNDRY_KEY",
        replay=False,
        config_version="test",
        code_version="test",
        load_config=lambda name: {"filter_cutoff": 0.5},
    )


def test_decision_backoff_spans_the_job_window_and_other_kinds_keep_theirs():
    delays = [retry_delay("FILTER_ITEM", attempt) for attempt in range(1, 9)]
    assert delays == [15, 30, 60, 120, 240, 480, 900, 900]
    assert [retry_delay("ROUTE_ITEM", attempt) for attempt in range(1, 9)] == delays
    window = sum(retry_delay("FILTER_ITEM", attempt) for attempt in range(1, DECISION_MAX_ATTEMPTS))
    assert window >= timedelta(hours=6).total_seconds()
    assert [retry_delay("DELIVER", attempt) for attempt in (1, 2, 3, 6, 9)] == [2, 4, 8, 60, 60]


@pytest.mark.asyncio
async def test_filter_item_outlasts_a_decision_one_outage_then_routes_the_item():
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items",
        "i",
        {
            "item_id": "i",
            "source_id": "S30",
            "text": "ETH is a crypto asset",
            "relevance": {"decision": None, "pending_reason": "AWAITING_FILTER"},
        },
    )
    outage = 5
    calls = []

    def foundry(request):
        calls.append(request)
        if len(calls) <= outage:
            return httpx.Response(503, json={"error": {"message": "unavailable"}})
        return httpx.Response(
            200,
            json={
                "model": "microsoft-decision-1",
                "answers": {"relevant": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 10, "output_tokens": 1},
            },
        )

    job = await repo.enqueue("FILTER_ITEM", {"item_id": "i"}, "i", max_attempts=DECISION_MAX_ATTEMPTS)
    async with httpx.AsyncClient(transport=httpx.MockTransport(foundry)) as http:
        pipeline = Pipeline(repo, settings(), http=http)
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        delays = []
        for _ in range(outage + 1):
            stop = asyncio.Event()

            async def filter_once(payload, stop=stop):
                try:
                    return await pipeline.filter_item(payload)
                finally:
                    stop.set()

            runtime.handlers = {"FILTER_ITEM": filter_once}
            failed_at = utcnow()
            await runtime.worker(stop)
            current = await repo.get("ops.jobs", job["job_id"])
            if current["status"] != "QUEUED":
                break
            delays.append(round((parse_time(current["run_after"]) - failed_at).total_seconds()))
            await repo.put("ops.jobs", job["job_id"], {**current, **collection_clock().fields("run_after")})
    assert delays == [15, 30, 60, 120, 240]
    assert current["status"] == "DONE" and current["attempts"] == outage + 1
    item = await repo.get("ingest.source_items", "i")
    assert item["relevance"]["decision"] == "SEND"
    route = await repo.list("ops.jobs", filters={"type": "ROUTE_ITEM", "idem_key": "i"})
    assert len(route) == 1 and route[0]["max_attempts"] == DECISION_MAX_ATTEMPTS
