"""Capture work must commit independently and drain before queue coordination releases."""

import asyncio
from datetime import timedelta

import httpx
import pytest
from pydantic import JsonValue
from test_reaction_arbitration import actual_running_invocation
from test_storage import repo as repo

from tank.contracts.scheduler_commands import ReactionCaptureCommand, ReactionTarget
from tank.contracts.scheduler_results import ReactionCaptureResult
from tank.runtime_reaction import capture_reaction_job, reaction_coordination_key
from tank.scheduler import REACTION_RETRY_S
from tank.sources.capture_fetch import FetchRequest, capture_fetch
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


class ActualFetchProbe:
    def __init__(self, repo: StorageBackend, *, hold_cleanup: bool = False) -> None:
        self.repo = repo
        self.hold_cleanup = hold_cleanup
        self.cleanup_started = asyncio.Event()
        self.release_cleanup = asyncio.Event()
        self.drained = asyncio.Event()

    async def reaction_capture(self, request: ReactionCaptureCommand) -> ReactionCaptureResult:
        try:
            await capture_fetch(self.repo, "GET", "https://api.binance.com/api/v3/time")
            raise AssertionError("This fixture declares unavailable capture inputs, never measured prices")
        finally:
            self.cleanup_started.set()
            if self.hold_cleanup:
                await self.release_cleanup.wait()
            self.drained.set()


async def committed_fetch(repo: StorageBackend) -> dict[str, JsonValue]:
    async with asyncio.timeout(10):
        while True:
            rows = await repo.list(
                "mkt.capture_requests", filters={"kind": "HTTP_FETCH", "status": "PENDING"}
            )
            if rows:
                assert len(rows) == 1
                return rows[0]
            await asyncio.sleep(0.02)


def fetch_identity(record: dict[str, JsonValue]) -> str:
    value = record["request_id"]
    assert isinstance(value, str) and value
    request = FetchRequest.model_validate(record["request"])
    assert request.method == "GET" and request.url == "https://api.binance.com/api/v3/time"
    assert request.source_id is None
    return value


async def test_real_fetch_queue_is_visible_to_an_independent_task_while_coordination_is_held(
    repo: StorageBackend,
) -> None:
    invocation = await actual_running_invocation(repo)
    probe = ActualFetchProbe(repo)
    runner = asyncio.create_task(
        capture_reaction_job(
            probe, repo, invocation, clock=collection_clock, retry_delay=timedelta(seconds=REACTION_RETRY_S)
        )
    )
    try:
        request = await committed_fetch(repo)
        request_id = fetch_identity(request)
        assert not runner.done()
        await repo.put(
            "mkt.capture_requests",
            request_id,
            {
                **request,
                "status": "FAILED",
                "reason": "SYNTHETIC_CAPTURE_INPUTS_UNAVAILABLE",
                "result": None,
                **collection_clock().fields("finished_at"),
            },
        )
        with pytest.raises(httpx.TransportError, match="SYNTHETIC_CAPTURE_INPUTS_UNAVAILABLE"):
            await asyncio.wait_for(runner, 10)
    finally:
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
    assert probe.drained.is_set()
    current = await repo.get("ops.jobs", invocation.context.scope.job_id)
    assert current is not None and current["status"] == "RUNNING" and "result" not in current
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []
    assert await repo.list("ana.market_truth") == []


async def test_cancelled_capture_drains_before_releasing_the_actual_coordination_lock(
    repo: StorageBackend,
) -> None:
    invocation = await actual_running_invocation(repo)
    command = invocation.command
    key = reaction_coordination_key(
        ReactionTarget(event_id=command.event_id, token_id=command.token_id, checkpoint=command.checkpoint)
    )
    probe = ActualFetchProbe(repo, hold_cleanup=True)
    runner = asyncio.create_task(
        capture_reaction_job(
            probe, repo, invocation, clock=collection_clock, retry_delay=timedelta(seconds=REACTION_RETRY_S)
        )
    )
    competing_started, competing_acquired = asyncio.Event(), asyncio.Event()

    async def compete() -> None:
        competing_started.set()
        async with repo.lock(key):
            assert probe.drained.is_set()
            competing_acquired.set()

    competitor: asyncio.Task[None] | None = None
    try:
        request = await committed_fetch(repo)
        request_id = fetch_identity(request)
        runner.cancel()
        await asyncio.wait_for(probe.cleanup_started.wait(), 10)
        competitor = asyncio.create_task(compete())
        await competing_started.wait()
        await asyncio.sleep(0.05)
        assert not runner.done() and not competing_acquired.is_set()
        probe.release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await runner
        await asyncio.wait_for(competitor, 10)
        assert competing_acquired.is_set() and probe.drained.is_set()
        retained = await repo.get("mkt.capture_requests", request_id)
        assert retained is not None and retained["status"] == "PENDING" and retained["result"] is None
    finally:
        probe.release_cleanup.set()
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if competitor is not None:
            if not competitor.done():
                competitor.cancel()
            await asyncio.gather(competitor, return_exceptions=True)
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []
