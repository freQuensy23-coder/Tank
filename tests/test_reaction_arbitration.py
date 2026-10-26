"""Actual claims and EDF policy cannot turn queued peer intent into capture evidence."""

from datetime import timedelta
from typing import Never

import pytest
from pydantic import JsonValue
from test_runtime_reaction_coalescing import revised_payload
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import JobEnvelope
from tank.contracts.scheduler_commands import ReactionCaptureCommand
from tank.runtime_reaction import (
    ReactionCoalescingDeferred,
    ReactionJobInvocation,
    capture_reaction_job,
    reaction_leader,
)
from tank.scheduler import CAPTURE_EXPIRY_S, REACTION_RETRY_S
from tank.storage import StorageBackend
from tank.time_precision import collection_clock, offset_schedule
from tank.util import parse_time, utcnow


class RefuseUnexpectedCapture:
    async def reaction_capture(self, request: ReactionCaptureCommand) -> Never:
        raise AssertionError("An EDF follower must not begin measured capture")


async def actual_running_invocation(repo: StorageBackend) -> ReactionJobInvocation:
    jobs = JobRepository(repo)
    job = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("claimed-change"),
        "claimed-change",
        priority=2,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    claimed = await jobs.claim()
    assert isinstance(claimed, JobEnvelope) and claimed.job_id == job.job_id
    assert claimed.status == "RUNNING" and claimed.claim_token is not None
    return ReactionJobInvocation.from_envelope(claimed)


async def test_eligible_edf_peer_defers_actual_claim_without_a_completion_receipt(
    repo: StorageBackend,
) -> None:
    invocation = await actual_running_invocation(repo)
    jobs = JobRepository(repo)
    peer = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("eligible-peer") | {"_depends_on": []},
        "eligible-peer",
        priority=1,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    before = await repo.get("ops.jobs", invocation.context.scope.job_id)
    stamp = collection_clock()
    with pytest.raises(ReactionCoalescingDeferred) as outcome:
        await capture_reaction_job(
            RefuseUnexpectedCapture(),
            repo,
            invocation,
            clock=lambda: stamp,
            retry_delay=timedelta(seconds=REACTION_RETRY_S),
        )
    assert outcome.value.pending.coalesced_into == peer.job_id
    assert stamp.value is not None and outcome.value.not_before.value is not None
    assert (
        parse_time(outcome.value.not_before.value) - parse_time(stamp.value)
    ).total_seconds() == REACTION_RETRY_S
    assert await repo.get("ops.jobs", invocation.context.scope.job_id) == before
    duplicate = await jobs.enqueue(
        "REACTION_CAPTURE",
        revised_payload("claimed-change"),
        "claimed-change",
        priority=2,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    assert duplicate.job_id == invocation.context.scope.job_id and duplicate.status == "RUNNING"
    assert "result" not in duplicate.model_fields_set
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []


@pytest.mark.parametrize("disposition", ["future", "missing-dependency", "other-subject"])
async def test_non_runnable_or_unrelated_peer_cannot_consume_current_change(
    repo: StorageBackend, disposition: str
) -> None:
    invocation = await actual_running_invocation(repo)
    payload: dict[str, JsonValue] = revised_payload("ineligible-peer")
    if disposition == "missing-dependency":
        payload["_depends_on"] = ["unreported-dependency"]
    elif disposition == "other-subject":
        payload["event_id"] = "other-event"
    due = (
        offset_schedule(collection_clock(), timedelta(minutes=1))
        if disposition == "future"
        else collection_clock()
    )
    await JobRepository(repo).enqueue(
        "REACTION_CAPTURE",
        payload,
        "ineligible-peer",
        priority=1,
        run_after=due,
        expires_at=offset_schedule(collection_clock(), timedelta(seconds=CAPTURE_EXPIRY_S)),
    )
    selected = await reaction_leader(repo, invocation, utcnow())
    assert selected.job_id == invocation.context.scope.job_id
    assert await repo.list("out.reaction_checkpoints") == []
    assert await repo.list("ops.scheduler_checkpoints") == []
