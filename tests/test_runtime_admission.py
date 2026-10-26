"""Typed worker admission uses actual claimed routing and original queue order."""

import pytest
from pydantic import JsonValue
from test_storage import repo as repo

from tank.contracts.session_admission import SessionTarget
from tank.job_repository import JobRepository
from tank.runtime_admission import worker_admission
from tank.runtime_dispatch_contracts import ClaimedWork
from tank.runtime_worker_repository import RepositoryWorkerQueue
from tank.storage import StorageBackend
from tank.util import parse_time


@pytest.mark.parametrize(
    ("kind", "payload", "target"),
    [
        (
            "AGENT_MESSAGE",
            {"kind": "NEW_ITEM", "item_id": "item", "event_id": "event"},
            SessionTarget(session_id=None, event_id="event", item_id="item", proposal_id=None),
        ),
        (
            "AGENT_MESSAGE",
            {"kind": "USER_QUESTION", "question_id": "question", "session_id": "session"},
            SessionTarget(session_id="session", event_id=None, item_id=None, proposal_id=None),
        ),
        (
            "SCOUT_REVIEW",
            {"proposal_id": "proposal"},
            SessionTarget(session_id=None, event_id=None, item_id=None, proposal_id="proposal"),
        ),
    ],
)
async def test_claimed_command_projects_actual_target_identity_and_queue_order(
    repo: StorageBackend, kind: str, payload: dict[str, JsonValue], target: SessionTarget
) -> None:
    queued = await JobRepository(repo).enqueue(kind, payload, "admission", priority=3)
    work = await RepositoryWorkerQueue(repo).claim()
    assert isinstance(work, ClaimedWork)
    candidate = worker_admission(work)
    assert candidate is not None and candidate.job_id == queued.job_id
    assert candidate.target == target
    assert candidate.order.priority == 3
    assert candidate.order.since == parse_time(queued.created_at)


async def test_non_session_command_never_consumes_or_invents_an_admission_slot(repo: StorageBackend) -> None:
    queued = await JobRepository(repo).enqueue("FILTER_ITEM", {"item_id": "item"}, "filter")
    work = await RepositoryWorkerQueue(repo).claim()
    assert isinstance(work, ClaimedWork)
    before = await repo.get("ops.jobs", queued.job_id)
    assert worker_admission(work) is None
    assert await repo.get("ops.jobs", queued.job_id) == before
