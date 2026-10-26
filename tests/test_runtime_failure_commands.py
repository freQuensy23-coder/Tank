"""Terminal notification commands retain actual queued subjects and terminal phase."""

import pytest
from pydantic import JsonValue
from test_storage import repo as repo

from tank.contracts.job_repository import JobRepository
from tank.contracts.progress import JobFailureProgressView
from tank.runtime_contracts import NoFailureProgress, SuppliedFailureReason
from tank.runtime_dispatch_contracts import ClaimedWork, WorkerFailureRequest
from tank.runtime_failure_commands import terminal_work_failure
from tank.runtime_retry import ApplicationBackoff
from tank.runtime_worker_repository import RepositoryWorkerQueue
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


@pytest.mark.parametrize(
    ("kind", "payload", "notification", "reported"),
    [
        ("DELIVER", {"alert_id": "alert", "version": 1, "channel": "telegram:alerts"}, "DELIVERY", False),
        (
            "AGENT_MESSAGE",
            {"kind": "USER_QUESTION", "question_id": "question", "session_id": "session"},
            "QUESTION",
            False,
        ),
        ("TELEGRAM_ANSWER", {"question_id": "question"}, "ANSWER", False),
        ("FILTER_ITEM", {"item_id": "item"}, "NONE", True),
    ],
)
async def test_actual_failed_claim_builds_only_its_matching_terminal_commands(
    repo: StorageBackend, kind: str, payload: dict[str, JsonValue], notification: str, reported: bool
) -> None:
    await JobRepository(repo).enqueue(kind, payload, "terminal", max_attempts=1)
    queue = RepositoryWorkerQueue(repo)
    work = await queue.claim()
    assert isinstance(work, ClaimedWork)
    reason = SuppliedFailureReason(kind="SUPPLIED", value="RuntimeError")
    with pytest.raises(ValueError, match="actual terminal"):
        terminal_work_failure(work, work.job, reason, progress_requested=True, notifications_requested=True)
    terminal = await queue.fail(
        WorkerFailureRequest(
            claim=work.scope(),
            error_class="RuntimeError",
            schedule=ApplicationBackoff(basis="APPLICATION_BACKOFF", attempts=1),
            observed_at=collection_clock(),
        )
    )
    assert terminal is not None and terminal.status == "FAILED"
    command = terminal_work_failure(
        work, terminal, reason, progress_requested=True, notifications_requested=True
    )
    assert command.header.job_id == work.job.job_id and command.header.status == "FAILED"
    assert command.notification.kind == notification
    assert isinstance(command.progress, JobFailureProgressView) is reported
    if isinstance(command.progress, JobFailureProgressView):
        assert command.progress.target.item_id == "item"
    else:
        assert isinstance(command.progress, NoFailureProgress) and command.progress.reason == "NOT_APPLICABLE"
    foreign = terminal.model_copy(update={"payload": {"foreign": "subject"}})
    with pytest.raises(ValueError, match="actual claimed command"):
        terminal_work_failure(work, foreign, reason, progress_requested=True, notifications_requested=True)
