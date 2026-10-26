"""Actual worker progress while terminal question acceptance remains unproved."""

import asyncio
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Literal, Self

import httpx
import pytest
import respx
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from runtime_failure_fixtures import runtime_failure_fixture

from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import JobEnvelope
from tank.contracts.question_retirement import QuestionFailureRefusalLedger
from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.runtime_contracts import FailureRecordIdentity, SuppliedFailureReason, UserQuestionMessage
from tank.runtime_failure_repository import RepositoryRuntimeFailures
from tank.storage import MemoryRepository, PostgresRepository, Repository
from tank.time_precision import Timestamp, collection_clock
from tank.util import JsonInput


async def seed_unproved_question(repo: Repository) -> str:
    message: dict[str, JsonValue] = {
        "message_id": "message",
        "turn_status": "RUNNING",
        "body": "Opaque retained body",
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
    }
    journal: list[JsonValue] = [message]
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "active_message_id": "message",
        "proposal_id": "synthetic-proposal",
        "agent_version_reason": "This authored retained session has no observed provider version",
        "last_item_at_reason": "This authored scout session has no source item",
        "last_event_seq": 0,
        **collection_clock().fields("opened_at"),
        "message_log": journal,
    }
    await repo.put("ops.agent_sessions", "session", session)
    await repo.put(
        "out.user_questions",
        "question",
        {"question_id": "question", "session_id": "session", "status": "QUEUED"},
    )
    jobs = JobRepository(repo)
    job = await jobs.enqueue(
        "AGENT_MESSAGE",
        {"kind": "USER_QUESTION", "session_id": "session", "question_id": "question"},
        "terminal-question",
        max_attempts=1,
    )
    terminal = JobEnvelope.model_validate(
        {
            **job.model_dump(mode="json"),
            "status": "FAILED",
            "last_error": "SyntheticDeliveryFailure",
            **collection_clock().fields("finished_at"),
        }
    )
    return (await jobs.put(terminal)).job_id


class RefusalWriteFault:
    def __init__(self, store: Repository, mode: Literal["BEFORE", "AFTER"]) -> None:
        self.store = store
        self.mode = mode
        self.failed = False
        self.written = False
        self.depth: ContextVar[int] = ContextVar("owned-refusal-test-lock-depth", default=0)

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        return await self.store.get(collection, key)

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        refusal = "host_question_failure_refusals" in record
        if refusal and self.mode == "BEFORE" and not self.failed:
            self.failed = True
            raise ConnectionError("Synthetic private transport https://synthetic.invalid/private-token")
        result = await self.store.put(collection, key, record, immutable=immutable)
        if refusal:
            self.written = True
        return result

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        depth = self.depth.get()
        token = self.depth.set(depth + 1)
        try:
            async with self.store.lock(key):
                yield self
        finally:
            self.depth.reset(token)
        if depth == 0 and self.mode == "AFTER" and self.written and not self.failed:
            self.failed = True
            raise ConnectionError("Synthetic private commit https://synthetic.invalid/private-token")

    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None, batch_size: int = 1000
    ) -> AsyncIterator[dict[str, JsonValue]]:
        return self.store.iter_all(collection, filters=filters, batch_size=batch_size)


async def exercise_refusal_write_fault(
    repo: MemoryRepository | PostgresRepository,
    mode: Literal["BEFORE", "AFTER"],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = await seed_unproved_question(repo)
    original = await repo.get("ops.agent_sessions", "session")
    fault = RefusalWriteFault(repo, mode)
    stop = asyncio.Event()
    output = tmp_path / "fresh-work.txt"
    async with runtime_failure_fixture(repo, tmp_path / "first", ops_progress_enabled=False) as runtime:
        runtime.failures = RepositoryRuntimeFailures(
            fault, runtime.settings.require_versions(), collection_clock
        )

        evaluate = runtime.scheduler.eval_conditions

        async def fresh_work(cutoff: Timestamp) -> ConditionEvaluationResult:
            result = await evaluate(cutoff)
            output.write_text("actual fresh worker work")
            stop.set()
            return result

        patch = pytest.MonkeyPatch()
        patch.setattr(runtime.scheduler, "eval_conditions", fresh_work)
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            async with asyncio.timeout(2):
                while not fault.failed:
                    if worker.done():
                        await worker
                    await asyncio.sleep(0.001)
            observed = await repo.get("ops.jobs", job_id)
            assert observed is not None and not observed.get("followup_failure_recorded_at")
            if mode == "BEFORE":
                assert "host_question_failure_refusals" not in observed
            else:
                entries = observed["host_question_failure_refusals"]
                assert isinstance(entries, dict) and len(entries) == 1
            fresh = await repo.enqueue(
                "EVAL_CONDITIONS", collection_clock().fields("cutoff"), "fresh-worker-work"
            )
            async with asyncio.timeout(2):
                await worker
            completed = await repo.get("ops.jobs", FailureRecordIdentity.from_record(fresh).job_id)
            assert completed is not None and completed["status"] == "DONE"
            assert output.read_text() == "actual fresh worker work"
        finally:
            stop.set()
            await worker
            patch.undo()
    assert "persistence unavailable" in caplog.text and "private-token" not in caplog.text
    assert await repo.get("ops.agent_sessions", "session") == original
    question = await repo.get("out.user_questions", "question")
    assert question is not None and "answer_status" not in question
    assert await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"}) == []
    async with runtime_failure_fixture(repo, tmp_path / "restart", ops_progress_enabled=False) as restarted:
        await restarted.reconcile_delivery_failures()
        retained = await repo.get("ops.jobs", job_id)
        assert retained is not None and not retained.get("followup_failure_recorded_at")
        entries = retained["host_question_failure_refusals"]
        assert isinstance(entries, dict)
        ledger = QuestionFailureRefusalLedger.model_validate({"job_id": job_id, "entries": entries})
        assert len(ledger.entries) == 1
        receipt = next(iter(ledger.entries.values()))
        assert receipt.request.witness.reason == "INITIAL_ACCEPTANCE_NOT_RETAINED"
        assert receipt.request.witness.target.session_id == "session"
        assert receipt.recorded_at.value is not None
        restarted._failure_reconcile_attempted_at = None
        await restarted.reconcile_delivery_failures()
        assert await repo.get("ops.jobs", job_id) == retained
        envelope = await JobRepository(repo).get(job_id)
        assert envelope is not None and envelope.host_question_failure_refusals is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["BEFORE", "AFTER"])
async def test_receipt_write_failure_does_not_starve_fresh_worker_work_or_invent_a_question_answer(
    mode: Literal["BEFORE", "AFTER"],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await exercise_refusal_write_fault(MemoryRepository(), mode, tmp_path, caplog)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["BEFORE", "AFTER"])
async def test_actual_postgres_worker_refusal_is_durable_and_preserves_fresh_work_after_persistence_failure(
    postgres_template: PostgresTemplate,
    mode: Literal["BEFORE", "AFTER"],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.migrate()
        await exercise_refusal_write_fault(repo, mode, tmp_path, caplog)


@pytest.mark.asyncio
async def test_actual_missing_native_observation_reconciles_changed_facts_without_rewriting_past_refusal(
    tmp_path: Path,
) -> None:
    repo = MemoryRepository()
    job_id = await seed_unproved_question(repo)
    routes: list[respx.Route] = []

    def native_missing(router: respx.MockRouter) -> None:
        routes.append(
            router.get("http://native.invalid/v1/sessions/session").mock(return_value=httpx.Response(404))
        )

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=native_missing
    ) as runtime:
        await runtime.reconcile_delivery_failures()
        first = await repo.get("ops.jobs", job_id)
        assert first is not None
        old_refusals = first["host_question_failure_refusals"]
        assert not await runtime.sessions.continuable(
            UserQuestionMessage(kind="USER_QUESTION", session_id="session", question_id="question")
        )
        runtime._failure_reconcile_attempted_at = None
        await runtime.reconcile_delivery_failures()
        stored = await repo.get("ops.jobs", job_id)
        assert stored is not None and stored["followup_failure_recorded_at_prec"] == "COLLECTION_ONLY"
        assert stored["host_question_failure_refusals"] == old_refusals
        assert len(routes) == 1 and routes[0].call_count == 1
    question = await repo.get("out.user_questions", "question")
    assert question is not None and question["answer_status"] == "NO_SESSION"
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1
    session = await repo.get("ops.agent_sessions", "session")
    assert session is not None and session["status"] == "OPEN"
    assert "termination_receipt" not in session and "retired_message_ids" not in session


@pytest.mark.asyncio
async def test_terminal_delivery_requires_the_actual_integer_attempt_before_any_effect(
    tmp_path: Path,
) -> None:
    repo = MemoryRepository()
    job = await repo.enqueue(
        "DELIVER", {"alert_id": "alert", "version": 1, "channel": "telegram:alerts"}, "invalid-attempt"
    )
    job_id = FailureRecordIdentity.from_record(job).job_id
    job.update(status="FAILED", attempts=True, **collection_clock().fields("finished_at"))
    await repo.put("ops.jobs", job_id, job)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        with pytest.raises(ValidationError):
            await runtime.terminal_delivery_failure_wire(
                job, SuppliedFailureReason(kind="SUPPLIED", value="ValidationError")
            )
    stored = await repo.get("ops.jobs", job_id)
    assert stored == job and stored is not None and stored["attempts"] is True
    assert await repo.list("out.deliveries") == [] and await repo.list("out.ops_notices") == []


@pytest.mark.asyncio
async def test_changed_job_target_cannot_execute_or_mark_an_older_failure_command() -> None:
    from agent_source_fixtures import declared_versions_fixture

    from tank.contracts.session_followups import FollowupRefusalTarget
    from tank.question_answers import record_answer
    from tank.runtime_contracts import (
        FailureReconciliationItem,
        TerminalFailureHandled,
        TerminalFailureOutcome,
        TerminalFailureSkipped,
    )

    repo = MemoryRepository()
    job_id = await seed_unproved_question(repo)
    original = await repo.get("ops.jobs", job_id)
    assert original is not None
    item = FailureReconciliationItem.from_record(
        original,
        marker_name="followup_failure_recorded_at",
        progress_requested=False,
        notifications_requested=True,
    )
    await repo.put(
        "out.user_questions",
        "foreign",
        {"question_id": "foreign", "session_id": "session", "status": "QUEUED"},
    )
    changed = {
        **original,
        "payload": {"kind": "USER_QUESTION", "session_id": "session", "question_id": "foreign"},
    }
    await repo.put("ops.jobs", job_id, changed)
    failures = RepositoryRuntimeFailures(repo, declared_versions_fixture(), collection_clock)

    async def wrong_recipient_effect() -> TerminalFailureOutcome:
        await record_answer(
            repo, FollowupRefusalTarget(question_id="question", session_id="session"), None, status="FAILED"
        )
        return TerminalFailureHandled(status="HANDLED")

    outcome = await failures.execute(item.failure, wrong_recipient_effect)
    assert isinstance(outcome, TerminalFailureSkipped) and outcome.reason == "JOB_CHANGED"
    assert not await failures.mark(item)
    assert await repo.get("ops.jobs", job_id) == changed
    for identity in ("question", "foreign"):
        question = await repo.get("out.user_questions", identity)
        assert question is not None and "answer_status" not in question
    assert await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"}) == []


class RejectRefusalStore(RefusalWriteFault):
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        from psycopg import IntegrityError

        if "host_question_failure_refusals" in record:
            raise IntegrityError("Synthetic private row https://synthetic.invalid/private-token")
        return await super().put(collection, key, record, immutable=immutable)


@pytest.mark.asyncio
async def test_invalid_persistence_is_not_relabelled_as_retryable_availability_or_handled() -> None:
    from agent_source_fixtures import declared_versions_fixture

    from tank.contracts.question_retirement import QuestionFailureRefusalRequest, UnknownQuestionAcceptance
    from tank.contracts.session_followups import FollowupRefusalTarget
    from tank.runtime_contracts import FailureJournalRejected

    repo = MemoryRepository()
    job_id = await seed_unproved_question(repo)
    original = await repo.get("ops.jobs", job_id)
    failures = RepositoryRuntimeFailures(
        RejectRefusalStore(repo, "BEFORE"), declared_versions_fixture(), collection_clock
    )
    request = QuestionFailureRefusalRequest(
        job_id=job_id,
        job_status="FAILED",
        witness=UnknownQuestionAcceptance(
            target=FollowupRefusalTarget(question_id="question", session_id="session"),
            message_id="message",
            turn_status="RUNNING",
            reason="INITIAL_ACCEPTANCE_NOT_RETAINED",
        ),
    )
    with pytest.raises(FailureJournalRejected) as rejected:
        await failures.record(request)
    assert "private-token" not in str(rejected.value)
    assert await repo.get("ops.jobs", job_id) == original
    assert await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"}) == []


def test_terminal_failure_keeps_wire_alias_admission_and_revalidates_typed_payload() -> None:
    from tank.runtime_contracts import (
        AlertDeliveryJob,
        FailedDeliveryNotification,
        TerminalJobFailure,
    )

    record: dict[str, JsonValue] = {
        "job_id": "delivery",
        "type": "DELIVER",
        "status": "FAILED",
        "attempts": 1,
        "payload": {
            "alert_id": "alert",
            "version": 1,
            "channel": "telegram:alerts",
            "_depends_on": ["prior"],
        },
    }
    command = TerminalJobFailure.from_record(
        record,
        reason=SuppliedFailureReason(kind="SUPPLIED", value="DeliveryFailure"),
        progress_requested=False,
        notifications_requested=True,
    )
    assert isinstance(command.notification, FailedDeliveryNotification)
    assert command.notification.payload.depends_on == ["prior"]
    assert TerminalJobFailure.model_validate(command, by_name=True) == command
    wrong_wire: dict[str, JsonValue] = {
        **record,
        "payload": {"alert_id": "alert", "version": 1, "channel": "telegram:alerts", "depends_on": ["prior"]},
    }
    with pytest.raises(ValidationError):
        TerminalJobFailure.from_record(
            wrong_wire,
            reason=command.reason,
            progress_requested=False,
            notifications_requested=True,
        )
    corrupted = AlertDeliveryJob.model_construct(
        alert_id="alert",
        version=True,
        channel="telegram:alerts",
        depends_on=[],
    )
    with pytest.raises(ValidationError):
        FailedDeliveryNotification.model_validate(
            {"kind": "DELIVERY", "payload": corrupted, "attempt": 1},
            by_name=True,
        )
