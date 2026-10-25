"""A refusal can answer only its authoritative locked question/session binding."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Self

import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue

from tank.contracts.session_followups import (
    Archived,
    FollowupRefusalTarget,
    NoSession,
    RetentionArchived,
    Terminated,
)
from tank.question_answers import QuestionAnswerRepository
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_inbox import (
    ARCHIVED_TEXT,
    EARLY_ARCHIVED_TEXT,
    GONE_TEXT,
    KEPT_ARCHIVED_TEXT,
    TERMINATED_TEXT,
    RepositoryFollowupRefusals,
    closed_reason,
    format_followup_closure,
    record_answer,
)
from tank.time_precision import Timestamp


def question(*, identity: str = "question", session: str | None = "session") -> dict[str, JsonValue]:
    return {
        "question_id": identity,
        "session_id": session,
        "status": "QUEUED",
        "text": "Why?",
        "reference": {"text": "Actual prior publication"},
        "attachments": [],
    }


class ObservedQuestionLock:
    """Observe lock entry while every operation uses the actual repository backend."""

    def __init__(self, repo: QuestionAnswerRepository) -> None:
        self.repo = repo
        self.attempted = asyncio.Event()

    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None:
        return await self.repo.get(collection, key)

    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        return await self.repo.put(collection, key, record, immutable=immutable)

    @asynccontextmanager
    async def lock(self, key: str) -> AsyncIterator[Self]:
        self.attempted.set()
        async with self.repo.lock(key):
            yield self

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        async with self.repo.transaction():
            yield self

    async def enqueue(
        self,
        kind: str,
        payload: dict[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        run_after: Timestamp,
        max_attempts: int,
        expires_at: Timestamp,
    ) -> JsonValue:
        return await self.repo.enqueue(
            kind,
            payload,
            idem_key,
            priority=priority,
            run_after=run_after,
            max_attempts=max_attempts,
            expires_at=expires_at,
        )


async def assert_locked_refusal_ownership(repo: MemoryRepository | PostgresRepository) -> None:
    target = FollowupRefusalTarget(question_id="question", session_id="session")
    closure = NoSession(status="NO_SESSION", reason="NATIVE_MISSING")
    reporter = RepositoryFollowupRefusals(repo)
    for stored in (question(identity="foreign"), question(session="foreign"), question(session=None)):
        await repo.put("out.user_questions", "question", stored)
        with pytest.raises(ValueError, match="identity|another session"):
            await reporter.record(target, closure)
        assert await repo.get("out.user_questions", "question") == stored
        assert await repo.list("ops.jobs") == []

    observed = ObservedQuestionLock(repo)
    race_reporter = RepositoryFollowupRefusals(observed)
    original = question()
    await repo.put("out.user_questions", "question", original)
    async with repo.lock("question:question"):
        task = asyncio.create_task(race_reporter.record(target, closure))
        await asyncio.wait_for(observed.attempted.wait(), timeout=2)
        assert not task.done()
        foreign = question(session="rebound")
        async with repo.transaction():
            await repo.put("out.user_questions", "question", foreign)
    with pytest.raises(ValueError, match="another session"):
        await task
    assert await repo.get("out.user_questions", "question") == foreign
    assert await repo.list("ops.jobs") == []

    await repo.put("out.user_questions", "question", original)
    await reporter.record(target, closure)
    answered = await repo.get("out.user_questions", "question")
    assert answered is not None
    assert answered["question_id"] == target.question_id and answered["session_id"] == target.session_id
    assert answered["answer_status"] == "NO_SESSION" and answered["reason_text"] == GONE_TEXT
    assert answered["answer_text"] is None and answered["reference"] == original["reference"]
    assert answered["attachments"] == original["attachments"]
    jobs = await repo.list("ops.jobs")
    assert len(jobs) == 1 and jobs[0]["type"] == "TELEGRAM_ANSWER"
    assert jobs[0]["payload"] == {"question_id": target.question_id}
    assert jobs[0]["max_attempts"] == 5 and jobs[0]["priority"] == 0
    await reporter.record(target, Terminated(status="TERMINATED", reason="TERMINATED"))
    assert await repo.get("out.user_questions", "question") == answered
    assert len(await repo.list("ops.jobs")) == 1


@pytest.mark.asyncio
async def test_memory_refusal_checks_both_ids_after_actual_lock_and_preserves_idempotence() -> None:
    await assert_locked_refusal_ownership(MemoryRepository())


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_refusal_checks_both_ids_after_actual_lock_and_preserves_idempotence(
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await assert_locked_refusal_ownership(repo)


@pytest.mark.parametrize(
    "closure,expected",
    [
        (NoSession(status="NO_SESSION", reason="ABSENT"), GONE_TEXT),
        (NoSession(status="NO_SESSION", reason="REPLAY"), GONE_TEXT),
        (NoSession(status="NO_SESSION", reason="NATIVE_MISSING"), GONE_TEXT),
        (Terminated(status="TERMINATED", reason="TERMINATED"), TERMINATED_TEXT),
        (Archived(status="ARCHIVED", reason="ARCHIVED"), ARCHIVED_TEXT),
        (Archived(status="ARCHIVED", reason="EARLY_CLOSURE"), EARLY_ARCHIVED_TEXT),
        (Archived(status="ARCHIVED", reason="RETENTION_UNREPORTED"), ARCHIVED_TEXT),
        (
            RetentionArchived(status="ARCHIVED", reason="RETENTION", keep_days=7),
            KEPT_ARCHIVED_TEXT.format(days="7 дней"),
        ),
    ],
)
def test_closure_formatting_preserves_existing_user_status_wording(
    closure: NoSession | Terminated | Archived | RetentionArchived, expected: str
) -> None:
    assert format_followup_closure(closure) == expected


def test_wrapper_consumes_actual_inward_policy_and_preserves_termination_over_missing() -> None:
    stored: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "remote_state": {"kind": "MISSING", "missing": True, "session_id": "session"},
        "termination_receipt": {"event_id": "actual-termination"},
    }
    assert closed_reason(stored) == ("TERMINATED", TERMINATED_TEXT)
    del stored["termination_receipt"]
    assert closed_reason(stored) == ("NO_SESSION", GONE_TEXT)


@pytest.mark.asyncio
async def test_existing_answer_writer_rejects_mismatched_stored_question_key() -> None:
    repo = MemoryRepository()
    original = question(identity="foreign")
    await repo.put("out.user_questions", "question", original)
    with pytest.raises(ValueError, match="identity"):
        await record_answer(repo, "question", "actual text", status="ANSWERED")
    assert await repo.get("out.user_questions", "question") == original
    assert await repo.list("ops.jobs") == []
