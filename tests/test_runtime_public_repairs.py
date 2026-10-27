"""Real queue effects for absent provider delay and actual admission-clock writers."""

import asyncio
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.contracts.jobs import JobEnvelope
from tank.job_repository import JobRepository
from tank.storage import StorageBackend
from tank.time_precision import collection_clock
from tank.util import parse_time, utcnow


async def wait_until(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.005)


@pytest.mark.parametrize(
    ("reported", "max_attempts", "expected_status", "delay"),
    [
        (None, 3, "QUEUED", 2),
        (7, 3, "QUEUED", 7),
        (None, 1, "FAILED", 2),
    ],
)
async def test_real_answer_error_requeues_or_finishes_using_reported_or_application_delay(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reported: int | None,
    max_attempts: int,
    expected_status: str,
    delay: int,
) -> None:
    await repo.put(
        "out.user_questions",
        "question",
        {
            "question_id": "question",
            "session_id": "session",
            "status": "ANSWERED",
            "answer_status": "ANSWERED",
            "answer_text": "Synthetic answer",
            "chat_id": "42",
            "reply_to_message_id": 17,
        },
    )
    job = await JobRepository(repo).enqueue(
        "TELEGRAM_ANSWER", {"question_id": "question"}, "answer", max_attempts=max_attempts
    )
    attempted = asyncio.Event()

    def reject(request: httpx.Request) -> httpx.Response:
        attempted.set()
        failure: dict[str, JsonValue] = {
            "ok": False,
            "error_code": 400,
            "description": "Synthetic provider rejection",
        }
        if reported is not None:
            failure["parameters"] = {"retry_after": reported}
        return httpx.Response(400, json=failure)

    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        route = router.post("https://api.telegram.org/bottest/sendMessage").mock(side_effect=reject)
        stop = asyncio.Event()
        before = utcnow()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await asyncio.wait_for(attempted.wait(), 2)
            await wait_until(lambda: job.job_id not in runtime.active_jobs)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        after = utcnow()
        current = await repo.get("ops.jobs", job.job_id)
        assert current is not None
        assert current["status"] == expected_status, (
            f"Actual worker left {current['status']} after provider delay {reported}"
        )
        assert current["attempts"] == 1 and current["last_error"] == "TelegramResponseError"
        scheduled = current["run_after"]
        assert isinstance(scheduled, str)
        assert before + timedelta(seconds=delay) <= parse_time(scheduled) <= after + timedelta(seconds=delay)
        assert route.call_count == 1
        question = await repo.get("out.user_questions", "question")
        assert question is not None
        assert question["answer_status"] == "ANSWERED" and question["answer_text"] == "Synthetic answer"
        if max_attempts == 1:
            assert current["finished_at"] is not None and question["status"] == "DELIVERY_FAILED"
        else:
            assert current["finished_at"] is None and question["status"] == "ANSWERED"


async def test_real_admission_defer_then_wake_preserves_typed_queue_contract(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        for index in range(runtime.sessions.running_session_cap()):
            session_id = f"busy-{index}"
            await repo.put(
                "ops.agent_sessions",
                session_id,
                {
                    "session_id": session_id,
                    "status": "OPEN",
                    "runtime_status": "RUNNING",
                    "proposal_id": "synthetic-busy-proposal-" + str(index),
                    "agent_version_reason": "Authored occupied slot has no observed native version",
                    "last_item_at_reason": "Authored scout slot has no source item",
                    "last_event_seq": 0,
                    **collection_clock().fields("opened_at"),
                },
            )
        jobs = JobRepository(repo)
        payload: dict[str, JsonValue] = {"kind": "NEW_ITEM", "item_id": "new-item"}
        queued = await jobs.enqueue("AGENT_MESSAGE", payload, "admission")
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            async with asyncio.timeout(2):
                while True:
                    current = await repo.get("ops.jobs", queued.job_id)
                    if (
                        current is not None
                        and current["status"] == "QUEUED"
                        and current.get("slot_waiting_since") is not None
                    ):
                        break
                    await asyncio.sleep(0.005)
        finally:
            stop.set()
            await asyncio.wait_for(worker, 2)
        deferred = await jobs.get(queued.job_id)
        assert deferred is not None and deferred.attempts == 0
        assert deferred.slot_waiting_since == deferred.created_at
        assert (await jobs.enqueue("AGENT_MESSAGE", payload, "admission")).job_id == queued.job_id
        for index in range(runtime.sessions.running_session_cap()):
            session_id = f"busy-{index}"
            current = await repo.get("ops.agent_sessions", session_id)
            assert current is not None
            current["runtime_status"] = "IDLE"
            await repo.put("ops.agent_sessions", session_id, current)
        await runtime.sessions.wake_waiting()
        reclaimed = await jobs.claim()
        assert isinstance(reclaimed, JobEnvelope) and reclaimed.job_id == queued.job_id
        assert reclaimed.attempts == 1
