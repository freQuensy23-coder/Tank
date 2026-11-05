import asyncio
import os
import sys
from contextlib import asynccontextmanager

import httpx
import pytest
from test_session_binding_concurrency import fixture_repo
from test_session_supervisor_recovery import settings
from test_telegram_followups import FakeTelegram, ask, inbox_settings

from tank.agents.sessions import SessionService
from tank.runtime import Runtime
from tank.telegram_inbox import ANSWER_CHANNEL, EARLY_ARCHIVED_TEXT, TelegramInbox, closed_reason


@asynccontextmanager
async def preserve_repository_error(repository):
    primary = None
    try:
        async with repository as repo:
            try:
                yield repo
            except BaseException as error:
                primary = error
                raise
    except BaseException as error:
        if primary is None or error is primary:
            raise
        if isinstance(error, asyncio.CancelledError):
            error.add_note("Failure before repository cancellation")
            raise
        primary.add_note("Repository cleanup failure")
        raise primary.with_traceback(primary.__traceback__) from None


def report_cleanup_errors(primary, errors):
    cancellation = next((error for error in errors if isinstance(error, asyncio.CancelledError)), None)
    if cancellation is not None:
        if primary is not None:
            cancellation.add_note("Failure before cleanup cancellation")
        for error in errors:
            if error is not cancellation:
                cancellation.add_note("Additional cleanup failure")
        raise cancellation
    if primary is not None:
        for _ in errors:
            primary.add_note("Cleanup failure")
    elif errors:
        for _ in errors[1:]:
            errors[0].add_note("Additional cleanup failure")
        raise errors[0]


async def close_resources(resources, primary, deadline):
    errors = []
    for resource, method in resources:
        if resource is None:
            continue
        try:
            async with asyncio.timeout_at(deadline):
                await getattr(resource, method)()
        except BaseException as error:
            errors.append(error)
    report_cleanup_errors(primary, errors)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_controlled_job_reclaim_runs_worker_and_delivers_closed_question(postgres_template):
    async with asyncio.timeout(45):
        if not os.getenv("TANK_TEST_DATABASE_URL"):
            pytest.fail("TANK_TEST_DATABASE_URL is required for this PostgreSQL integration test")
        async with preserve_repository_error(fixture_repo("postgres", postgres_template)) as repo:
            service = inbox = openma_http = telegram_http = None
            try:
                telegram = FakeTelegram()
                requests = []
                followers = []
                handler_errors = []
                claims = []
                stop = asyncio.Event()

                def unexpected_openma(request):
                    requests.append((request.method, str(request.url)))
                    raise AssertionError("Unexpected OpenMA request")

                def unexpected_follower(sid):
                    followers.append(sid)
                    raise AssertionError("Unexpected follower")

                async def no_delivery():
                    return None

                async with asyncio.timeout(5):
                    question_id = await ask(repo, session_status="ENDED")
                    session = await repo.get("ops.agent_sessions", "ses_1")
                    assert closed_reason(session) == ("ARCHIVED", EARLY_ARCHIVED_TEXT)
                    question = await repo.get("out.user_questions", question_id)
                    assert question["status"] == "QUEUED" and not question.get("answer_status")
                    assert not question.get("answer_text") and not question.get("reason_text")
                    assert not await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})
                    assert not await repo.list(
                        "out.deliveries", filters={"question_id": question_id, "channel": ANSWER_CHANNEL}
                    )
                    assert telegram.sent == []
                    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
                    job = await repo.enqueue("AGENT_MESSAGE", payload, question_id, priority=0)
                    assert job["status"] == "QUEUED" and job["attempts"] == 0
                    assert job["expires_at"] is not None
                    interrupted = await repo.claim_job()
                    assert interrupted["job_id"] == job["job_id"]
                    assert interrupted["type"] == "AGENT_MESSAGE" and interrupted["payload"] == payload
                    assert interrupted["status"] == "RUNNING" and interrupted["attempts"] == 1
                    assert interrupted["expires_at"] == job["expires_at"]
                    assert await repo.recover_jobs() == 1
                    recovered = await repo.get("ops.jobs", job["job_id"])
                    assert recovered["job_id"] == job["job_id"] and recovered["payload"] == payload
                    assert recovered["status"] == "QUEUED" and recovered["attempts"] == 1
                    assert recovered["recovery_reclaim"] is True and recovered["recovery_count"] == 1
                    assert recovered["recovery_at"] and recovered["recovery_at_prec"] == "COLLECTION_ONLY"
                    assert recovered["expires_at"] == interrupted["expires_at"]

                openma_http = httpx.AsyncClient(transport=httpx.MockTransport(unexpected_openma))
                telegram_http = httpx.AsyncClient(transport=httpx.MockTransport(telegram))
                service = SessionService(repo, settings(), None, http=openma_http)
                assert service.http is openma_http and service.stream_http is openma_http
                assert not service.owns_http and not service.owns_stream_http
                service.start_stream = unexpected_follower
                inbox = TelegramInbox(repo, inbox_settings(), http=telegram_http)
                assert not inbox.owns_http

                async def send_message(body):
                    try:
                        current = await repo.get("ops.jobs", job["job_id"])
                        claims.append((current["type"], current["job_id"], current["status"], current["attempts"]))
                        assert current["payload"] == body == payload
                        assert current["recovery_reclaim"] is False and current["recovery_count"] == 1
                        return await service.send_message(body)
                    except BaseException as error:
                        handler_errors.append(error)
                        stop.set()
                        raise

                async def answer(body):
                    try:
                        rows = await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})
                        assert len(rows) == 1
                        current = rows[0]
                        claims.append((current["type"], current["job_id"], current["status"], current["attempts"]))
                        assert body == current["payload"] == {"question_id": question_id}
                        assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "DONE"
                        return await inbox.answer(body)
                    except BaseException as error:
                        handler_errors.append(error)
                        raise
                    finally:
                        stop.set()

                runtime = object.__new__(Runtime)
                runtime.repo, runtime.sessions, runtime.active_jobs = repo, service, set()
                runtime.inbox = inbox
                runtime.handlers = {"AGENT_MESSAGE": send_message, "TELEGRAM_ANSWER": answer}
                runtime.reconcile_delivery_failures = no_delivery
                async with asyncio.timeout(20):
                    await runtime.worker(stop)
                assert not handler_errors, handler_errors
                assert not requests and not followers and not service.tasks and not runtime.active_jobs
                agent_job = await repo.get("ops.jobs", job["job_id"])
                answer_jobs = await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})
                assert len(answer_jobs) == 1
                answer_job = answer_jobs[0]
                assert claims == [
                    ("AGENT_MESSAGE", job["job_id"], "RUNNING", 1),
                    ("TELEGRAM_ANSWER", answer_job["job_id"], "RUNNING", 1),
                ]
                assert agent_job["status"] == "DONE" and agent_job["payload"] == payload
                assert agent_job["attempts"] == 1 and agent_job["recovery_count"] == 1
                assert agent_job["recovery_reclaim"] is False and agent_job["expires_at"] == job["expires_at"]
                assert agent_job["last_error"] is None
                assert answer_job["status"] == "DONE" and answer_job["payload"] == {"question_id": question_id}
                assert answer_job["attempts"] == 1 and answer_job["last_error"] is None
                question = await repo.get("out.user_questions", question_id)
                assert question["status"] == "DELIVERED" and question["answer_status"] == "ARCHIVED"
                assert question["answer_text"] is None and question["reason_text"] == EARLY_ARCHIVED_TEXT
                assert len(telegram.sent) == 1
                assert telegram.sent[0]["chat_id"] == "42" and telegram.sent[0]["text"] == EARLY_ARCHIVED_TEXT
                assert telegram.sent[0]["reply_parameters"]["message_id"] == 9
                deliveries = await repo.list(
                    "out.deliveries", filters={"question_id": question_id, "channel": ANSWER_CHANNEL}
                )
                assert sorted(row["status"] for row in deliveries) == ["GENERATED", "SENT"]
                sent = [row for row in deliveries if row["status"] == "SENT"]
                assert len(sent) == 1 and sent[0]["text"] == EARLY_ARCHIVED_TEXT
                assert sent[0]["message_ref"] == str(telegram.next_id)
                assert sent[0]["purpose"] == "ANSWER" and sent[0]["chat_id"] == "42"
            finally:
                primary = sys.exc_info()[1]
                deadline = asyncio.get_running_loop().time() + 5
                await close_resources(
                    (
                        (service, "close"),
                        (inbox, "close"),
                        (telegram_http, "aclose"),
                        (openma_http, "aclose"),
                    ),
                    primary,
                    deadline,
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body_error", "exit_error", "expected"),
    [
        (None, None, None),
        ("body", None, "body"),
        ("body", "exit", "body"),
        (None, "exit", "exit"),
        ("cancel", "exit", "cancel"),
        ("body", "cancel", "cancel"),
    ],
)
async def test_preserve_repository_error(body_error, exit_error, expected):
    errors = {
        "body": AssertionError("body"),
        "exit": RuntimeError("exit"),
        "cancel": asyncio.CancelledError("cancel"),
    }
    events = []
    value = object()

    @asynccontextmanager
    async def repository():
        events.append("enter")
        try:
            yield value
        finally:
            events.append("exit")
            if exit_error:
                raise errors[exit_error]

    caught = None
    try:
        async with preserve_repository_error(repository()) as repo:
            assert repo is value
            events.append("body")
            if body_error:
                raise errors[body_error]
    except BaseException as error:
        caught = error

    assert events == ["enter", "body", "exit"]
    assert caught is (errors[expected] if expected else None)
    if body_error and exit_error:
        note = "Repository cleanup failure" if expected == body_error else "Failure before repository cancellation"
        assert caught.__notes__ == [note]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_exit", [False, True])
async def test_repository_notes_do_not_expose_exception_values(cancel_exit):
    secret = "synthetic-secret-not-a-credential"
    primary = AssertionError(secret)
    exit_error = asyncio.CancelledError(secret) if cancel_exit else RuntimeError(secret)

    @asynccontextmanager
    async def repository():
        try:
            yield
        finally:
            raise exit_error

    caught = None
    try:
        async with preserve_repository_error(repository()):
            raise primary
    except BaseException as error:
        caught = error

    correct_identity = caught is (exit_error if cancel_exit else primary)
    notes = getattr(caught, "__notes__", [])
    notes_are_safe = all(secret not in note for note in notes)
    notes_match = notes == ["Failure before repository cancellation" if cancel_exit else "Repository cleanup failure"]
    assert correct_identity
    assert notes_are_safe
    assert notes_match


@pytest.mark.parametrize("has_primary", [False, True])
def test_cleanup_notes_do_not_expose_exception_values(has_primary):
    secret = "synthetic-secret-not-a-credential"
    primary = AssertionError(secret) if has_primary else None
    errors = [RuntimeError(secret), RuntimeError(secret)]
    caught = None
    try:
        report_cleanup_errors(primary, errors)
    except BaseException as error:
        caught = error

    target = primary if has_primary else errors[0]
    correct_identity = caught is (None if has_primary else target)
    notes = getattr(target, "__notes__", [])
    notes_are_safe = all(secret not in note for note in notes)
    notes_match = notes == (["Cleanup failure", "Cleanup failure"] if has_primary else ["Additional cleanup failure"])
    assert correct_identity
    assert notes_are_safe
    assert notes_match


@pytest.mark.asyncio
async def test_repository_entry_failure_preserves_exception():
    entry_error = RuntimeError("entry")
    events = []

    @asynccontextmanager
    async def repository():
        events.append("enter")
        raise entry_error
        yield

    caught = None
    try:
        async with preserve_repository_error(repository()):
            events.append("body")
    except BaseException as error:
        caught = error

    assert caught is entry_error
    assert events == ["enter"]


@pytest.mark.asyncio
async def test_repository_suppression_is_preserved():
    body_error = RuntimeError("body")
    events = []

    @asynccontextmanager
    async def repository():
        events.append("enter")
        try:
            yield
        except RuntimeError as error:
            assert error is body_error
            events.append("suppressed")
        finally:
            events.append("exit")

    async with preserve_repository_error(repository()):
        events.append("body")
        raise body_error

    assert events == ["enter", "body", "suppressed", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("primary_kind", [None, "error", "cancel"])
@pytest.mark.parametrize("prior_failure", [False, True])
async def test_resource_cleanup_propagates_external_task_cancellation(primary_kind, prior_failure):
    secret = "synthetic-secret-not-a-credential"
    primary = {
        None: None,
        "error": AssertionError(secret),
        "cancel": asyncio.CancelledError(secret),
    }[primary_kind]
    entered = asyncio.Event()
    events = []
    delivered = []

    class Resource:
        def __init__(self, name):
            self.name = name

        async def close(self):
            events.append(self.name)
            if self.name == "first" and prior_failure:
                raise RuntimeError(secret)
            if self.name == "blocked":
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError as error:
                    delivered.append(error)
                    raise

    async def run():
        try:
            if primary is not None:
                raise primary
        finally:
            await close_resources(
                [(Resource(name), "close") for name in ("first", "blocked", "last")],
                sys.exc_info()[1],
                asyncio.get_running_loop().time() + 5,
            )

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel(secret)
    caught = None
    try:
        await task
    except BaseException as error:
        caught = error

    assert events == ["first", "blocked", "last"]
    assert len(delivered) == 1 and caught is delivered[0]
    notes = getattr(caught, "__notes__", [])
    assert notes == (["Failure before cleanup cancellation"] if primary else []) + (
        ["Additional cleanup failure"] if prior_failure else []
    )
    assert all(secret not in note for note in notes)


@pytest.mark.asyncio
@pytest.mark.parametrize("primary_kind", [None, "error", "cancel"])
async def test_resource_cleanup_timeout_is_an_ordinary_failure(primary_kind):
    primary = {
        None: None,
        "error": AssertionError("body"),
        "cancel": asyncio.CancelledError("body"),
    }[primary_kind]
    events = []

    class Resource:
        def __init__(self, name):
            self.name = name

        async def close(self):
            events.append(self.name)
            if self.name == "blocked":
                await asyncio.Event().wait()

    caught = None
    try:
        try:
            if primary is not None:
                raise primary
        finally:
            await close_resources(
                [(Resource(name), "close") for name in ("blocked", "last")],
                sys.exc_info()[1],
                asyncio.get_running_loop().time(),
            )
    except BaseException as error:
        caught = error

    assert events == ["blocked", "last"]
    if primary is None:
        assert type(caught) is TimeoutError
    else:
        assert caught is primary
        assert caught.__notes__ == ["Cleanup failure"]
