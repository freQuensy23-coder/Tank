"""Real native history, owned retry phases and locked question settlement."""

import os

import httpx
import pytest
from agent_source_fixtures import declared_versions_fixture
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue, ValidationError
from session_followup_fixtures import UnexpectedFollowupRefusals

from tank.agents.answer_settlement import AnswerHistoryPage
from tank.agents.input_ownership import NativeOwnershipSnapshot
from tank.agents.sessions import SessionService
from tank.settings import Settings
from tank.storage import MemoryRepository, Repository
from tank.time_precision import collection_clock


def question_session() -> dict[str, JsonValue]:
    return {
        "session_id": "session",
        "status": "OPEN",
        "message_log": [
            {
                "message_id": "message",
                "payload": {"kind": "USER_QUESTION", "question_id": "question", "session_id": "session"},
                "remote_event_id": "input",
                "retry_count": 0,
                "turn_status": "RUNNING",
            }
        ],
    }


def answer_history(text: str | None) -> list[dict[str, JsonValue]]:
    events: list[dict[str, JsonValue]] = [
        {"id": "input", "type": "user.message"},
        {"id": "running", "type": "session.status_running", "execution_id": "input"},
    ]
    if text is not None:
        events.append({"id": "text", "type": "agent.message", "content": [{"type": "text", "text": text}]})
    events.append(
        {
            "id": "idle",
            "type": "session.status_idle",
            "execution_id": "input",
            "stop_reason": {"type": "end_turn"},
        }
    )
    return events


def answer_service(repo: Repository, http: httpx.AsyncClient) -> SessionService:
    return SessionService(
        repo,
        Settings(openma_url="http://native", ops_progress_enabled=False, _env_file=None),
        None,
        http=http,
        versions=declared_versions_fixture(),
        refusals=UnexpectedFollowupRefusals(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("text", [None, "Actual final answer"])
async def test_completed_owned_turn_records_actual_text_or_explicit_absence_once(text: str | None) -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", question_session())
    await repo.put("out.user_questions", "question", {"question_id": "question", "session_id": "session"})
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET" and request.url.params["order"] == "desc"
        return httpx.Response(200, json={"data": answer_history(text)[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        await service.deliver_answers("session")
        await service.deliver_answers("session")
        await service.close()
    question = await repo.get("out.user_questions", "question")
    assert (
        question is not None and question["answer_status"] == "ANSWERED" and question["answer_text"] == text
    )
    session = await repo.get("ops.agent_sessions", "session")
    assert session is not None
    journal = session["message_log"]
    assert (
        isinstance(journal, list) and isinstance(journal[0], dict) and journal[0]["answer_recorded"] is True
    )
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_managed_history_keeps_zero_cursor_and_stops_at_the_actual_requested_input() -> None:
    repo = MemoryRepository()
    snapshot = NativeOwnershipSnapshot.from_record(question_session())
    history = answer_history("Final")
    pages: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page")
        pages.append(page)
        if page is None:
            return httpx.Response(200, json={"data": history[2:][::-1], "next_page": "0"})
        assert page == "0"
        return httpx.Response(200, json={"data": history[:2][::-1], "next_page": "unneeded-old-history"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        answers = await service.turn_answers(snapshot, ["input"])
        await service.close()
    assert pages == [None, "0"]
    assert answers["message"].text == "Final" and answers["message"].ended
    assert answers["message"].attempt == 0


@pytest.mark.parametrize(
    "page",
    [
        {"next_page": None},
        {"data": []},
        {"data": [], "next_page": ""},
        {"data": [], "next_page": False},
        {"data": [], "has_more": False},
        {"data": [], "next_page": None, "has_more": False},
    ],
)
def test_descending_managed_history_requires_its_actual_paging_contract(page: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        AnswerHistoryPage.from_wire(page)


@pytest.mark.asyncio
async def test_cycling_history_refuses_before_question_or_cursor_effects() -> None:
    repo = MemoryRepository()
    original = question_session()
    await repo.put("ops.agent_sessions", "session", original)
    question = {"question_id": "question", "session_id": "session"}
    await repo.put("out.user_questions", "question", question)
    calls: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        page = request.url.params.get("page")
        calls.append(page)
        return httpx.Response(200, json={"data": [], "next_page": "a" if page != "a" else "b"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        with pytest.raises(ValueError, match="advance"):
            await service.deliver_answers("session")
        await service.close()
    assert calls == [None, "a", "b"]
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.get("out.user_questions", "question") == question
    assert await repo.list("ops.jobs") == []


@pytest.mark.asyncio
async def test_a_retry_during_history_read_cannot_publish_the_previous_attempt_text() -> None:
    repo = MemoryRepository()
    original = question_session()
    await repo.put("ops.agent_sessions", "session", original)
    question = {"question_id": "question", "session_id": "session"}
    await repo.put("out.user_questions", "question", question)

    async def handler(request: httpx.Request) -> httpx.Response:
        current = await repo.get("ops.agent_sessions", "session")
        assert current is not None
        journal = current["message_log"]
        assert isinstance(journal, list) and isinstance(journal[0], dict)
        journal[0].update(
            remote_event_id="retry", retry_count=1, native_input_attempts={"input": 0, "retry": 1}
        )
        await repo.put("ops.agent_sessions", "session", current)
        return httpx.Response(
            200, json={"data": answer_history("Old attempt answer")[::-1], "next_page": None}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        await service.deliver_answers("session")
        await service.close()
    assert await repo.get("out.user_questions", "question") == question
    assert await repo.list("ops.jobs") == []
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None
    journal = current["message_log"]
    assert isinstance(journal, list) and isinstance(journal[0], dict) and "answer_recorded" not in journal[0]


@pytest.mark.asyncio
async def test_missing_question_does_not_mark_the_journal_answer_recorded() -> None:
    repo = MemoryRepository()
    original = question_session()
    await repo.put("ops.agent_sessions", "session", original)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": answer_history(None)[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        await service.deliver_answers("session")
        await service.close()
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.list("ops.jobs") == []


@pytest.mark.asyncio
async def test_uncorrelated_running_frame_cannot_inherit_another_inputs_text() -> None:
    repo = MemoryRepository()
    history = answer_history("Earlier owned text")[:-1]
    history.extend(
        [
            {"id": "unreported-running", "type": "session.status_running"},
            {
                "id": "unowned-text",
                "type": "agent.message",
                "content": [{"type": "text", "text": "Unowned text"}],
            },
            {
                "id": "idle",
                "type": "session.status_idle",
                "execution_id": "input",
                "stop_reason": {"type": "end_turn"},
            },
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": history[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        answers = await service.turn_answers(NativeOwnershipSnapshot.from_record(question_session()))
        await service.close()
    assert answers["message"].text == "Earlier owned text"
    assert answers["message"].ended


@pytest.mark.asyncio
async def test_new_running_observation_does_not_reuse_an_earlier_end_turn_as_completion() -> None:
    repo = MemoryRepository()
    history = answer_history("Earlier text")
    history.append({"id": "running-again", "type": "session.status_running", "execution_id": "input"})

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": history[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        answers = await service.turn_answers(NativeOwnershipSnapshot.from_record(question_session()))
        await service.close()
    assert answers["message"].text == "Earlier text"
    assert not answers["message"].ended


@pytest.mark.asyncio
async def test_redacted_only_completed_turn_keeps_text_genuinely_unreported() -> None:
    repo = MemoryRepository()
    history = answer_history(None)
    history.insert(2, {"id": "redacted", "type": "agent.message", "content": [{"type": "redacted"}]})

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": history[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        answers = await service.turn_answers(NativeOwnershipSnapshot.from_record(question_session()))
        await service.close()
    assert answers["message"].ended and answers["message"].text is None


@pytest.mark.asyncio
async def test_repeated_native_event_identity_with_conflicting_text_cannot_settle_a_question() -> None:
    repo = MemoryRepository()
    original = question_session()
    await repo.put("ops.agent_sessions", "session", original)
    history = answer_history("First text")[::-1]
    history.insert(
        1, {"id": "text", "type": "agent.message", "content": [{"type": "text", "text": "Conflicting text"}]}
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": history, "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        with pytest.raises(ValueError, match="changed an observed event identity"):
            await service.deliver_answers("session")
        await service.close()
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.list("ops.jobs") == []


@pytest.mark.asyncio
async def test_question_binding_changed_during_history_read_refuses_before_answer_effects() -> None:
    repo = MemoryRepository()
    await repo.put("ops.agent_sessions", "session", question_session())
    question = {"question_id": "question", "session_id": "session"}
    await repo.put("out.user_questions", "question", question)

    async def handler(request: httpx.Request) -> httpx.Response:
        current = await repo.get("ops.agent_sessions", "session")
        assert current is not None
        journal = current["message_log"]
        assert isinstance(journal, list) and isinstance(journal[0], dict)
        journal[0]["payload"] = {"kind": "USER_QUESTION", "question_id": "foreign", "session_id": "session"}
        await repo.put("ops.agent_sessions", "session", current)
        return httpx.Response(200, json={"data": answer_history("Final")[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        with pytest.raises(ValueError, match="changed its question binding"):
            await service.deliver_answers("session")
        await service.close()
    assert await repo.get("out.user_questions", "question") == question
    assert await repo.get("out.user_questions", "foreign") is None
    assert await repo.list("ops.jobs") == []


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("foreign", [False, True])
async def test_actual_postgres_settlement_rechecks_question_session_before_answer_and_delivery(
    postgres_template: PostgresTemplate, foreign: bool
) -> None:
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        await repo.migrate()
        original = question_session()
        original.update(
            proposal_id="synthetic-proposal",
            agent_version_reason="This authored session has no observed provider version",
            last_item_at_reason="This authored scout session has no source item",
            last_event_seq=0,
            **collection_clock().fields("opened_at"),
        )
        journal = original["message_log"]
        assert isinstance(journal, list) and isinstance(journal[0], dict)
        payload = journal[0]["payload"]
        assert isinstance(payload, dict)
        payload.pop("session_id")
        await repo.put("ops.agent_sessions", "session", original)
        question = {"question_id": "question", "session_id": "foreign" if foreign else "session"}
        await repo.put("out.user_questions", "question", question)

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            return httpx.Response(200, json={"data": answer_history(None)[::-1], "next_page": None})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            service = answer_service(repo, http)
            if foreign:
                with pytest.raises(ValueError, match="another session"):
                    await service.deliver_answers("session")
                assert await repo.get("ops.agent_sessions", "session") == original
                assert await repo.get("out.user_questions", "question") == question
                assert await repo.list("ops.jobs") == []
            else:
                await service.deliver_answers("session")
                await service.deliver_answers("session")
                answered = await repo.get("out.user_questions", "question")
                assert answered is not None and answered["answer_status"] == "ANSWERED"
                assert answered["answer_text"] is None
                jobs = await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})
                assert len(jobs) == 1
                assert jobs[0]["payload"] == {"question_id": "question"}
            await service.close()


@pytest.mark.asyncio
async def test_absent_journal_is_unknown_and_cannot_manufacture_an_answer_or_history_request() -> None:
    repo = MemoryRepository()
    original: dict[str, JsonValue] = {"session_id": "session", "status": "ENDED"}
    await repo.put("ops.agent_sessions", "session", original)
    snapshot = NativeOwnershipSnapshot.from_record(original)
    assert snapshot.lineages is None
    observed_empty = NativeOwnershipSnapshot.from_record({**original, "message_log": []})
    assert observed_empty.lineages == ()
    requests: list[httpx.Request] = []

    def refuse_http(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("Unobserved journal cannot authorize answer-history effects")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = answer_service(repo, http)
        await service.deliver_answers("session")
        with pytest.raises(ValueError, match="actual retained input journal"):
            await service.turn_answers(snapshot)
        await service.close()
    assert requests == []
    assert await repo.get("ops.agent_sessions", "session") == original
    assert await repo.list("ops.jobs") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("inputs", [[""], [" "]])
async def test_missing_requested_input_identity_refuses_before_native_history(inputs: list[str]) -> None:
    repo = MemoryRepository()
    requests: list[httpx.Request] = []

    def refuse_http(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("Malformed accepted-input identity cannot authorize history")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse_http)) as http:
        service = answer_service(repo, http)
        with pytest.raises(ValidationError):
            await service.turn_answers(NativeOwnershipSnapshot.from_record(question_session()), inputs)
        await service.close()
    assert requests == []


@pytest.mark.asyncio
async def test_failed_question_with_opaque_history_does_not_block_a_fresh_owned_answer() -> None:
    repo = MemoryRepository()
    original = question_session()
    journal = original["message_log"]
    assert isinstance(journal, list)
    failed: dict[str, JsonValue] = {
        "message_id": "failed-message",
        "payload": {"kind": "USER_QUESTION", "question_id": "failed-question", "session_id": "session"},
        "turn_status": "FAILED",
        "remote_event_id": {"opaque": "unused prior input"},
        "retry_count": {"opaque": "unused prior budget"},
    }
    journal.insert(0, failed)
    await repo.put("ops.agent_sessions", "session", original)
    for question_id in ("question", "failed-question"):
        await repo.put(
            "out.user_questions", question_id, {"question_id": question_id, "session_id": "session"}
        )
    historical_tool: dict[str, JsonValue] = {
        "session_id": "session",
        "remote_event_id": "old-input",
        "native_turn_message_id": "failed-message",
        "native_turn_retry_count": {"opaque": "unused prior tool budget"},
    }
    await repo.put("ops.tool_calls", "old-tool", historical_tool)
    history: list[dict[str, JsonValue]] = [
        {"id": "old-running", "type": "session.status_running", "execution_id": "old-input"},
        {
            "id": "old-text",
            "type": "agent.message",
            "content": [{"type": "text", "text": "Never use stale text"}],
        },
        *answer_history("Fresh answer"),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"data": history[::-1], "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = answer_service(repo, http)
        await service.deliver_answers("session")
        await service.close()
    failed_answer = await repo.get("out.user_questions", "failed-question")
    fresh_answer = await repo.get("out.user_questions", "question")
    assert failed_answer is not None and failed_answer["answer_status"] == "FAILED"
    assert failed_answer["answer_text"] is None
    assert fresh_answer is not None and fresh_answer["answer_text"] == "Fresh answer"
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 2
    stored = await repo.get("ops.agent_sessions", "session")
    assert stored is not None
    stored_journal = stored["message_log"]
    assert isinstance(stored_journal, list) and stored_journal[0] == {**failed, "answer_recorded": True}
    assert await repo.get("ops.tool_calls", "old-tool") == historical_tool
