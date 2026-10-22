import json
import time
from types import SimpleNamespace

import httpx
import pytest

from tank.agents import SessionService
from tank.storage import MemoryRepository
from tank.telegram_inbox import (
    CARD_CHANNEL,
    FAILED_TEXT,
    NO_SESSION_TEXT,
    PROMPT_TEXT,
    TelegramInbox,
    chunks,
    question_id_for,
)

BOT = 777
ALERTS = "-100500"
USER = 42


def inbox_settings():
    return SimpleNamespace(telegram_token="test", telegram_alerts_chat=ALERTS, telegram_ops_chat="-100600")


def session_settings():
    config = {"openma": {"agent_id": "agt_1", "environment_id": "env_1", "prompt_version": "test"}}
    return SimpleNamespace(
        openma_api_key="secret",
        openma_url="http://oma",
        openma_model="gpt-6.1-sol",
        config_version="test",
        code_version="test",
        load_config=lambda name: config[name],
    )


class FakeTelegram:
    def __init__(self, updates=(), member="member", is_member=None, member_error=None):
        self.updates = list(updates)
        self.sent = []
        self.member = member
        self.is_member = is_member
        self.member_error = member_error
        self.files = {}
        self.next_id = 1000

    def __call__(self, request):
        if request.url.path.startswith("/file/"):
            return httpx.Response(200, content=self.files[request.url.path.rsplit("/", 1)[-1]])
        method = request.url.path.rsplit("/", 1)[-1]
        body = json.loads(request.content or b"{}")
        if method == "getFile":
            if body["file_id"] not in self.files:
                return httpx.Response(400, json={"ok": False, "description": "Bad Request: file is too big"})
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {
                        "file_path": "docs/" + body["file_id"],
                        "file_size": len(self.files[body["file_id"]]),
                    },
                },
            )
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": BOT}})
        if method == "getUpdates":
            ready = [row for row in self.updates if row["update_id"] >= body.get("offset", 0)]
            return httpx.Response(200, json={"ok": True, "result": ready})
        if method == "getChatMember":
            if self.member_error:
                return httpx.Response(self.member_error, json={"ok": False, "parameters": {"retry_after": 1}})
            result = {"status": self.member}
            if self.is_member is not None:
                result["is_member"] = self.is_member
            return httpx.Response(200, json={"ok": True, "result": result})
        if method == "sendChatAction":
            return httpx.Response(200, json={"ok": True, "result": True})
        if method == "sendMessage":
            self.sent.append(body)
            self.next_id += 1
            return httpx.Response(
                200, json={"ok": True, "result": {"message_id": self.next_id, "date": int(time.time())}}
            )
        raise AssertionError(method)


async def seed(repo, *, status="OPEN"):
    await repo.put(
        "out.alerts",
        "a:1",
        {"alert_id": "a", "version": 1, "event_id": "evt_1", "card_text": "<b>AAVE</b> exploit card"},
        immutable=True,
    )
    await repo.put(
        "out.deliveries",
        "d1",
        {
            "alert_id": "a",
            "version": 1,
            "channel": CARD_CHANNEL,
            "status": "SENT",
            "attempt": 1,
            "message_ref": "55",
            "telegram_date": 1_800_000_000,
            "at": "2027-01-15T08:00:00Z",
        },
        immutable=True,
    )
    await repo.put(
        "ops.agent_sessions",
        "ses_1",
        {
            "session_id": "ses_1",
            "event_id": "evt_1",
            "status": status,
            "runtime_status": "IDLE",
            "model_id": "gpt-6.1-sol",
            "prompt_version": "p" * 64,
            "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT",
            "messages": ["m0"],
            "message_log": [
                {
                    "message_id": "m0",
                    "payload": {"kind": "NEW_ITEM", "item_id": "itm_1"},
                    "remote_event_id": "in_0",
                    "turn_status": "DONE" if status != "FAILED" else "FAILED",
                }
            ],
            "last_event_seq": 0,
        },
    )


def message(message_id, *, date, text=None, chat=USER, chat_type="private", **extra):
    body = {
        "message_id": message_id,
        "date": date,
        "chat": {"id": chat, "type": chat_type},
        "from": {"id": USER, "is_bot": False},
        **extra,
    }
    if text is not None:
        body["text"] = text
    return body


async def run_inbox(repo, telegram, now):
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.poll(timeout=0)
        await inbox.sweep(now=now)


@pytest.mark.asyncio
async def test_reply_to_card_in_alerts_chat_queues_question_for_card_session():
    repo = MemoryRepository()
    await seed(repo)
    reply = message(
        9,
        date=1_800_000_050,
        text="Почему не шорт?",
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 55, "from": {"id": BOT, "is_bot": True}},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": reply}])
    await run_inbox(repo, telegram, now=1_800_000_051)
    question = await repo.get("out.user_questions", question_id_for(ALERTS, 9))
    assert question["session_id"] == "ses_1" and question["text"] == "Почему не шорт?"
    assert question["reference"]["alert_id"] == "a"
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert jobs[0]["payload"] == {
        "kind": "USER_QUESTION",
        "session_id": "ses_1",
        "question_id": question["question_id"],
    }
    assert (await repo.get("ops.source_checkpoints", "telegram:bot-updates"))["cursor"] == "2"


@pytest.mark.asyncio
async def test_forward_with_comment_pairs_and_matches_card_by_origin_date():
    repo = MemoryRepository()
    await seed(repo)
    comment = message(10, date=1_800_000_200, text="А что с ликвидациями?")
    forward = message(
        11,
        date=1_800_000_200,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT, "is_bot": True}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": comment}, {"update_id": 2, "message": forward}])
    await run_inbox(repo, telegram, now=1_800_000_201)
    question = await repo.get("out.user_questions", question_id_for(USER, 10))
    assert question["session_id"] == "ses_1"
    assert question["forwarded_message_id"] == 11
    assert (await repo.get("ops.telegram_messages", f"{USER}:11"))["status"] == "PAIRED"
    assert telegram.sent == []


@pytest.mark.asyncio
async def test_lonely_forward_waits_then_prompts_and_reply_to_prompt_continues_session():
    repo = MemoryRepository()
    await seed(repo)
    forward = message(
        12,
        date=1_800_000_300,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT, "is_bot": True}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": forward}])
    await run_inbox(repo, telegram, now=1_800_000_301)
    assert (await repo.get("ops.telegram_messages", f"{USER}:12"))["status"] == "RECEIVED"
    await run_inbox(repo, telegram, now=1_800_000_400)
    assert telegram.sent[0]["text"] == PROMPT_TEXT
    prompt_ref = 1001
    reply = message(
        13,
        date=1_800_000_410,
        text="Сколько вывели?",
        reply_to_message={"message_id": prompt_ref, "from": {"id": BOT, "is_bot": True}},
    )
    telegram.updates.append({"update_id": 2, "message": reply})
    await run_inbox(repo, telegram, now=1_800_000_411)
    assert (await repo.get("out.user_questions", question_id_for(USER, 13)))["session_id"] == "ses_1"


@pytest.mark.asyncio
async def test_private_question_from_non_member_is_ignored_and_unknown_reference_is_explained():
    repo = MemoryRepository()
    await seed(repo)
    stranger = message(
        14, date=1_800_000_500, text="?", reply_to_message={"message_id": 55, "from": {"id": BOT}}
    )
    telegram = FakeTelegram([{"update_id": 1, "message": stranger}], member="left")
    await run_inbox(repo, telegram, now=1_800_000_501)
    assert (await repo.get("ops.telegram_messages", f"{USER}:14"))["status"] == "IGNORED"
    assert not await repo.list("out.user_questions")
    unknown = message(
        15, date=1_800_000_600, text="?", reply_to_message={"message_id": 999, "from": {"id": BOT}}
    )
    member = FakeTelegram([{"update_id": 2, "message": unknown}])
    await run_inbox(repo, member, now=1_800_000_601)
    assert member.sent[0]["text"] == NO_SESSION_TEXT


def test_long_answers_split_below_telegram_limit():
    parts = chunks(("строка " * 100 + "\n") * 20)
    assert len(parts) > 1 and all(len(part) <= 4000 for part in parts)


async def ask(repo, session_status="OPEN"):
    await seed(repo, status=session_status)
    question_id = "q_1"
    await repo.put(
        "out.user_questions",
        question_id,
        {
            "question_id": question_id,
            "session_id": "ses_1",
            "event_id": "evt_1",
            "status": "QUEUED",
            "text": "Почему не шорт?",
            "chat_id": str(USER),
            "reply_to_message_id": 9,
            "reference": {"kind": "CARD", "alert_id": "a", "version": 1, "text": "Карточка AAVE " * 200},
        },
    )
    return question_id


def openma(posts, history):
    def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)
            posts.append((request.url.path, body))
            return httpx.Response(200, json={"data": [{"id": "in_q"}]})
        if request.url.path.endswith("/events"):
            ordered = history[::-1] if request.url.params.get("order") == "desc" else history
            return httpx.Response(200, json={"data": ordered, "next_page": None})
        raise AssertionError(request.url.path)

    return handler


@pytest.mark.asyncio
async def test_question_goes_into_the_same_session_and_final_turn_text_becomes_the_answer():
    repo = MemoryRepository()
    question_id = await ask(repo)
    posts = []
    history = [
        {"id": "r0", "type": "session.status_running", "execution_id": "in_0"},
        {"id": "x0", "type": "agent.message", "content": [{"type": "text", "text": "Карточка готова"}]},
        {
            "id": "i0",
            "type": "session.status_idle",
            "execution_id": "in_0",
            "stop_reason": {"type": "end_turn"},
        },
        {"id": "r1", "type": "session.status_running", "execution_id": "in_q"},
        {"id": "x1", "type": "agent.message", "content": [{"type": "text", "text": "Сначала проверю."}]},
        {"id": "x2", "type": "agent.message", "content": [{"type": "text", "text": "Потому что OI падает."}]},
        {
            "id": "i1",
            "type": "session.status_idle",
            "execution_id": "in_q",
            "stop_reason": {"type": "end_turn"},
        },
    ]
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, history))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
        await service.send_message(payload)
        path, body = posts[0]
        assert path == "/v1/sessions/ses_1/events"
        text = body["events"][0]["content"][0]["text"]
        assert "Пришёл фоллоуап через Telegram" in text and "Почему не шорт?" in text
        assert "question_id" not in text and "{" not in text
        session = await repo.get("ops.agent_sessions", "ses_1")
        answers = await service.turn_answers(session)
        assert answers[session["message_log"][-1]["message_id"]] == {
            "text": "Потому что OI падает.",
            "ended": True,
        }
        session["message_log"][-1]["turn_status"] = "DONE"
        await repo.put("ops.agent_sessions", "ses_1", session)
        await service.deliver_answers("ses_1")
        await service.deliver_answers("ses_1")
    question = await repo.get("out.user_questions", question_id)
    assert question["answer_text"] == "Потому что OI падает."
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1


@pytest.mark.asyncio
async def test_failed_session_reopens_and_retires_its_failed_turn_from_the_barrier():
    repo = MemoryRepository()
    question_id = await ask(repo, session_status="FAILED")
    posts = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        await service.send_message(
            {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
        )
        session = await repo.get("ops.agent_sessions", "ses_1")
        barrier = await service.execution_barrier(session)
    assert session["status"] == "OPEN" and session["retired_message_ids"] == ["m0"]
    assert barrier["accepted_input_ids"] == ["in_q"]
    assert len(posts) == 1


@pytest.mark.asyncio
async def test_terminated_session_answers_no_session_and_answer_job_replies_once():
    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session["termination_receipt"] = {"event_id": "t"}
    await repo.put("ops.agent_sessions", "ses_1", session)
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma([], []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        assert (
            await service.send_message(
                {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
            )
            is None
        )
    telegram = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.answer({"question_id": question_id})
        await inbox.answer({"question_id": question_id})
    assert [row["text"] for row in telegram.sent] == [NO_SESSION_TEXT]
    assert telegram.sent[0]["reply_parameters"]["message_id"] == 9
    assert FAILED_TEXT != NO_SESSION_TEXT


@pytest.mark.asyncio
async def test_session_with_delivered_card_is_kept_open_instead_of_archived():
    repo = MemoryRepository()
    await seed(repo)
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma([], []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        assert await service.followable(await repo.get("ops.agent_sessions", "ses_1"))


@pytest.mark.asyncio
async def test_archived_session_answers_archived_instead_of_posting():
    repo = MemoryRepository()
    question_id = await ask(repo, session_status="ENDED")
    posts = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma(posts, []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
        assert await service.send_message(payload) is None
    assert posts == []
    assert (await repo.get("out.user_questions", question_id))["answer_status"] == "ARCHIVED"
    assert (await repo.get("ops.agent_sessions", "ses_1"))["status"] == "ENDED"


@pytest.mark.asyncio
async def test_retried_question_is_never_retired_by_a_concurrent_failure():
    repo = MemoryRepository()
    question_id = await ask(repo)
    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    posts = []
    healthy = openma(posts, [])
    outage = {"left": 1}

    def handler(request):
        if request.method == "POST" and outage["left"]:
            outage["left"] -= 1
            return httpx.Response(503, json={})
        return healthy(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        with pytest.raises(httpx.HTTPStatusError):
            await service.send_message(payload)
        session = await repo.get("ops.agent_sessions", "ses_1")
        session["status"] = "FAILED"
        session["message_log"][0]["turn_status"] = "FAILED"
        await repo.put("ops.agent_sessions", "ses_1", session)
        await service.send_message(payload)
        session = await repo.get("ops.agent_sessions", "ses_1")
        barrier = await service.execution_barrier(session)
    assert session["status"] == "OPEN" and session["retired_message_ids"] == ["m0"]
    assert barrier["accepted_input_ids"] == ["in_q"] and len(posts) == 1


@pytest.mark.asyncio
async def test_answered_question_requeues_its_lost_delivery_job():
    from tank.telegram_inbox import record_answer

    repo = MemoryRepository()
    question_id = await ask(repo)
    question = await repo.get("out.user_questions", question_id)
    question.update(answer_status="ANSWERED", status="ANSWERED", answer_text="x")
    await repo.put("out.user_questions", question_id, question)
    await record_answer(repo, question_id, "y", status="ANSWERED")
    await record_answer(repo, question_id, "z", status="ANSWERED")
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1
    assert (await repo.get("out.user_questions", question_id))["answer_text"] == "x"


@pytest.mark.asyncio
async def test_terminal_question_job_records_a_failure_answer():
    from tank.runtime import Runtime

    repo = MemoryRepository()
    question_id = await ask(repo)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    job = {"type": "AGENT_MESSAGE", "payload": {"kind": "USER_QUESTION", "question_id": question_id}}
    await runtime.terminal_delivery_failure(job, "EXPIRED")
    assert (await repo.get("out.user_questions", question_id))["answer_status"] == "FAILED"
    assert len(await repo.list("ops.jobs", filters={"type": "TELEGRAM_ANSWER"})) == 1


@pytest.mark.asyncio
async def test_restricted_user_counts_only_while_still_a_member():
    repo = MemoryRepository()
    await seed(repo)
    question = message(
        16, date=1_800_000_700, text="?", reply_to_message={"message_id": 55, "from": {"id": BOT}}
    )
    gone = FakeTelegram([{"update_id": 1, "message": question}], member="restricted", is_member=False)
    await run_inbox(repo, gone, now=1_800_000_701)
    assert (await repo.get("ops.telegram_messages", f"{USER}:16"))["status"] == "IGNORED"
    again = message(
        17, date=1_800_000_800, text="?", reply_to_message={"message_id": 55, "from": {"id": BOT}}
    )
    present = FakeTelegram([{"update_id": 2, "message": again}], member="restricted", is_member=True)
    await run_inbox(repo, present, now=1_800_000_801)
    assert (await repo.get("ops.telegram_messages", f"{USER}:17"))["status"] != "IGNORED"


@pytest.mark.asyncio
async def test_reply_resolves_the_session_recorded_on_the_alert():
    repo = MemoryRepository()
    await seed(repo)
    alert = await repo.get("out.alerts", "a:1")
    await repo.put("out.alerts", "a:2", {**alert, "version": 2, "session_id": "ses_origin"}, immutable=True)
    await repo.put(
        "out.deliveries",
        "d2",
        {
            "alert_id": "a",
            "version": 2,
            "channel": CARD_CHANNEL,
            "status": "SENT",
            "attempt": 1,
            "message_ref": "56",
            "at": "2027-01-15T09:00:00Z",
        },
        immutable=True,
    )
    reply = message(
        18,
        date=1_800_000_900,
        text="?",
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 56, "from": {"id": BOT}},
    )
    await run_inbox(repo, FakeTelegram([{"update_id": 1, "message": reply}]), now=1_800_000_901)
    assert (await repo.get("out.user_questions", question_id_for(ALERTS, 18)))["session_id"] == "ses_origin"


@pytest.mark.asyncio
async def test_first_question_is_answered_while_a_second_is_still_running():
    repo = MemoryRepository()
    first = await ask(repo)
    await repo.put(
        "out.user_questions",
        "q_2",
        {**(await repo.get("out.user_questions", first)), "question_id": "q_2", "text": "И ещё?"},
    )
    ids = iter(["in_q", "in_q2"])
    posts = []
    history = [
        {"id": "r1", "type": "session.status_running", "execution_id": "in_q"},
        {"id": "x1", "type": "agent.message", "content": [{"type": "text", "text": "Ответ один."}]},
        {
            "id": "i1",
            "type": "session.status_idle",
            "execution_id": "in_q",
            "stop_reason": {"type": "end_turn"},
        },
        {"id": "r2", "type": "session.status_running", "execution_id": "in_q2"},
    ]

    def handler(request):
        if request.method == "POST":
            posts.append(json.loads(request.content))
            return httpx.Response(200, json={"data": [{"id": next(ids)}]})
        ordered = history[::-1] if request.url.params.get("order") == "desc" else history
        return httpx.Response(200, json={"data": ordered, "next_page": None})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.start_stream = lambda sid: None
        await service.send_message({"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": first})
        await service.send_message({"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": "q_2"})
        await service.process_event(
            "ses_1",
            {
                "id": "i1",
                "type": "session.status_idle",
                "execution_id": "in_q",
                "stop_reason": {"type": "end_turn"},
            },
        )
    assert (await repo.get("out.user_questions", first))["answer_text"] == "Ответ один."
    assert not (await repo.get("out.user_questions", "q_2")).get("answer_status")


@pytest.mark.asyncio
async def test_failed_session_without_answer_is_reconciled_after_restart():
    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    session["status"] = "FAILED"
    session["message_log"].append(
        {"message_id": "mq", "payload": payload, "turn_status": "FAILED", "remote_event_id": "in_q"}
    )
    await repo.put("ops.agent_sessions", "ses_1", session)
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma([], []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        await service.reconcile_answers()
    assert (await repo.get("out.user_questions", question_id))["answer_status"] == "FAILED"


@pytest.mark.asyncio
async def test_terminal_answer_job_is_recorded_once_and_marked_reconciled():
    from tank.runtime import Runtime

    repo = MemoryRepository()
    question_id = await ask(repo)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.delivery = None
    runtime.inbox = TelegramInbox(repo, inbox_settings(), http=httpx.AsyncClient())
    job = await repo.enqueue("TELEGRAM_ANSWER", {"question_id": question_id}, question_id)
    await repo.put("ops.jobs", job["job_id"], {**job, "status": "FAILED"})
    await runtime.reconcile_delivery_failures()
    runtime._delivery_reconciled_at = None
    await runtime.reconcile_delivery_failures()
    assert (await repo.get("out.user_questions", question_id))["status"] == "DELIVERY_FAILED"
    failures = await repo.list("out.deliveries", filters={"question_id": question_id, "status": "FAILED"})
    assert len(failures) == 1
    assert (await repo.get("ops.jobs", job["job_id"])).get("followup_failure_recorded_at")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_answer_ledger_rows_satisfy_the_delivery_schema_and_route_replies(backend, postgres_template):
    import os
    from contextlib import AsyncExitStack

    async with AsyncExitStack() as lifetime:
        if backend == "postgres":
            database_url = os.getenv("TANK_TEST_DATABASE_URL")
            if not database_url:
                pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
            repo = await lifetime.enter_async_context(postgres_template.repository(database_url))
        else:
            repo = await MemoryRepository().open()
        await repo.put(
            "out.user_questions",
            "q_pg",
            {
                "question_id": "q_pg",
                "session_id": "ses_pg",
                "status": "ANSWERED",
                "answer_status": "ANSWERED",
                "answer_text": "Ответ.",
                "chat_id": str(USER),
                "reply_to_message_id": 9,
            },
        )
        telegram = FakeTelegram()
        async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
            inbox = TelegramInbox(repo, inbox_settings(), http=http)
            await inbox.answer({"question_id": "q_pg"})
            await inbox.answer({"question_id": "q_pg"})
            target = await inbox.reply_target(USER, {"message_id": 1001})
        assert [row["text"] for row in telegram.sent] == ["Ответ."]
        assert target["session_id"] == "ses_pg" and target["kind"] == "ANSWER"
        assert (await repo.get("out.user_questions", "q_pg"))["status"] == "DELIVERED"


def test_question_message_is_plain_text_with_truncated_reference():
    from tank.telegram_inbox import question_message

    text = question_message({"text": "А дальше?", "reference": {"text": "x" * 5000}})
    assert text.count("x") == 1024 and text.index("А дальше?") > text.index("x")


@pytest.mark.asyncio
async def test_send_telegram_file_replies_with_the_session_output_once():
    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    session["message_log"].append({"message_id": "mq", "payload": payload, "turn_status": "RUNNING"})
    session["active_message_id"] = "mq"
    await repo.put("ops.agent_sessions", "ses_1", session)
    documents = []

    def handler(request):
        if request.url.host == "oma":
            assert request.url.path == "/v1/sessions/ses_1/outputs/report.pdf"
            return httpx.Response(200, content=b"%PDF-1.7 tank")
        assert request.url.path.endswith("/sendDocument")
        documents.append(request.content)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 3001, "date": 1_800_000_000}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.telegram = TelegramInbox(repo, inbox_settings(), http=http)
        first = await service.invoke_tool(session, "send_telegram_file", {"filename": "report.pdf"}, "call_1")
        again = await service.invoke_tool(session, "send_telegram_file", {"filename": "report.pdf"}, "call_1")
        target = await service.telegram.reply_target(USER, {"message_id": 3001})
    assert first["sent"] and again["duplicate"] and len(documents) == 1
    assert b"%PDF-1.7 tank" in documents[0] and b'"message_id": 9' in documents[0]
    assert target["session_id"] == "ses_1"


@pytest.mark.asyncio
async def test_send_telegram_file_outside_a_follow_up_is_refused():
    repo = MemoryRepository()
    await seed(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma([], []))) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.telegram = TelegramInbox(repo, inbox_settings(), http=http)
        with pytest.raises(ValueError, match="only while answering"):
            await service.invoke_tool(session, "send_telegram_file", {"filename": "a.pdf"}, "call_2")


@pytest.mark.asyncio
async def test_idle_session_receives_the_current_tools_once():
    repo = MemoryRepository()
    await seed(repo)
    custom = [{"name": "send_telegram_file", "description": "d", "input_schema": {"type": "object"}}]
    settings = session_settings()
    settings.read_config_json = lambda relative: custom
    requests = []

    def handler(request):
        requests.append((request.method, request.url.path, json.loads(request.content or b"{}")))
        if request.method == "GET":
            return httpx.Response(200, json={"id": "ses_1", "agent": {"id": "agt_1"}})
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, settings, None, http=http)
        await service.sync_tools("ses_1")
        await service.sync_tools("ses_1")
    posts = [row for row in requests if row[0] == "POST"]
    assert len(posts) == 1 and posts[0][1] == "/v1/sessions/ses_1"
    names = [tool.get("name") for tool in posts[0][2]["agent"]["tools"]]
    assert (
        names[-1] == "send_telegram_file"
        and posts[0][2]["agent"]["tools"][0]["type"] == "agent_toolset_20260401"
    )
    assert (await repo.get("ops.agent_sessions", "ses_1"))["tools_sha256"]


@pytest.mark.asyncio
async def test_agent_watch_rules_arm_disarm_and_survive_decision_reconciliation():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1", "lifecycle": "MONITORING"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(config_version="c", code_version="v")
    armed = await tools.set_watch_condition(
        {
            "event_id": "evt_1",
            "action": "ARM",
            "token": "bitcoin:BTC",
            "text": "Wake me if OI jumps",
            "conditions": [{"metric": "deriv.native_oi_chg_pct", "op": ">", "value": 5, "persist_count": 2}],
            "expires_in_hours": 24,
        },
        {"session_id": "ses_1"},
    )
    condition = armed["condition"]
    assert condition["role"] == "AGENT" and condition["expr"]["persist"]["count"] == 2
    await tools.reconcile_conditions(
        {"dec_id": "dec_1", "event_id": "evt_1", "token_id": "bitcoin:BTC", "conditions": []},
        [await repo.get("ana.watch_conditions", condition["cond_id"])],
        [],
        {},
    )
    assert (await repo.get("ana.watch_conditions", condition["cond_id"]))["status"] == "ARMED"
    with pytest.raises(ValueError, match="not evaluated"):
        await tools.set_watch_condition(
            {
                "event_id": "evt_1",
                "action": "ARM",
                "token": "bitcoin:BTC",
                "conditions": [{"metric": "DROP TABLE", "op": ">", "value": 1}],
            },
            {},
        )
    disarmed = await tools.set_watch_condition(
        {"event_id": "evt_1", "action": "DISARM", "cond_id": condition["cond_id"]}, {}
    )
    assert disarmed["status"] == "CANCELLED"
    assert (await repo.get("ana.watch_conditions", condition["cond_id"]))["cancelled_by"] == "AGENT"


def test_update_tools_publishes_a_new_agent_version_only_when_tools_are_missing(tmp_path):
    import importlib.util
    import shutil
    from pathlib import Path

    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("prepare_openma", root / "deploy/prepare_openma.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = tmp_path / "config"
    shutil.copytree(root / "config/skills/tank-event-analyst", config / "skills/tank-event-analyst")
    (config / "openma.yaml").write_text("agent_id: agt_1\n")
    names = [tool["name"] for tool in module.analyst_tools(config) if tool.get("type") == "custom"]
    posts = []
    state = {"tools": [{"type": "custom", "name": "get_item"}], "version": 3}

    def handler(request):
        if request.method == "POST":
            body = json.loads(request.content)
            posts.append(body)
            state.update(tools=body["tools"], version=body["version"] + 1)
        return httpx.Response(200, json={"id": "agt_1", **state})

    with httpx.Client(base_url="http://oma", transport=httpx.MockTransport(handler)) as client:
        first = module.update_tools(config, client=client)
        second = module.update_tools(config, client=client)
    assert first["status"] == "UPDATED" and "send_telegram_file" in first["added"]
    assert second == {"status": "CURRENT", "agent_version": 4}
    assert posts[0]["version"] == 3 and "set_watch_condition" in names


async def asked(repo, telegram, message_id=20, text="Почему не шорт?"):
    reply = message(
        message_id,
        date=1_800_001_000 + message_id,
        text=text,
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 55, "from": {"id": BOT}},
    )
    telegram.updates.append({"update_id": message_id, "message": reply})
    await run_inbox(repo, telegram, now=1_800_001_001 + message_id)
    return question_id_for(ALERTS, message_id)


@pytest.mark.asyncio
async def test_reply_to_the_readers_own_follow_up_continues_the_same_session():
    repo = MemoryRepository()
    await seed(repo)
    telegram = FakeTelegram()
    await asked(repo, telegram)
    again = message(
        21,
        date=1_800_001_100,
        text="И ещё вопрос",
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 20, "from": {"id": USER}},
    )
    telegram.updates.append({"update_id": 21, "message": again})
    await run_inbox(repo, telegram, now=1_800_001_101)
    question = await repo.get("out.user_questions", question_id_for(ALERTS, 21))
    assert question["session_id"] == "ses_1" and question["reference"]["kind"] == "USER_MESSAGE"
    assert question["reference"]["text"] == "Почему не шорт?"


@pytest.mark.asyncio
async def test_forward_of_own_follow_up_continues_its_session_and_hidden_senders_are_rejected():
    repo = MemoryRepository()
    await seed(repo)
    telegram = FakeTelegram()
    await asked(repo, telegram)
    comment = message(30, date=1_800_002_000, text="Уточню")
    own = message(
        31,
        date=1_800_002_000,
        text="Почему не шорт?",
        forward_origin={"type": "user", "sender_user": {"id": USER}, "date": 1_800_001_020},
    )
    hidden_comment = message(32, date=1_800_003_000, text="?")
    hidden = message(
        33,
        date=1_800_003_000,
        text="AAVE exploit card",
        forward_origin={"type": "hidden_user", "sender_user_name": "x", "date": 1_800_000_000},
    )
    telegram.updates += [
        {"update_id": 30, "message": comment},
        {"update_id": 31, "message": own},
        {"update_id": 32, "message": hidden_comment},
        {"update_id": 33, "message": hidden},
    ]
    await run_inbox(repo, telegram, now=1_800_003_001)
    assert (await repo.get("out.user_questions", question_id_for(USER, 30)))["session_id"] == "ses_1"
    assert (await repo.get("ops.telegram_messages", f"{USER}:33"))["status"] == "UNMATCHED"
    assert not await repo.get("out.user_questions", question_id_for(USER, 32))


@pytest.mark.asyncio
async def test_reader_file_is_stored_for_the_session_and_named_in_the_agent_message():
    import base64

    from tank.telegram_inbox import question_message

    repo = MemoryRepository()
    await seed(repo)
    telegram = FakeTelegram()
    telegram.files["doc_1"] = b"col\n1\n"
    upload = message(
        40,
        date=1_800_004_000,
        chat=int(ALERTS),
        chat_type="supergroup",
        caption="Посчитай по этому файлу",
        document={"file_id": "doc_1", "file_unique_id": "u1", "file_name": "my data.csv", "file_size": 6},
        reply_to_message={"message_id": 55, "from": {"id": BOT}},
    )
    telegram.updates.append({"update_id": 40, "message": upload})
    await run_inbox(repo, telegram, now=1_800_004_001)
    question = await repo.get("out.user_questions", question_id_for(ALERTS, 40))
    item = question["attachments"][0]
    stored = await repo.get("ops.telegram_files", item["file_key"])
    assert base64.b64decode(stored["content_base64"]) == b"col\n1\n" and item["filename"] == "my_data.csv"
    text = question_message(question)
    assert (
        "Посчитай по этому файлу" in text
        and item["file_key"] in text
        and f"/workspace/uploads/{item['file_key'][-8:]}-my_data.csv" in text
    )


@pytest.mark.asyncio
async def test_files_over_20_mb_are_refused_with_a_notice():
    from tank.telegram_inbox import TOO_LARGE_TEXT

    repo = MemoryRepository()
    await seed(repo)
    big = message(
        41,
        date=1_800_005_000,
        chat=int(ALERTS),
        chat_type="supergroup",
        caption="?",
        document={"file_id": "big", "file_name": "a.zip", "file_size": 21 * 1024 * 1024},
        reply_to_message={"message_id": 55, "from": {"id": BOT}},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": big}])
    await run_inbox(repo, telegram, now=1_800_005_001)
    assert telegram.sent[0]["text"] == TOO_LARGE_TEXT
    assert not await repo.get("out.user_questions", question_id_for(ALERTS, 41))


@pytest.mark.asyncio
async def test_transient_membership_failure_retries_instead_of_ignoring():
    repo = MemoryRepository()
    await seed(repo)
    question = message(
        42, date=1_800_006_000, text="?", reply_to_message={"message_id": 55, "from": {"id": BOT}}
    )
    telegram = FakeTelegram([{"update_id": 1, "message": question}], member_error=429)
    with pytest.raises(RuntimeError):
        await run_inbox(repo, telegram, now=1_800_006_001)
    assert (await repo.get("ops.telegram_messages", f"{USER}:42"))["status"] == "RECEIVED"


@pytest.mark.asyncio
async def test_numeric_watch_comparisons_need_numbers():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(config_version="c", code_version="v")
    with pytest.raises(ValueError, match="numeric"):
        await tools.set_watch_condition(
            {
                "event_id": "evt_1",
                "action": "ARM",
                "token": "bitcoin:BTC",
                "conditions": [{"metric": "deriv.native_oi_chg_pct", "op": ">", "value": "HIGH"}],
            },
            {},
        )


def test_reopening_fails_queued_follow_ups_of_the_failed_session():
    service = object.__new__(SessionService)
    service.clock_timestamp = lambda: __import__("tank.time_precision", fromlist=["x"]).collection_clock()
    session = {
        "status": "FAILED",
        "message_log": [
            {
                "message_id": "old_q",
                "payload": {"kind": "USER_QUESTION", "question_id": "q0"},
                "turn_status": "PENDING",
            },
            {
                "message_id": "new_q",
                "payload": {"kind": "USER_QUESTION", "question_id": "q1"},
                "turn_status": "ACCEPTING",
            },
        ],
    }
    service.reopen_for_question(session, {"question_id": "q1"}, "new_q")
    assert session["message_log"][0]["turn_status"] == "FAILED"
    assert session["message_log"][1]["turn_status"] == "ACCEPTING"
    assert session["retired_message_ids"] == ["old_q"] and session["status"] == "OPEN"


@pytest.mark.asyncio
async def test_unsized_file_that_telegram_refuses_is_set_aside_without_blocking_the_inbox():
    from tank.telegram_inbox import TOO_LARGE_TEXT

    repo = MemoryRepository()
    await seed(repo)
    unsized = message(
        50,
        date=1_800_007_000,
        chat=int(ALERTS),
        chat_type="supergroup",
        caption="?",
        document={"file_id": "huge", "file_name": "big.bin"},
        reply_to_message={"message_id": 55, "from": {"id": BOT}},
    )
    after = message(
        51,
        date=1_800_007_001,
        text="А так?",
        chat=int(ALERTS),
        chat_type="supergroup",
        reply_to_message={"message_id": 55, "from": {"id": BOT}},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": unsized}, {"update_id": 2, "message": after}])
    await run_inbox(repo, telegram, now=1_800_007_002)
    assert (await repo.get("ops.telegram_messages", f"{ALERTS}:50"))["reason"] == "FILE_TOO_LARGE"
    assert telegram.sent[0]["text"] == TOO_LARGE_TEXT
    assert (await repo.get("out.user_questions", question_id_for(ALERTS, 51)))["session_id"] == "ses_1"


@pytest.mark.asyncio
async def test_output_filename_is_one_encoded_path_segment():
    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    session["message_log"].append({"message_id": "mq", "payload": payload, "turn_status": "RUNNING"})
    session["active_message_id"] = "mq"
    paths = []

    def handler(request):
        if request.url.host == "oma":
            paths.append(request.url.raw_path)
            return httpx.Response(200, content=b"x")
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 3002, "date": 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.telegram = TelegramInbox(repo, inbox_settings(), http=http)
        await service.invoke_tool(session, "send_telegram_file", {"filename": "q?a#b.txt"}, "call_9")
    assert paths == [b"/v1/sessions/ses_1/outputs/q%3Fa%23b.txt"]


@pytest.mark.asyncio
async def test_fired_agent_watch_wakes_the_session_that_armed_it():
    from datetime import timedelta
    from unittest.mock import AsyncMock

    from tank.scheduler import Scheduler
    from tank.util import iso, utcnow

    repo = MemoryRepository()
    now = utcnow()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    await repo.put(
        "ana.contradictions",
        "fatal",
        {"contradiction_id": "fatal", "event_id": "evt_1", "type": "FATAL", "detected_at": iso(now)},
    )
    await repo.put(
        "ana.watch_conditions",
        "c1",
        {
            "cond_id": "c1",
            "event_id": "evt_1",
            "token_id": "bitcoin:BTC",
            "role": "AGENT",
            "agent_defined": True,
            "session_id": "ses_followup",
            "status": "ARMED",
            "expires_at": iso(now + timedelta(days=1)),
            "expr": {"metric": "event.contradiction_fatal", "window": "W1H", "op": "==", "value": True},
        },
    )
    scheduler = Scheduler(repo, SimpleNamespace(), None, SimpleNamespace(assess=AsyncMock(return_value={})))
    await scheduler.eval_conditions({"cutoff": iso(now)})
    jobs = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert (
        jobs[0]["payload"]["kind"] == "CONDITION_FIRED" and jobs[0]["payload"]["session_id"] == "ses_followup"
    )


@pytest.mark.asyncio
async def test_event_scoped_watches_refuse_persistence():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(config_version="c", code_version="v")
    with pytest.raises(ValueError, match="first match"):
        await tools.set_watch_condition(
            {
                "event_id": "evt_1",
                "action": "ARM",
                "token": "bitcoin:BTC",
                "conditions": [
                    {"metric": "event.state", "op": "==", "value": "CONFIRMED", "persist_count": 2}
                ],
            },
            {},
        )


def test_ordered_comparison_on_a_non_number_never_matches():
    from tank.scheduler import compare

    assert compare("ANALYZING", ">", 1) is False and compare("2", ">", 1) is True


@pytest.mark.asyncio
async def test_ordered_watch_on_categorical_metric_is_refused():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(config_version="c", code_version="v")
    with pytest.raises(ValueError, match="categorical"):
        await tools.set_watch_condition(
            {
                "event_id": "evt_1",
                "action": "ARM",
                "token": "bitcoin:BTC",
                "conditions": [{"metric": "event.state", "op": ">", "value": 1}],
            },
            {},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("status,raises", [(503, True), (429, True), (400, False)])
async def test_tool_sync_retries_transient_rejections_only(status, raises):
    repo = MemoryRepository()
    await seed(repo)
    settings = session_settings()
    settings.read_config_json = lambda relative: [
        {"name": "x", "description": "d", "input_schema": {"type": "object"}}
    ]

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(status, json={}))
    ) as http:
        service = SessionService(repo, settings, None, http=http)
        if raises:
            with pytest.raises(httpx.HTTPStatusError):
                await service.sync_tools("ses_1")
        else:
            await service.sync_tools("ses_1")
    assert not (await repo.get("ops.agent_sessions", "ses_1")).get("tools_sha256")


@pytest.mark.asyncio
async def test_interrupted_file_send_is_not_repeated_blindly():
    repo = MemoryRepository()
    question_id = await ask(repo)
    await repo.put(
        "out.deliveries",
        "intent",
        {
            "channel": "telegram:answer",
            "question_id": question_id,
            "call_id": "call_x",
            "status": "GENERATED",
            "attempt": 1,
            "at": "2027-01-15T08:00:00Z",
        },
        immutable=True,
    )
    telegram = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        result = await inbox.send_session_file(question_id, "call_x", "a.pdf", b"x")
    assert result["sent"] == "UNKNOWN" and telegram.sent == []


@pytest.mark.asyncio
async def test_ambiguous_forward_of_own_messages_is_refused():
    repo = MemoryRepository()
    await seed(repo)
    for message_id, session_id in ((60, "ses_1"), (61, "ses_2")):
        await repo.put(
            "ops.telegram_messages",
            f"{USER}:{message_id}",
            {
                "chat_id": str(USER),
                "message_id": message_id,
                "user_id": str(USER),
                "status": "QUESTION",
                "session_id": session_id,
                "message": message(message_id, date=1_800_008_000, text="одно и то же"),
            },
        )
    async with httpx.AsyncClient(transport=httpx.MockTransport(FakeTelegram())) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        forward = message(
            62,
            date=1_800_009_000,
            text="одно и то же",
            forward_origin={"type": "user", "sender_user": {"id": USER}, "date": 1_800_008_000},
        )
        assert await inbox.forwarded_target(forward, BOT) is None
        forward["text"] = "другое"
        assert await inbox.forwarded_target(forward, BOT) is None


@pytest.mark.asyncio
async def test_prompt_is_sent_once_even_if_marking_the_forward_fails():
    repo = MemoryRepository()
    await seed(repo)
    forward = message(
        70,
        date=1_800_010_000,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": forward}])
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.poll(timeout=0)
        row = await repo.get("ops.telegram_messages", f"{USER}:70")
        target = await inbox.forwarded_target(row["message"], BOT)
        original = inbox.mark

        async def broken(*args, **kwargs):
            raise RuntimeError("lost write")

        inbox.mark = broken
        with pytest.raises(RuntimeError):
            await inbox.prompt(row, target)
        inbox.mark = original
        await inbox.prompt(row, target)
    assert [body["text"] for body in telegram.sent] == [PROMPT_TEXT]
    assert (await repo.get("ops.telegram_messages", f"{USER}:70"))["status"] == "PROMPTED"


def test_update_tools_republishes_when_a_tool_definition_changes(tmp_path):
    import importlib.util
    import shutil
    from pathlib import Path

    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("prepare_openma", root / "deploy/prepare_openma.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = tmp_path / "config"
    shutil.copytree(root / "config/skills/tank-event-analyst", config / "skills/tank-event-analyst")
    (config / "openma.yaml").write_text("agent_id: agt_1\n")
    stale = [dict(tool) for tool in module.analyst_tools(config)]
    stale[-1] = {**stale[-1], "description": "old"}
    stale.append({"type": "custom", "name": "retired_tool", "description": "x", "input_schema": {}})
    state = {"tools": stale, "version": 7}
    posts = []

    def handler(request):
        if request.method == "POST":
            posts.append(json.loads(request.content))
            state.update(tools=posts[-1]["tools"], version=8)
        return httpx.Response(200, json={"id": "agt_1", **state})

    with httpx.Client(base_url="http://oma", transport=httpx.MockTransport(handler)) as client:
        result = module.update_tools(config, client=client)
    assert result["status"] == "UPDATED" and result["removed"] == ["retired_tool"]
    assert result["changed"] == [stale[-2]["name"]] and result["added"] == []


@pytest.mark.asyncio
async def test_two_forwards_with_one_comment_are_not_guessed():
    repo = MemoryRepository()
    await seed(repo)
    comment = message(80, date=1_800_011_000, text="Что это значит?")
    first = message(
        81,
        date=1_800_011_000,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    second = message(
        82,
        date=1_800_011_001,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram(
        [{"update_id": n, "message": m} for n, m in enumerate([comment, first, second], 1)]
    )
    await run_inbox(repo, telegram, now=1_800_011_100)
    assert not await repo.get("out.user_questions", question_id_for(USER, 80))
    assert [body["text"] for body in telegram.sent].count(PROMPT_TEXT) == 2


@pytest.mark.asyncio
async def test_prompt_is_resent_after_a_definite_telegram_rejection():
    repo = MemoryRepository()
    await seed(repo)
    forward = message(
        83,
        date=1_800_012_000,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": forward}])
    outage = {"left": 1}
    original = telegram.__call__

    def flaky(request):
        if request.url.path.endswith("/sendMessage") and outage["left"]:
            outage["left"] -= 1
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
        return original(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(flaky)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.poll(timeout=0)
        with pytest.raises(RuntimeError):
            await inbox.sweep(now=1_800_012_100)
        await inbox.sweep(now=1_800_012_101)
    assert [body["text"] for body in telegram.sent] == [PROMPT_TEXT]
    assert (await repo.get("ops.telegram_messages", f"{USER}:83"))["status"] == "PROMPTED"


@pytest.mark.asyncio
async def test_reader_files_expire_after_thirty_days():
    from datetime import timedelta

    from tank.util import iso, utcnow

    repo = MemoryRepository()
    await repo.put(
        "ops.telegram_files",
        "old",
        {"file_key": "old", "question_id": "q", "stored_at": iso(utcnow() - timedelta(days=31))},
    )
    await repo.put(
        "ops.telegram_files", "new", {"file_key": "new", "question_id": "q", "stored_at": iso(utcnow())}
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(FakeTelegram())) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        assert await inbox.prune_files() == 1
    assert await repo.get("ops.telegram_files", "new") and not await repo.get("ops.telegram_files", "old")


@pytest.mark.asyncio
async def test_watch_rules_refuse_mixed_windows_and_unevaluated_metrics():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(config_version="c", code_version="v")
    base = {"event_id": "evt_1", "action": "ARM", "token": "bitcoin:BTC"}
    with pytest.raises(ValueError, match="same window"):
        await tools.set_watch_condition(
            {
                **base,
                "conditions": [
                    {
                        "metric": "spot.cohort.cwb_buy",
                        "op": ">",
                        "value": 0.6,
                        "window": "W15M",
                        "persist_count": 2,
                    },
                    {"metric": "deriv.native_oi_chg_pct", "op": ">", "value": 3, "window": "W1H"},
                ],
            },
            {},
        )
    for metric in ("milestone.outcome", "relative.windows.EVENT.r_raw"):
        with pytest.raises(ValueError, match="not evaluated"):
            await tools.set_watch_condition(
                {**base, "conditions": [{"metric": metric, "op": "==", "value": 1}]}, {}
            )


@pytest.mark.asyncio
async def test_answer_chunks_resend_only_after_definite_rejection():
    repo = MemoryRepository()
    question_id = await ask(repo)
    question = await repo.get("out.user_questions", question_id)
    question.update(answer_status="ANSWERED", answer_text="a" * 3000 + "\n" + "b" * 3000)
    await repo.put("out.user_questions", question_id, question)
    base = {"channel": "telegram:answer", "question_id": question_id, "purpose": "ANSWER", "attempt": 1}
    await repo.put(
        "out.deliveries", "c0", {**base, "chunk": 0, "status": "GENERATED", "at": "2027-01-15T08:00:00Z"}
    )
    await repo.put(
        "out.deliveries", "c1g", {**base, "chunk": 1, "status": "GENERATED", "at": "2027-01-15T08:00:00Z"}
    )
    await repo.put(
        "out.deliveries", "c1f", {**base, "chunk": 1, "status": "FAILED", "at": "2027-01-15T08:00:01Z"}
    )
    telegram = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
    assert [body["text"][0] for body in telegram.sent] == ["b"]


@pytest.mark.asyncio
async def test_card_reply_ignores_deliveries_recorded_for_another_chat():
    repo = MemoryRepository()
    await seed(repo)
    await repo.put(
        "out.deliveries",
        "other-chat",
        {
            "alert_id": "a",
            "version": 1,
            "channel": CARD_CHANNEL,
            "status": "SENT",
            "attempt": 1,
            "message_ref": "77",
            "chat_id": "-100999",
            "at": "2027-01-15T08:00:00Z",
        },
        immutable=True,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(FakeTelegram())) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        assert await inbox.reply_target(ALERTS, {"message_id": 77}) is None
        assert (await inbox.reply_target(ALERTS, {"message_id": 55}))["session_id"] == "ses_1"


@pytest.mark.asyncio
async def test_forward_already_linked_to_a_question_is_marked_paired_without_prompt():
    repo = MemoryRepository()
    await seed(repo)
    await repo.put(
        "out.user_questions",
        "q_linked",
        {
            "question_id": "q_linked",
            "session_id": "ses_1",
            "status": "QUEUED",
            "chat_id": str(USER),
            "forwarded_message_id": 91,
            "reply_to_message_id": 90,
        },
    )
    forward = message(
        91,
        date=1_800_013_000,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": forward}])
    await run_inbox(repo, telegram, now=1_800_013_100)
    assert (await repo.get("ops.telegram_messages", f"{USER}:91"))["status"] == "PAIRED"
    assert telegram.sent == []


@pytest.mark.asyncio
async def test_abandoned_question_retires_its_unsent_intent_but_keeps_accepted_ones():
    repo = MemoryRepository()
    await seed(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session["message_log"] += [
        {
            "message_id": "m_unsent",
            "payload": {"kind": "USER_QUESTION", "question_id": "q_a"},
            "turn_status": "ACCEPTING",
        },
        {
            "message_id": "m_sent",
            "payload": {"kind": "USER_QUESTION", "question_id": "q_b"},
            "turn_status": "PENDING",
            "remote_event_id": "in_b",
        },
    ]
    await repo.put("ops.agent_sessions", "ses_1", session)
    service = SessionService(repo, session_settings(), None, http=httpx.AsyncClient())
    assert await service.abandon_question("ses_1", "q_a") is True
    assert await service.abandon_question("ses_1", "q_b") is False
    session = await repo.get("ops.agent_sessions", "ses_1")
    unsent = next(row for row in session["message_log"] if row["message_id"] == "m_unsent")
    assert unsent["turn_status"] == "FAILED" and "m_unsent" in session["retired_message_ids"]
    await service.close()


@pytest.mark.asyncio
async def test_follow_up_to_a_busy_session_with_stale_tools_is_deferred():
    from tank.delivery import DeliveryDeferred

    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session.update(runtime_status="RUNNING", active_message_id="m0")
    await repo.put("ops.agent_sessions", "ses_1", session)
    settings = session_settings()
    settings.read_config_json = lambda relative: [
        {"name": "x", "description": "d", "input_schema": {"type": "object"}}
    ]
    async with httpx.AsyncClient(transport=httpx.MockTransport(openma([], []))) as http:
        service = SessionService(repo, settings, None, http=http)
        with pytest.raises(DeliveryDeferred):
            await service.send_message(
                {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
            )


@pytest.mark.asyncio
async def test_answer_lookup_reads_history_only_back_to_the_question():
    repo = MemoryRepository()
    await seed(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session["message_log"].append(
        {
            "message_id": "mq",
            "payload": {"kind": "USER_QUESTION", "question_id": "q"},
            "remote_event_id": "in_q",
        }
    )
    history = [{"id": f"old{n}", "type": "agent.message", "content": []} for n in range(250)] + [
        {"id": "in_q", "type": "user.message"},
        {"id": "r", "type": "session.status_running", "execution_id": "in_q"},
        {"id": "x", "type": "agent.message", "content": [{"type": "text", "text": "Ответ"}]},
        {
            "id": "i",
            "type": "session.status_idle",
            "execution_id": "in_q",
            "stop_reason": {"type": "end_turn"},
        },
    ]
    pages = []

    def handler(request):
        page = int(request.url.params.get("page") or 0)
        pages.append(page)
        newest = history[::-1]
        chunk = newest[page * 100 : (page + 1) * 100]
        following = str(page + 1) if (page + 1) * 100 < len(newest) else None
        return httpx.Response(200, json={"data": chunk, "next_page": following})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        answers = await service.turn_answers(session, ["in_q"])
    assert answers["mq"] == {"text": "Ответ", "ended": True} and pages == [0]


def test_chunks_respect_telegram_utf16_length():
    from tank.telegram_inbox import utf16_length

    parts = chunks("😀" * 5000)
    assert len(parts) == 3 and all(utf16_length(part) <= 4000 for part in parts)
    assert "".join(parts) == "😀" * 5000


@pytest.mark.asyncio
async def test_oversized_session_output_is_refused_while_streaming(monkeypatch):
    import tank.agents.sessions as module

    monkeypatch.setattr(module, "DOCUMENT_LIMIT", 10)
    repo = MemoryRepository()
    question_id = await ask(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    payload = {"kind": "USER_QUESTION", "session_id": "ses_1", "question_id": question_id}
    session["message_log"].append({"message_id": "mq", "payload": payload, "turn_status": "RUNNING"})
    session["active_message_id"] = "mq"
    sent = []

    def handler(request):
        if request.url.host == "oma":
            return httpx.Response(200, content=b"x" * 100)
        sent.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), None, http=http)
        service.telegram = TelegramInbox(repo, inbox_settings(), http=http)
        with pytest.raises(ValueError, match="45 MB"):
            await service.send_file(session, {"filename": "big.bin"}, "call_big")
    assert sent == []


@pytest.mark.asyncio
async def test_forwarded_prompt_matches_by_the_text_actually_sent():
    repo = MemoryRepository()
    await seed(repo)
    forward = message(
        95,
        date=1_800_014_000,
        text="AAVE exploit card",
        forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": 1_800_000_000},
    )
    telegram = FakeTelegram([{"update_id": 1, "message": forward}])
    await run_inbox(repo, telegram, now=1_800_014_100)
    prompt = next(
        row
        for row in await repo.list("out.deliveries")
        if row.get("purpose") == "PROMPT" and row["status"] == "SENT"
    )
    assert prompt["text"] == PROMPT_TEXT and "exploit card" in prompt["reference_text"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        target = await inbox.forwarded_target(
            message(
                96,
                date=1_800_014_200,
                text=PROMPT_TEXT,
                forward_origin={"type": "user", "sender_user": {"id": BOT}, "date": prompt["telegram_date"]},
            ),
            BOT,
        )
    assert target["kind"] == "PROMPT" and target["session_id"] == "ses_1" and "exploit card" in target["text"]


@pytest.mark.asyncio
async def test_watch_token_must_exist_in_the_registry():
    from tank.event_tools import EventToolService

    repo = MemoryRepository()
    await repo.put("evt.events", "evt_1", {"event_id": "evt_1"})
    tools = object.__new__(EventToolService)
    tools.repo = repo
    tools.settings = SimpleNamespace(
        config_version="c",
        code_version="v",
        load_config=lambda name: {"tokens": [{"token_id": "bitcoin:BTC"}]},
    )
    arm = {
        "event_id": "evt_1",
        "action": "ARM",
        "conditions": [{"metric": "deriv.native_oi_chg_pct", "op": ">", "value": 3}],
    }
    with pytest.raises(ValueError, match="not a registry token"):
        await tools.set_watch_condition({**arm, "token": "bitcoin:BTX"}, {})
    assert (await tools.set_watch_condition({**arm, "token": "bitcoin:BTC"}, {}))["accepted"]


def test_captions_are_cut_by_utf16_length_and_photos_fall_back_to_dimensions():
    from tank.telegram_inbox import media, utf16_length, utf16_prefix

    assert utf16_length(utf16_prefix("😀" * 900, 1024)) == 1024
    photo = media(
        {
            "message_id": 1,
            "photo": [
                {"file_id": "thumb", "width": 90, "height": 90},
                {"file_id": "full", "width": 1280, "height": 960},
            ],
        }
    )
    assert photo["file_id"] == "full"


@pytest.mark.asyncio
async def test_notice_is_retried_after_a_transient_failure_and_sent_once():
    from tank.telegram_inbox import HINT_TEXT

    repo = MemoryRepository()
    await seed(repo)
    lonely = message(97, date=1_800_015_000, text="просто текст")
    telegram = FakeTelegram([{"update_id": 1, "message": lonely}])
    outage = {"left": 1}
    original = telegram.__call__

    def flaky(request):
        if request.url.path.endswith("/sendMessage") and outage["left"]:
            outage["left"] -= 1
            return httpx.Response(503, json={"ok": False})
        return original(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(flaky)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.poll(timeout=0)
        with pytest.raises(RuntimeError):
            await inbox.sweep(now=1_800_015_100)
        assert (await repo.get("ops.telegram_messages", f"{USER}:97"))["status"] == "RECEIVED"
        await inbox.sweep(now=1_800_015_101)
        await inbox.sweep(now=1_800_015_102)
    assert [body["text"] for body in telegram.sent] == [HINT_TEXT]
    assert (await repo.get("ops.telegram_messages", f"{USER}:97"))["status"] == "IGNORED"


@pytest.mark.asyncio
async def test_watch_mutation_hydrates_prompt_provenance_first():
    repo = MemoryRepository()
    await seed(repo)
    session = await repo.get("ops.agent_sessions", "ses_1")
    session.pop("prompt_version_basis")
    session["prompt_version"] = "legacy-label"
    await repo.put("ops.agent_sessions", "ses_1", session)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "id": "ses_1",
                "agent": {"id": "agt_1", "system": "s", "skills": [], "tools": [], "model": {"id": "m"}},
            },
        )

    async def dispatch(tool, arguments, context):
        return {"tool": tool, "prompt_version": context["prompt_version"]}

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        service = SessionService(repo, session_settings(), dispatch, http=http)
        result = await service.invoke_tool(session, "set_watch_condition", {"event_id": "evt_1"}, "call_w")
    assert calls == ["/v1/sessions/ses_1"] and len(result["prompt_version"]) == 64
