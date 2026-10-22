import json
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
                200, json={"ok": True, "result": {"message_id": self.next_id, "date": 1_800_000_100}}
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
            return httpx.Response(200, json={"data": history, "next_page": None})
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
        return httpx.Response(200, json={"data": history, "next_page": None})

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
    with pytest.raises(ValueError, match="dotted"):
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
        and "/workspace/uploads/my_data.csv" in text
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
                "conditions": [{"metric": "dc.class", "op": ">", "value": "HIGH"}],
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
                    {"metric": "milestone.outcome", "op": "==", "value": "VERIFIED", "persist_count": 2}
                ],
            },
            {},
        )
