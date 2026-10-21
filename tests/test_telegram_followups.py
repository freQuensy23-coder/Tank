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
    def __init__(self, updates=(), member="member"):
        self.updates = list(updates)
        self.sent = []
        self.member = member
        self.next_id = 1000

    def __call__(self, request):
        method = request.url.path.rsplit("/", 1)[-1]
        body = json.loads(request.content or b"{}")
        if method == "getMe":
            return httpx.Response(200, json={"ok": True, "result": {"id": BOT}})
        if method == "getUpdates":
            ready = [row for row in self.updates if row["update_id"] >= body.get("offset", 0)]
            return httpx.Response(200, json={"ok": True, "result": ready})
        if method == "getChatMember":
            return httpx.Response(200, json={"ok": True, "result": {"status": self.member}})
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
            "reference": {"kind": "CARD", "alert_id": "a", "version": 1},
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
        content = json.loads(body["events"][0]["content"][0]["text"])
        assert content["question"]["text"] == "Почему не шорт?"
        assert "final assistant message" in content["question"]["instructions"]
        session = await repo.get("ops.agent_sessions", "ses_1")
        answers = await service.turn_answers(session)
        assert answers[session["message_log"][-1]["message_id"]] == "Потому что OI падает."
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
