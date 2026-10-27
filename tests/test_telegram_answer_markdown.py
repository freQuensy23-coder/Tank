import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from test_telegram_followups import USER, FakeTelegram, ask, inbox_settings

from tank.storage import MemoryRepository
from tank.telegram_inbox import ANSWER_CHANNEL, TelegramInbox

NEAR_BOLD = [
    "not about gifts or airdrops",
    "smart-contract owners",
    "Before:",
    "30% rebate",
    "After the upgrade activates:",
    "0%",
    "It does not itself increase users’ gas prices or prohibit future rewards, giveaways, or airdrops.",
    "the software has been released, but the change is not yet confirmed active on mainnet.",
]


@pytest.fixture
def near_answer():
    return (Path(__file__).parent / "fixtures" / "telegram-near-answer.md").read_text(encoding="utf-8")


def bold_texts(body):
    encoded = body["text"].encode("utf-16-le")
    result = []
    for entity in body["entities"]:
        assert set(entity) == {"type", "offset", "length"}
        assert entity["type"] == "bold"
        offset, length = entity["offset"], entity["length"]
        assert type(offset) is int and type(length) is int
        assert offset >= 0 and length > 0
        start, end = 2 * offset, 2 * (offset + length)
        assert end <= len(encoded)
        encoded[:start].decode("utf-16-le")
        encoded[end:].decode("utf-16-le")
        result.append(encoded[start:end].decode("utf-16-le"))
    return result


def assert_near_payload(body, markdown):
    expected = markdown.replace("**", "")
    assert body["text"] == expected
    assert body["text"].split("\n\n") == expected.split("\n\n")
    assert [line for line in body["text"].splitlines() if line.startswith("• ")] == [
        line.replace("**", "") for line in markdown.splitlines() if line.startswith("• ")
    ]
    assert "30% rebate" in body["text"] and "0%" in body["text"]
    assert "November 4 and 6, 2026" in body["text"]
    assert "contract’s" in body["text"] and "(“gas”)" in body["text"]
    assert "developers—not" in body["text"]
    assert "parse_mode" not in body
    assert body["chat_id"] == str(USER)
    assert body["reply_parameters"] == {"message_id": 9, "allow_sending_without_reply": True}
    assert bold_texts(body) == NEAR_BOLD


async def answered_question(repo, markdown):
    question_id = await ask(repo)
    question = await repo.get("out.user_questions", question_id)
    question.update(status="ANSWERED", answer_status="ANSWERED", answer_text=markdown)
    await repo.put("out.user_questions", question_id, question)
    return question_id


async def assert_answer_ledger(repo, question_id, bodies, markdown):
    question = await repo.get("out.user_questions", question_id)
    assert question["status"] == "DELIVERED"
    assert question["answer_status"] == "ANSWERED"
    assert question["answer_text"] == markdown
    assert question["delivered_at"]
    ledger = await repo.list(
        "out.deliveries", filters={"question_id": question_id, "channel": ANSWER_CHANNEL}
    )
    assert sorted((row["chunk"], row["status"]) for row in ledger) == [
        (index, status) for index in range(len(bodies)) for status in ("GENERATED", "SENT")
    ]
    for row in ledger:
        assert row["purpose"] == "ANSWER"
        assert row["session_id"] == "ses_1"
        assert row["chat_id"] == str(USER)
        assert row["attempt"] == 1
        assert row["chunks"] == len(bodies)
        assert row["text"] == bodies[row["chunk"]]["text"]
        if row["status"] == "SENT":
            assert row["message_ref"] == str(1001 + row["chunk"])
    return ledger


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", ["", "🚀 "])
async def test_exact_near_answer_preserves_text_and_utf16_bold_entities(near_answer, prefix):
    markdown = prefix + near_answer
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    telegram = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        inbox = TelegramInbox(repo, inbox_settings(), http=http)
        await inbox.answer({"question_id": question_id})
        [body] = telegram.sent
        assert_near_payload(body, markdown)
        first_bold = body["entities"][0]
        first_bold_index = body["text"].index(NEAR_BOLD[0])
        assert first_bold["offset"] == len(body["text"][:first_bold_index].encode("utf-16-le")) // 2
        if prefix:
            assert first_bold["offset"] > first_bold_index
        ledger = await assert_answer_ledger(repo, question_id, telegram.sent, markdown)
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
        assert telegram.sent == [body]
        assert await assert_answer_ledger(repo, question_id, telegram.sent, markdown) == ledger


@pytest.mark.asyncio
async def test_long_bold_answer_preserves_all_text_and_entities_across_chunks():
    bold = ("NEAR 30% → 0%; contract’s gas. 🚀 " * 300).rstrip()
    markdown = "🚀 **" + bold + "**"
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    telegram = FakeTelegram()
    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
        assert len(telegram.sent) > 1
        assert "".join(body["text"] for body in telegram.sent) == markdown.replace("**", "")
        assert "".join(text for body in telegram.sent for text in bold_texts(body)) == bold
        for index, body in enumerate(telegram.sent):
            assert 0 < len(body["text"].encode("utf-16-le")) // 2 <= 4000
            assert body["entities"]
            assert "parse_mode" not in body
            assert body["chat_id"] == str(USER)
            assert body["reply_parameters"] == {
                "message_id": 9 if index == 0 else 1000 + index,
                "allow_sending_without_reply": True,
            }
        bodies = list(telegram.sent)
        ledger = await assert_answer_ledger(repo, question_id, bodies, markdown)
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
        assert telegram.sent == bodies
        assert await assert_answer_ledger(repo, question_id, bodies, markdown) == ledger


@pytest.fixture
def near_answer_http_server(near_answer):
    state = {"requests": [], "errors": []}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_POST(self):
            try:
                assert self.path == "/bottest/sendMessage"
                assert self.headers.get_content_type() == "application/json"
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                state["requests"].append(body)
                assert_near_payload(body, near_answer)
            except (AssertionError, ValueError, KeyError, TypeError) as error:
                state["errors"].append(repr(error))
                status = 400
                response = {"ok": False, "description": "Loopback formatted-answer contract rejected"}
            else:
                status = 200
                response = {
                    "ok": True,
                    "result": {
                        "message_id": 1000 + len(state["requests"]),
                        "date": 1_800_000_000,
                        "chat": {"id": USER, "type": "private"},
                        "text": body["text"],
                        "entities": body["entities"],
                    },
                }
            content = json.dumps(response, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_near_formatted_answer_real_loopback_http_delivery_is_idempotent(
    near_answer, near_answer_http_server
):
    origin, state = near_answer_http_server

    class LoopbackInbox(TelegramInbox):
        def url(self, method):
            return f"{origin}/bot{self.settings.telegram_token}/{method}"

    repo = MemoryRepository()
    question_id = await answered_question(repo, near_answer)
    async with httpx.AsyncClient(trust_env=False, timeout=3) as http:
        await LoopbackInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
    assert state["errors"] == []
    [body] = state["requests"]
    assert_near_payload(body, near_answer)
    ledger = await assert_answer_ledger(repo, question_id, state["requests"], near_answer)
    async with httpx.AsyncClient(trust_env=False, timeout=3) as http:
        await LoopbackInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})
    assert state["errors"] == []
    assert state["requests"] == [body]
    assert await assert_answer_ledger(repo, question_id, state["requests"], near_answer) == ledger
