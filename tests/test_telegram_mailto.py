import httpx
import pytest

from tank.storage import MemoryRepository
from tank.telegram_inbox import TelegramInbox
from test_telegram_answer_resume import FakeTelegram, answer_body, answered_question, inbox_settings


@pytest.mark.parametrize(
    "markdown",
    [
        "Contact [the incident owner](mailto:ops@example.org).",
        "🚀 Contact [**the incident owner**](mailto:ops@example.org?subject=NEAR).",
    ],
)
async def test_labelled_mailto_answer_preserves_the_complete_original_on_unsupported_conversion(markdown):
    repo = MemoryRepository()
    question_id = await answered_question(repo, markdown)
    telegram = FakeTelegram()

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == [answer_body(markdown, 9)]
    question = await repo.get("out.user_questions", question_id)
    assert question["status"] == "DELIVERED"
    assert question["answer_text"] == markdown
    assert question["answer_delivery_plan"] == [{"text": markdown, "entities": []}]
    ledger = await repo.list("out.deliveries", filters={"question_id": question_id})
    assert [row["text"] for row in ledger if row["status"] == "SENT"] == [markdown]

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await TelegramInbox(repo, inbox_settings(), http=http).answer({"question_id": question_id})

    assert telegram.sent == [answer_body(markdown, 9)]
    assert await repo.list("out.deliveries", filters={"question_id": question_id}) == ledger
