"""Actual Telegram creation paths cannot overwrite or race an answered binding."""

import asyncio
import os

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.session_followups import FollowupRefusalTarget, NoSession
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_inbox import RepositoryFollowupRefusals, TelegramInbox, question_id_for


class FixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


async def assert_actual_binders_share_question_lock(repo: MemoryRepository | PostgresRepository) -> None:
    row: dict[str, JsonValue] = {
        "chat_id": "777",
        "message_id": 11,
        "user_id": "777",
        "status": "RECEIVED",
        "message": {
            "message_id": 11,
            "date": 1800000000,
            "chat": {"id": 777, "type": "private"},
            "from": {"id": 777, "is_bot": False, "first_name": "Reader"},
            "text": "Why?",
        },
    }
    target: dict[str, JsonValue] = {"session_id": "session", "text": "Prior publication"}
    question_id = question_id_for("777", 11)
    foreign: dict[str, JsonValue] = {"question_id": question_id, "session_id": "foreign", "status": "QUEUED"}
    await repo.put("ops.telegram_messages", "777:11", row)
    await repo.put("out.user_questions", question_id, foreign)
    requests: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/sendChatAction"):
            return httpx.Response(200, json={"ok": True, "result": True})
        if request.url.path.endswith("/sendMessage"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {
                        "message_id": 99,
                        "date": 1800000001,
                        "chat": {"id": 777, "type": "private"},
                        "text": "Reply",
                    },
                },
            )
        raise AssertionError("Undeclared Telegram HTTP request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        inbox = TelegramInbox(
            repo, FixtureSettings(telegram_token="synthetic-token", telegram_alerts_chat="alerts"), http=http
        )
        try:
            with pytest.raises(ValueError, match="another session"):
                await inbox.question(row, target)
            with pytest.raises(ValueError, match="another session"):
                await inbox.prompt(row, target)
            assert requests == []
            assert await repo.get("out.user_questions", question_id) == foreign
            assert await repo.get("ops.telegram_messages", "777:11") == row
            assert await repo.list("ops.jobs") == []
            assert await repo.list("ops.telegram_files") == []

            original: dict[str, JsonValue] = {
                "question_id": question_id,
                "session_id": "session",
                "status": "QUEUED",
            }
            await repo.put("out.user_questions", question_id, original)
            reached = asyncio.Event()

            async def competing_question() -> None:
                reached.set()
                await inbox.question(row, target)

            async with repo.lock("question:" + question_id):
                pending = asyncio.create_task(competing_question())
                await asyncio.wait_for(reached.wait(), timeout=2)
                assert not pending.done()
                async with repo.transaction():
                    await repo.put("out.user_questions", question_id, foreign)
            with pytest.raises(ValueError, match="another session"):
                await pending
            assert await repo.get("out.user_questions", question_id) == foreign
            assert requests == [] and await repo.list("ops.jobs") == []

            await repo.delete("out.user_questions", question_id)
            await inbox.question(row, target)
            actual = await repo.get("out.user_questions", question_id)
            assert actual is not None
            assert actual["question_id"] == question_id and actual["session_id"] == "session"
            jobs = await repo.list("ops.jobs")
            assert len(jobs) == 1 and jobs[0]["type"] == "AGENT_MESSAGE"
            assert jobs[0]["payload"] == {
                "kind": "USER_QUESTION",
                "question_id": question_id,
                "session_id": "session",
            }
            reporter = RepositoryFollowupRefusals(repo)
            await reporter.record(
                FollowupRefusalTarget(question_id=question_id, session_id="session"),
                NoSession(status="NO_SESSION", reason="NATIVE_MISSING"),
            )
            answered = await repo.get("out.user_questions", question_id)
            assert answered is not None and answered["answer_status"] == "NO_SESSION"
            before_prompt = len(requests)
            await inbox.prompt(row, target)
            assert len(requests) == before_prompt
            assert await repo.get("out.user_questions", question_id) == answered
            assert len(await repo.list("ops.jobs")) == 2
        finally:
            await inbox.close()


@pytest.mark.asyncio
async def test_actual_memory_question_and_prompt_guard_binding_before_side_effects() -> None:
    await assert_actual_binders_share_question_lock(MemoryRepository())


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_actual_postgres_question_and_prompt_guard_binding_before_side_effects(
    postgres_template: PostgresTemplate,
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await assert_actual_binders_share_question_lock(repo)
