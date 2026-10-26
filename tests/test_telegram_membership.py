"""Membership permission requires the requested user's actual reported role."""

import os
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.json import decode_json
from tank.contracts.telegram_wire import TelegramMembershipCache
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_api import TelegramAuthorizationUnavailable, TelegramProtocolError
from tank.telegram_inbox import TelegramInbox


class MembershipSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def preferences(tmp_path: Path, selector: str = "-1001") -> MembershipSettings:
    return MembershipSettings(
        data_dir=tmp_path, telegram_token="synthetic-private-token", telegram_alerts_chat=selector
    )


def private_message() -> dict[str, JsonValue]:
    return {
        "chat": {"id": 42, "type": "private"},
        "from": {"id": 42, "is_bot": False, "first_name": "Synthetic requester"},
        "message_id": 1,
        "date": 1800000000,
        "text": "actual question",
    }


def membership(status: str = "member", user_id: int = 42, **fields: JsonValue) -> dict[str, JsonValue]:
    return {
        "status": status,
        "user": {"id": user_id, "is_bot": False, "first_name": "Synthetic user"},
        **fields,
    }


@pytest.mark.parametrize(
    "status,permitted",
    [("creator", True), ("administrator", True), ("member", True), ("left", False), ("kicked", False)],
)
async def test_actual_known_role_is_bound_then_cached(tmp_path: Path, status: str, permitted: bool) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert decode_json(request.content) == {"chat_id": "-1001", "user_id": 42}
        return httpx.Response(200, json={"ok": True, "result": membership(status)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        assert await inbox.authorized(private_message()) is permitted
        assert await inbox.authorized(private_message()) is permitted
        cached = inbox.members[("-1001", 42)]
    assert calls == 1 and cached.observation.user.id == 42 and cached.observation.status == status


@pytest.mark.parametrize("is_member", [False, True])
async def test_restricted_role_requires_its_actual_current_member_flag(
    tmp_path: Path, is_member: bool
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": membership("restricted", is_member=is_member)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        assert await inbox.authorized(private_message()) is is_member


@pytest.mark.parametrize("status", ["member", "left"])
async def test_foreign_user_reply_cannot_grant_or_deny_requested_user(tmp_path: Path, status: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert decode_json(request.content) == {"chat_id": "-1001", "user_id": 42}
        return httpx.Response(200, json={"ok": True, "result": membership(status, user_id=99)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        with pytest.raises(TelegramProtocolError, match="another user"):
            await inbox.authorized(private_message())
    assert inbox.members == {}


@pytest.mark.parametrize(
    "result",
    [
        {"status": "member"},
        {"user": {"id": 42}},
        membership("future-role"),
        membership("restricted"),
        membership("restricted", is_member=1),
        membership("restricted", is_member=None),
        {"status": "member", "user": {"id": True}},
    ],
)
async def test_missing_or_wrong_role_facts_are_not_false_permission_decisions(
    tmp_path: Path, result: dict[str, JsonValue]
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": result})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        with pytest.raises(TelegramProtocolError):
            await inbox.authorized(private_message())
    assert inbox.members == {}


async def assert_unavailable_retains_input(
    repo: MemoryRepository | PostgresRepository, tmp_path: Path, status: int, *, foreign: bool = False
) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.path.endswith("/getChatMember")
        reply: dict[str, JsonValue] = (
            {"ok": True, "result": membership(user_id=99)}
            if foreign
            else {"ok": False, "error_code": status, "description": "Synthetic lookup unavailable"}
        )
        return httpx.Response(status, json=reply)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, preferences(tmp_path), http=http)
        await inbox.store(private_message(), 1)
        await inbox.sweep(now=1800000100)
    row = await repo.get("ops.telegram_messages", "42:1")
    assert row is not None and row["status"] == "RECEIVED" and row["attempts"] == 1
    retry_at = row["retry_at"]
    assert isinstance(retry_at, (int, float)) and not isinstance(retry_at, bool)
    expected_error = "TelegramProtocolError" if foreign else "TelegramResponseError"
    assert row["last_error"] == expected_error and retry_at > 1800000100
    assert row["message"] == private_message() and inbox.members == {} and calls == 1
    assert await repo.list("out.user_questions") == [] and await repo.list("out.deliveries") == []
    assert await repo.list("ops.jobs") == []


@pytest.mark.parametrize("status", [400, 403])
async def test_unavailable_lookup_is_retryable_not_unauthorized(tmp_path: Path, status: int) -> None:
    await assert_unavailable_retains_input(MemoryRepository(), tmp_path, status)


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("foreign", [False, True])
async def test_postgres_unavailable_lookup_keeps_durable_input_retryable(
    postgres_template: PostgresTemplate, tmp_path: Path, foreign: bool
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await assert_unavailable_retains_input(repo, tmp_path, 200 if foreign else 403, foreign=foreign)


async def test_unreported_private_sender_does_not_borrow_chat_id_or_fake_denial(tmp_path: Path) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Missing actual sender cannot issue a membership request")

    message = private_message()
    del message["from"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        with pytest.raises(TelegramAuthorizationUnavailable):
            await inbox.authorized(message)
    assert inbox.members == {}


async def test_cache_ttl_and_request_selector_scope_are_preserved(tmp_path: Path) -> None:
    requests: list[JsonValue] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(decode_json(request.content))
        role = "left" if len(requests) == 2 else "member"
        return httpx.Response(200, json={"ok": True, "result": membership(role)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        assert await inbox.authorized(private_message()) is True
        assert await inbox.authorized(private_message()) is True and len(requests) == 1
        prior = inbox.members[("-1001", 42)]
        inbox.members[("-1001", 42)] = TelegramMembershipCache(
            observation=prior.observation, expires_monotonic=0.0
        )
        assert await inbox.authorized(private_message()) is False
        inbox.settings = preferences(tmp_path, selector="-1002")
        assert await inbox.authorized(private_message()) is True
    assert requests == [
        {"chat_id": "-1001", "user_id": 42},
        {"chat_id": "-1001", "user_id": 42},
        {"chat_id": "-1002", "user_id": 42},
    ]


@pytest.mark.parametrize(
    "identity",
    [{"id": 1234}, {"id": True, "is_bot": True}, {"id": 1234, "is_bot": False}, {"id": 1234, "is_bot": 1}],
)
async def test_getme_requires_actual_bot_identity_before_cache(
    tmp_path: Path, identity: dict[str, JsonValue]
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/getMe")
        return httpx.Response(200, json={"ok": True, "result": identity})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        with pytest.raises(TelegramProtocolError):
            await inbox.bot()
    assert inbox.bot_id is None


async def test_reported_bot_identity_is_cached_without_guessing_from_secret(tmp_path: Path) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True, "result": {"id": 1234, "is_bot": True}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        assert await inbox.bot() == 1234 and await inbox.bot() == 1234
    assert calls == 1


async def test_foreign_membership_keeps_durable_input_retryable(tmp_path: Path) -> None:
    await assert_unavailable_retains_input(MemoryRepository(), tmp_path, 200, foreign=True)


@pytest.mark.parametrize("role", ["left", "kicked"])
async def test_matching_known_nonmember_is_actual_denial(tmp_path: Path, role: str) -> None:
    repo = MemoryRepository()

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/getChatMember")
        return httpx.Response(200, json={"ok": True, "result": membership(role)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, preferences(tmp_path), http=http)
        await inbox.store(private_message(), 1)
        await inbox.sweep(now=1800000100)
    row = await repo.get("ops.telegram_messages", "42:1")
    assert row is not None and row["status"] == "IGNORED" and row["reason"] == "UNAUTHORIZED"
    assert inbox.members[("-1001", 42)].observation.permitted is False
    assert await repo.list("out.user_questions") == []


async def test_optional_sender_does_not_block_configured_chat_scope(tmp_path: Path) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Configured chat scope needs no membership lookup")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(MemoryRepository(), preferences(tmp_path), http=http)
        assert await inbox.authorized({"chat": {"id": -1001, "type": "channel"}}) is True
        assert await inbox.authorized({"chat": {"id": -2001, "type": "supergroup"}}) is False
    assert inbox.members == {}
