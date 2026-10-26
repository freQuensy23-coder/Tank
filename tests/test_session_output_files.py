"""Actual Session file effects require their originating question before native reads."""

from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, ValidationError
from runtime_public_fixtures import public_runtime
from test_storage import repo as repo

from tank.storage import StorageBackend
from tank.time_precision import collection_clock


def owner_snapshot(question_id: str = "question", session_id: str = "session") -> dict[str, JsonValue]:
    payload: dict[str, JsonValue] = {
        "kind": "USER_QUESTION",
        "question_id": question_id,
        "session_id": session_id,
    }
    return {
        "session_id": session_id,
        "event_id": None,
        "item_id": None,
        "proposal_id": None,
        "active_message_id": "actual-owner",
        "message_log": [{"message_id": "actual-owner", "payload": payload}],
        "last_payload": {"kind": "USER_QUESTION", "question_id": "stale-question", "session_id": session_id},
    }


async def seed_question(repo: StorageBackend, *, session_id: str = "session") -> None:
    await repo.put(
        "out.user_questions",
        "question",
        {
            "question_id": "question",
            "session_id": session_id,
            "status": "QUEUED",
            "chat_id": "123",
            "reply_to_message_id": 9,
        },
    )


@pytest.mark.parametrize("caption", [123, True, None, "x" * 1025])
async def test_invalid_caption_refuses_before_native_get_or_delivery_intent(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caption: JsonValue,
) -> None:
    await seed_question(repo)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises(ValidationError):
            await runtime.sessions.send_file(
                owner_snapshot(), {"filename": "report.pdf", "caption": caption}, "call"
            )
        assert router.calls.call_count == 0
    assert await repo.list("out.deliveries") == []


async def test_foreign_stored_question_refuses_before_native_get_or_telegram(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo, session_id="foreign-session")
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises(ValueError, match="another session"):
            await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert router.calls.call_count == 0
    assert await repo.list("out.deliveries") == []


async def test_absent_owner_cannot_recover_stale_last_payload_for_file_output(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    snapshot = owner_snapshot()
    snapshot["active_message_id"] = None
    snapshot["message_log"] = []
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises(ValueError, match="only while answering"):
            await runtime.sessions.send_file(snapshot, {"filename": "report.pdf"}, "call")
        assert router.calls.call_count == 0


async def test_actual_output_sends_opaque_bytes_to_owned_question_and_serializes_confirmed_result(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    content = b'{"opaque":1,"opaque":2}\x00'
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        native = router.get(runtime.sessions.url("sessions/session/outputs/report.pdf")).respond(
            200, content=content
        )
        telegram = router.post("https://api.telegram.org/bottest/sendDocument").respond(
            200,
            json={
                "ok": True,
                "result": {"message_id": 3001, "date": 1},
            },
        )
        result = await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "native-call")
        assert result == {"sent": True, "message_id": "3001", "filename": "report.pdf", "bytes": len(content)}
        assert native.call_count == telegram.call_count == 1
        assert content in telegram.calls[0].request.content
    rows = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(rows) == 1 and rows[0]["question_id"] == "question" and rows[0]["session_id"] == "session"


async def test_rebound_question_after_native_get_is_rechecked_before_telegram_post(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):

        async def native_response(request: httpx.Request) -> httpx.Response:
            async with repo.lock("question:question"):
                row = await repo.get("out.user_questions", "question")
                assert row is not None
                row["session_id"] = "changed-session"
                await repo.put("out.user_questions", "question", row)
            return httpx.Response(200, content=b"actual owned output")

        native = router.get(runtime.sessions.url("sessions/session/outputs/report.pdf")).mock(
            side_effect=native_response
        )
        with pytest.raises(ValueError, match="actual target"):
            await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert native.call_count == 1 and router.calls.call_count == 1
    assert await repo.list("out.deliveries") == []


async def test_known_sent_returns_cached_receipt_without_native_bytes_or_credentials(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        native = router.get(runtime.sessions.url("sessions/session/outputs/report.pdf")).respond(
            200, content=b"report"
        )
        telegram = router.post("https://api.telegram.org/bottest/sendDocument").respond(
            200,
            json={
                "ok": True,
                "result": {"message_id": 3001, "date": 1},
            },
        )
        await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        monkeypatch.setattr(runtime.settings, "telegram_token", None)
        cached = await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert cached == {"sent": True, "message_id": "3001", "duplicate": True}
        assert native.call_count == telegram.call_count == 1


async def test_unknown_earlier_delivery_returns_without_native_get_or_telegram_post(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    await repo.put(
        "out.deliveries",
        "uncertain",
        {
            "question_id": "question",
            "session_id": "session",
            "chat_id": "123",
            "channel": "telegram:answer",
            "purpose": "ATTACHMENT",
            "call_id": "call",
            "filename": "report.pdf",
            "status": "GENERATED",
            "attempt": 1,
            **collection_clock().fields("at"),
        },
        immutable=True,
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        monkeypatch.setattr(runtime.settings, "telegram_token", None)
        result = await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert result["sent"] == "UNKNOWN"
        assert router.calls.call_count == 0
    assert len(await repo.list("out.deliveries")) == 1


async def test_unbound_earlier_attempt_cannot_authorize_native_get(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    await repo.put(
        "out.deliveries",
        "legacy-unknown",
        {
            "question_id": "question",
            "chat_id": "123",
            "channel": "telegram:answer",
            "purpose": "ATTACHMENT",
            "call_id": "call",
            "filename": "report.pdf",
            "status": "GENERATED",
            "attempt": 1,
            **collection_clock().fields("at"),
        },
        immutable=True,
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        with pytest.raises(ValidationError):
            await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert router.calls.call_count == 0


async def test_fresh_unavailable_sender_refuses_before_native_get(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        monkeypatch.setattr(runtime.settings, "telegram_token", None)
        with pytest.raises(ValueError):
            await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert router.calls.call_count == 0


async def test_constructed_invalid_cached_receipt_refuses_before_any_native_read(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tank.contracts.session_followups import FollowupRefusalTarget
    from tank.contracts.telegram_state import KnownTelegramFileDelivery, TelegramFileAlreadySent

    await seed_question(repo)
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):

        async def corrupted_lookup(
            target: FollowupRefusalTarget,
            call_id: str,
            filename: str,
        ) -> KnownTelegramFileDelivery:
            return TelegramFileAlreadySent.model_construct(sent=True, message_id="0", duplicate=True)

        monkeypatch.setattr(runtime.inbox, "lookup_session_file", corrupted_lookup)
        with pytest.raises(ValidationError):
            await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        assert router.calls.call_count == 0


async def test_supplied_owner_snapshot_targets_its_question_instead_of_stale_payload(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await seed_question(repo)
    await repo.put(
        "out.user_questions",
        "stale-question",
        {
            "question_id": "stale-question",
            "session_id": "session",
            "status": "QUEUED",
            "chat_id": "456",
            "reply_to_message_id": 10,
        },
    )
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        router.get(runtime.sessions.url("sessions/session/outputs/report.pdf")).respond(
            200, content=b"owned output"
        )
        sent = router.post("https://api.telegram.org/bottest/sendDocument").respond(
            200,
            json={
                "ok": True,
                "result": {"message_id": 3002, "date": 1},
            },
        )
        await runtime.sessions.send_file(owner_snapshot(), {"filename": "report.pdf"}, "call")
        body = sent.calls[0].request.content
        assert b'name="chat_id"\r\n\r\n123\r\n' in body
        assert b'name="chat_id"\r\n\r\n456\r\n' not in body
    deliveries = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(deliveries) == 1 and deliveries[0]["question_id"] == "question"
