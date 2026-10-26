"""Only an actual new Telegram cache birth declares native preparation not requested."""

import hashlib
import os
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.native_files import NativeFileNotRequested
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_inbox import TelegramInbox
from tank.time_precision import propagate_timestamp


class InitializationSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


class Download:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request.method)
        if request.method == "POST":
            assert request.url.path.endswith("/getFile")
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {
                        "file_id": "download-returned",
                        "file_unique_id": "reported-unique",
                        "file_path": "docs/actual.bin",
                        "file_size": len(self.content),
                    },
                },
            )
        assert request.method == "GET"
        return httpx.Response(
            200, content=self.content, headers={"content-type": "application/x-native-fixture"}
        )


def media_record() -> dict[str, JsonValue]:
    return {
        "message_id": 1,
        "document": {
            "file_id": "download-request",
            "file_unique_id": "reported-unique",
            "file_name": "data.bin",
        },
    }


async def fresh_cache(
    repo: MemoryRepository | PostgresRepository, directory: Path, content: bytes
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    download = Download(content)
    async with httpx.AsyncClient(transport=httpx.MockTransport(download)) as http:
        inbox = TelegramInbox(
            repo,
            InitializationSettings(data_dir=directory, telegram_token="synthetic-private-token"),
            http=http,
        )
        [first] = await inbox.attachments("question", "session", media_record())
        metadata = await repo.get("ops.telegram_files", first.file_key)
        payload = await repo.get("ops.telegram_file_payloads", first.file_key)
        assert metadata is not None and payload is not None
        phase = NativeFileNotRequested.model_validate(metadata["native_preparation"])
        assert phase.recorded_at == propagate_timestamp(metadata, "stored_at")
        assert phase.scope.file_key == first.file_key
        assert (phase.scope.question_id, phase.scope.session_id) == ("question", "session")
        assert phase.scope.cached_sha256 == hashlib.sha256(content).hexdigest()
        assert phase.scope.size == len(content)
        assert phase.scope.filename == metadata["filename"] and phase.scope.mime == metadata["mime"]
        assert "versions" not in phase.model_dump()
        [again] = await inbox.attachments("question", "session", media_record())
        assert again == first and await repo.get("ops.telegram_files", first.file_key) == metadata
        assert await repo.get("ops.telegram_file_payloads", first.file_key) == payload
    assert download.calls == ["POST", "GET"]
    return metadata, payload


async def retained_cache(
    repo: MemoryRepository | PostgresRepository,
    directory: Path,
    metadata: dict[str, JsonValue],
    payload: dict[str, JsonValue],
) -> None:
    retained = {field: value for field, value in metadata.items() if field != "native_preparation"}
    key = retained["file_key"]
    assert isinstance(key, str)
    await repo.put("ops.telegram_files", key, retained)
    await repo.put("ops.telegram_file_payloads", key, payload)

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Retained verified cache reuse must not issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(
            repo, InitializationSettings(data_dir=directory, telegram_token=None), http=http
        )
        [reference] = await inbox.attachments("question", "session", media_record())
        assert reference.file_key == key
    assert await repo.get("ops.telegram_files", key) == retained
    assert await repo.get("ops.telegram_file_payloads", key) == payload
    assert "native_preparation" not in retained


@pytest.mark.parametrize("content", [b"", b"actual"])
async def test_new_cache_scope_and_clock_are_preserved_while_retained_absence_stays_unreported(
    tmp_path: Path, content: bytes
) -> None:
    original = MemoryRepository()
    metadata, payload = await fresh_cache(original, tmp_path, content)
    await retained_cache(MemoryRepository(), tmp_path / "retained", metadata, payload)
    key = metadata["file_key"]
    assert isinstance(key, str) and await original.get("ops.telegram_files", key) == metadata


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_actual_cache_birth_is_atomic_and_old_schema_reuse_never_initializes(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        metadata, payload = await fresh_cache(repo, tmp_path, b"actual")
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as retained:
        await retained_cache(retained, tmp_path / "retained", metadata, payload)
