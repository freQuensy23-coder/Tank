"""Telegram attachment admission preserves identities, header facts and bounded bytes."""

import base64
import hashlib
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.contracts.telegram_wire import TelegramMediaSelection, TelegramMimeSelection
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.telegram_api import (
    FileTooLarge,
    TelegramConfigurationError,
    TelegramProtocolError,
    TelegramResponseError,
    download_file,
    media_selection,
)
from tank.telegram_inbox import CURSOR_KEY, TelegramInbox
from tank.time_precision import collection_clock
from tank.util import sha256


class FileSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


def selection(*, size: int | None = None) -> TelegramMediaSelection:
    return TelegramMediaSelection(
        kind="document",
        file_id="download-id",
        file_unique_id="stable-id",
        filename="report.txt",
        reported_mime_type=None,
        declared_size=size,
    )


def file_info(**changes: JsonValue) -> dict[str, JsonValue]:
    return {
        "file_id": "returned-download-id",
        "file_unique_id": "stable-id",
        "file_path": "docs/actual.bin",
        **changes,
    }


class FileStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.yielded = 0
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("field", ["file_id", "file_unique_id"])
async def test_missing_required_file_identity_refuses_before_message_or_cursor_effects(
    tmp_path: Path, field: str
) -> None:
    file: dict[str, JsonValue] = {"file_id": "download-id", "file_unique_id": "stable-id"}
    del file[field]
    repo = MemoryRepository()
    message: dict[str, JsonValue] = {
        "message_id": 1,
        "date": 1800000000,
        "chat": {"id": 42, "type": "private"},
        "document": file,
    }

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/getUpdates")
        return httpx.Response(200, json={"ok": True, "result": [{"update_id": 1, "message": message}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path), http=http)
        with pytest.raises(TelegramProtocolError):
            await inbox.poll(timeout=0)
    assert await repo.list("ops.telegram_messages") == []
    assert await repo.get("ops.source_checkpoints", CURSOR_KEY) is None


def test_photo_selects_declared_dimensions_without_inventing_size_or_unique_identity() -> None:
    chosen = media_selection(
        {
            "message_id": 1,
            "photo": [
                {"file_id": "small", "file_unique_id": "s", "width": 90, "height": 90, "file_size": 100},
                {"file_id": "large", "file_unique_id": "l", "width": 1200, "height": 900},
            ],
        }
    )
    assert chosen is not None and chosen.file_id == "large" and chosen.file_unique_id == "l"
    assert chosen.declared_size is None and chosen.reported_mime_type is None
    assert chosen.filename == "photo-1"


@pytest.mark.parametrize("size", [None, -1, True, 1.0, "1"])
def test_present_declared_size_requires_actual_nonnegative_integer(size: JsonValue) -> None:
    with pytest.raises(ValueError):
        media_selection(
            {"message_id": 1, "document": {"file_id": "d", "file_unique_id": "u", "file_size": size}}
        )


async def test_unknown_size_stream_aborts_and_closes_before_retaining_over_limit_bytes() -> None:
    stream = FileStream([b"1234", b"56789", b"must-not-be-read"])
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True, "result": file_info()})
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(FileTooLarge):
            await download_file(http, token="synthetic-private-token", media=selection(), limit=8)
    assert calls == ["POST", "GET"] and stream.yielded == 2 and stream.closed


@pytest.mark.parametrize("content_type", [None, "application/x-provider-format"])
async def test_download_receipt_retains_exact_bytes_unknown_size_and_actual_header(
    content_type: str | None,
) -> None:
    content = b"\x00\xffactual-source-bytes"

    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True, "result": file_info()})
        return httpx.Response(
            200, content=content, headers={} if content_type is None else {"content-type": content_type}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        receipt = await download_file(http, token="synthetic-private-token", media=selection(), limit=100)
    assert receipt.content == content and receipt.telegram.file_size is None
    assert receipt.response_content_type == content_type
    assert receipt.telegram.file_id == "returned-download-id"


async def test_getfile_other_400_is_not_misreported_as_oversize() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400, json={"ok": False, "error_code": 400, "description": "Bad Request: wrong file identifier"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(TelegramResponseError) as error:
            await download_file(http, token="synthetic-private-token", media=selection(), limit=100)
    assert error.value.status == 400 and "synthetic-private-token" not in str(error.value)


@pytest.mark.parametrize(
    "bad_info",
    [
        {"file_id": "d", "file_path": "docs/a"},
        file_info(file_unique_id="foreign"),
        file_info(file_path="../other"),
        {"file_id": "d", "file_unique_id": "stable-id"},
    ],
)
async def test_unproven_getfile_identity_or_path_cannot_issue_download(
    bad_info: dict[str, JsonValue],
) -> None:
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(200, json={"ok": True, "result": bad_info})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(TelegramProtocolError):
            await download_file(http, token="synthetic-private-token", media=selection(), limit=100)
    assert calls == ["POST"]


async def test_ambiguous_getfile_json_is_refused_before_download() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        return httpx.Response(
            200,
            content=b'{"ok":true,"result":{"file_id":"d","file_unique_id":"foreign","file_unique_id":"stable-id","file_path":"docs/a"}}',
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(TelegramProtocolError):
            await download_file(http, token="synthetic-private-token", media=selection(), limit=100)


async def assert_attested_cache_retention(
    repo: MemoryRepository | PostgresRepository, tmp_path: Path
) -> None:
    old_key = "tgf_" + sha256({"question_id": "q", "file": "stable-id"})[:32]
    old_record: dict[str, JsonValue] = {
        "file_key": old_key,
        "question_id": "q",
        "session_id": "s",
        "filename": "old.bin",
        "mime": "application/octet-stream",
        "size": 3,
        "sha256": hashlib.sha256(b"old").hexdigest(),
        **collection_clock(datetime(2026, 10, 26, tzinfo=UTC)).fields("stored_at"),
    }
    old_payload: dict[str, JsonValue] = {
        "file_key": old_key,
        "content_base64": base64.b64encode(b"old").decode(),
    }
    await repo.put("ops.telegram_files", old_key, old_record)
    await repo.put("ops.telegram_file_payloads", old_key, old_payload)
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True, "result": file_info()})
        return httpx.Response(200, content=b"actual", headers={"content-type": "application/x-actual"})

    message: dict[str, JsonValue] = {
        "message_id": 1,
        "document": {"file_id": "download-id", "file_unique_id": "stable-id"},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        inbox = TelegramInbox(
            repo, FileSettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        [first] = await inbox.attachments("q", "s", message)
        [again] = await inbox.attachments("q", "s", message)
        cached_only = TelegramInbox(repo, FileSettings(data_dir=tmp_path, telegram_token=None), http=http)
        [without_token] = await cached_only.attachments("q", "s", message)
        assert without_token == first
        matching_file: dict[str, JsonValue] = {
            "file_id": "download-id",
            "file_unique_id": "stable-id",
            "file_size": 6,
            "mime_type": "text/x-later-claim",
            "file_name": "later-name.txt",
        }
        [matching] = await inbox.attachments("q", "s", {"message_id": 1, "document": matching_file})
        assert matching == first
        metadata_before = await repo.list("ops.telegram_files")
        payloads_before = await repo.list("ops.telegram_file_payloads")
        for current_size in (20, 0):
            with pytest.raises(ValueError, match="current declared size"):
                await inbox.attachments(
                    "q", "s", {"message_id": 1, "document": {**matching_file, "file_size": current_size}}
                )
        assert await repo.list("ops.telegram_files") == metadata_before
        assert await repo.list("ops.telegram_file_payloads") == payloads_before
        with pytest.raises(ValueError, match="ownership conflicts"):
            await inbox.attachments("q", "foreign-session", message)
    assert first == again and first.file_key != old_key and first.size == 6
    assert calls == ["POST", "GET"]
    assert await repo.get("ops.telegram_files", old_key) == old_record
    assert await repo.get("ops.telegram_file_payloads", old_key) == old_payload
    record = await repo.get("ops.telegram_files", first.file_key)
    assert record is not None and record["file_unique_id"] == "stable-id"
    assert record["declared_size"] is None and record["reported_mime_type"] is None
    assert record["mime"] == "application/x-actual" and record["mime_basis"] == "DOWNLOAD_HEADER"
    assert record["response_content_type"] == "application/x-actual"


@pytest.mark.parametrize(
    "reported,header,value,basis",
    [
        (None, None, "application/octet-stream", "STORAGE_DEFAULT"),
        ("", "", "application/octet-stream", "STORAGE_DEFAULT"),
        (None, "image/png", "image/png", "DOWNLOAD_HEADER"),
        ("text/csv", "application/octet-stream", "text/csv", "SENDER_REPORTED"),
    ],
)
def test_effective_mime_names_actual_fact_or_explicit_storage_default(
    reported: str | None, header: str | None, value: str, basis: str
) -> None:
    selected = TelegramMimeSelection.from_facts(reported, header)
    assert selected.value == value and selected.basis == basis


async def test_reported_zero_size_and_actual_empty_file_remain_known_zero() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json={"ok": True, "result": file_info(file_size=0)})
        return httpx.Response(200, content=b"")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        result = await download_file(http, token="synthetic-private-token", media=selection(size=0), limit=8)
    assert result.content == b"" and result.telegram.file_size == 0


async def test_conflicting_declared_file_sizes_refuse_before_download() -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "POST"
        return httpx.Response(200, json={"ok": True, "result": file_info(file_size=6)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(TelegramProtocolError, match="size conflicts"):
            await download_file(http, token="synthetic-private-token", media=selection(size=5), limit=100)
    assert calls == 1


async def test_attested_cache_preserves_old_unattested_bytes_and_reuses_only_verified_receipt(
    tmp_path: Path,
) -> None:
    await assert_attested_cache_retention(MemoryRepository(), tmp_path)


@pytest.mark.integration
@pytest.mark.postgres
async def test_postgres_attested_file_cache_retains_unattested_history_and_checks_ownership(
    postgres_template: PostgresTemplate, tmp_path: Path
) -> None:
    async with postgres_template.repository(os.environ["TANK_SOURCE_TEST_DATABASE_URL"]) as repo:
        await assert_attested_cache_retention(repo, tmp_path)


@pytest.mark.parametrize("token", [None, "", " \t"])
async def test_uncached_active_download_requires_real_nonblank_token_before_http(
    tmp_path: Path, token: str | None
) -> None:
    repo = MemoryRepository()

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Unconfigured download must not issue HTTP")

    message: dict[str, JsonValue] = {
        "message_id": 1,
        "document": {"file_id": "download-id", "file_unique_id": "stable-id"},
    }
    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, FileSettings(data_dir=tmp_path, telegram_token=token), http=http)
        with pytest.raises(TelegramConfigurationError, match="nonblank token"):
            await inbox.attachments("q", "s", message)
    assert await repo.list("ops.telegram_files") == []
    assert await repo.list("ops.telegram_file_payloads") == [] and await repo.list("ops.jobs") == []


async def test_direct_download_boundary_rejects_blank_typed_token_before_http() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Blank token must not issue HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        with pytest.raises(TelegramConfigurationError):
            await download_file(http, token="", media=selection(), limit=100)
