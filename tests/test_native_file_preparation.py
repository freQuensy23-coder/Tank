"""Real HTTP and durable cache phases prevent blind native file reposts."""

import base64
import hashlib
import json
from collections.abc import Mapping

import httpx
import pytest
from pydantic import JsonValue

from tank.agents.native_file_preparation import (
    NativeFilePreparationService,
    NativeFileTransport,
    NativePreparationUnavailable,
    PreparationRecord,
    PreparationStore,
    source_scope,
)
from tank.contracts.native_files import NativeFileNotRequested
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.telegram_wire import TelegramCachedFile
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import JsonInput


def cached_file() -> TelegramCachedFile:
    return TelegramCachedFile(
        file_key="key",
        question_id="question",
        session_id="session",
        filename="data.csv",
        mime="text/csv",
        size=3,
        file_unique_id="unique",
        file_identity_basis="TELEGRAM_FILE_UNIQUE_ID",
        sha256=hashlib.sha256(b"a\nb").hexdigest(),
        declared_size=3,
        reported_mime_type="text/csv",
        mime_basis="SENDER_REPORTED",
        response_content_type=None,
    )


async def seed_file(repo: PreparationStore) -> TelegramCachedFile:
    metadata = cached_file()
    stamp = collection_clock()
    record = metadata.model_dump(mode="json")
    record.update(stamp.fields("stored_at"))
    record["native_preparation"] = PreparationRecord(
        NativeFileNotRequested(
            phase="NOT_REQUESTED",
            scope=source_scope(metadata),
            recorded_at=stamp,
        )
    ).model_dump(mode="json")
    await repo.put("ops.telegram_files", metadata.file_key, record)
    await repo.put(
        "ops.telegram_file_payloads",
        metadata.file_key,
        {
            "file_key": metadata.file_key,
            "content_base64": base64.b64encode(b"a\nb").decode(),
        },
    )
    return metadata


class NativeFiles:
    def __init__(self, *, lose_upload: bool = False, lose_mount: bool = False) -> None:
        self.uploads = 0
        self.mounts = 0
        self.resources: list[dict[str, str]] = []
        self.lose_upload = lose_upload
        self.lose_mount = lose_mount
        self.metadata_reads = 0
        self.metadata_override: dict[str, JsonValue] | None = None

    def respond(self, request: httpx.Request) -> httpx.Response:
        assert "Idempotency-Key" not in request.headers
        if request.url.path == "/v1/files":
            assert request.headers["anthropic-beta"] == "files-api-2025-04-14"
            self.uploads += 1
            if self.lose_upload:
                raise httpx.ReadError("lost upload response", request=request)
            return httpx.Response(200, json={"id": "file-native"})
        if request.url.path == "/v1/files/file-native":
            assert request.headers["anthropic-beta"] == "files-api-2025-04-14"
            self.metadata_reads += 1
            metadata: dict[str, JsonValue] = {
                "id": "file-native",
                "type": "file",
                "filename": "data.csv",
                "mime_type": "text/csv",
                "size_bytes": 3,
                "created_at": "2026-10-26T00:00:00Z",
                "scope": None,
            }
            return httpx.Response(
                200, json=self.metadata_override if self.metadata_override is not None else metadata
            )
        assert request.url.path == "/v1/sessions/session/resources"
        assert request.headers["anthropic-beta"] == "managed-agents-2026-04-01"
        if request.method == "GET":
            return httpx.Response(200, json={"data": self.resources, "next_page": None})
        self.mounts += 1
        body = json.loads(request.content)
        resource = {
            "type": "file",
            "id": "resource-native",
            "file_id": body["file_id"],
            "mount_path": body["mount_path"],
            "created_at": "2026-10-26T00:00:00Z",
            "updated_at": "2026-10-26T00:00:00Z",
        }
        self.resources.append(resource)
        if self.lose_mount:
            raise httpx.ReadError("lost mount response", request=request)
        return httpx.Response(200, json=resource)


def service(repo: PreparationStore, http: httpx.AsyncClient) -> NativeFilePreparationService:
    return NativeFilePreparationService(
        repo,
        NativeFileTransport(http, "https://native.invalid", None),
        ResolvedVersionIdentity(config_version="fixture-config", code_version="fixture-code"),
    )


async def test_actual_upload_and_scoped_resource_readback_are_once_only() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        prepared = service(repo, http)
        path = await prepared.prepare(source_scope(metadata))
        assert await prepared.prepare(source_scope(metadata)) == path
    assert native.uploads == native.mounts == 1
    stored = await repo.get("ops.telegram_files", metadata.file_key)
    assert stored is not None
    phase = PreparationRecord.model_validate(stored["native_preparation"]).root
    assert phase.phase == "MOUNT_CONFIRMED"
    assert stored["sha256"] == metadata.sha256 and "model_mounts" not in stored


async def test_lost_upload_response_durably_refuses_blind_repost_on_restart() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles(lose_upload=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(httpx.ReadError):
            await service(repo, http).prepare(source_scope(metadata))
        with pytest.raises(NativePreparationUnavailable, match="automatic repost refused"):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == 1 and native.mounts == 0


async def test_lost_mount_response_reconciles_actual_resource_without_second_post() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles(lose_mount=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(httpx.ReadError):
            await service(repo, http).prepare(source_scope(metadata))
        path = await service(repo, http).prepare(source_scope(metadata))
    assert path == native.resources[0]["mount_path"]
    assert native.uploads == native.mounts == 1


async def test_unreported_legacy_preparation_never_becomes_fresh_upload() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    record = await repo.get("ops.telegram_files", metadata.file_key)
    assert record is not None
    del record["native_preparation"]
    await repo.put("ops.telegram_files", metadata.file_key, record)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(NativePreparationUnavailable, match="unreported"):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 0


class FailingReceiptStore(MemoryRepository):
    def __init__(self, fail_phase: str, *, commit_first: bool = False) -> None:
        super().__init__()
        self.fail_phase = fail_phase
        self.commit_first = commit_first
        self.failed = False

    async def put(
        self,
        collection: str,
        key: str,
        record: Mapping[str, JsonInput],
        *,
        immutable: bool = False,
    ) -> dict[str, JsonValue]:
        phase = record.get("native_preparation")
        fail = not self.failed and isinstance(phase, dict) and phase.get("phase") == self.fail_phase
        if fail:
            self.failed = True
            if self.commit_first:
                await super().put(collection, key, record, immutable=immutable)
            raise ConnectionError("authored receipt storage failure")
        return await super().put(collection, key, record, immutable=immutable)


async def test_upload_intent_persistence_failure_precedes_every_native_effect() -> None:
    repo = FailingReceiptStore("UPLOAD_UNCONFIRMED")
    metadata = await seed_file(repo)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ConnectionError):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 0
    stored = await repo.get("ops.telegram_files", metadata.file_key)
    assert stored is not None
    assert PreparationRecord.model_validate(stored["native_preparation"]).root.phase == "NOT_REQUESTED"


async def test_lost_file_receipt_commit_ack_recovers_observed_id_without_upload() -> None:
    repo = FailingReceiptStore("FILE_ID_OBSERVED", commit_first=True)
    metadata = await seed_file(repo)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ConnectionError):
            await service(repo, http).prepare(source_scope(metadata))
        await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 1


async def test_durable_effect_receipt_recovers_uncommitted_cache_phase() -> None:
    repo = FailingReceiptStore("FILE_ID_OBSERVED")
    metadata = await seed_file(repo)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ConnectionError):
            await service(repo, http).prepare(source_scope(metadata))
        await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 1


async def test_foreign_resource_at_mount_path_refuses_without_reposting() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles(lose_mount=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(httpx.ReadError):
            await service(repo, http).prepare(source_scope(metadata))
        native.resources[0]["file_id"] = "foreign-file"
        with pytest.raises(NativePreparationUnavailable, match="different resource"):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 1


async def test_question_binding_and_all_attachment_facts_admit_before_native_effects() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    from tank.contracts.session_followups import FollowupRefusalTarget

    target = FollowupRefusalTarget(question_id="question", session_id="session")
    question: dict[str, JsonValue] = {
        "question_id": "question",
        "session_id": "foreign",
        "text": "actual question",
        "reference": None,
        "attachments": [
            {
                "file_key": metadata.file_key,
                "filename": metadata.filename,
                "size": metadata.size,
                "mime": metadata.mime,
            }
        ],
    }
    await repo.put("out.user_questions", "question", question)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ValueError, match="another session"):
            await service(repo, http).prepare_question(target)
        row = await repo.get("out.user_questions", "question")
        assert row is not None
        row["session_id"] = "session"
        row["attachments"] = [{"filename": metadata.filename, "size": metadata.size}]
        await repo.put("out.user_questions", "question", row)
        with pytest.raises(ValueError):
            await service(repo, http).prepare_question(target)
    assert native.uploads == native.mounts == 0


async def test_guarded_question_producer_returns_paths_from_confirmed_resource_only() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    from tank.contracts.session_followups import FollowupRefusalTarget

    target = FollowupRefusalTarget(question_id="question", session_id="session")
    question: dict[str, JsonValue] = {
        "question_id": "question",
        "session_id": "session",
        "text": "actual question",
        "reference": None,
        "attachments": [
            {
                "file_key": metadata.file_key,
                "filename": metadata.filename,
                "size": metadata.size,
                "mime": metadata.mime,
            }
        ],
    }
    await repo.put("out.user_questions", "question", question)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        prompt = await service(repo, http).prepare_question(target)
    assert prompt.attachments is not None and len(prompt.attachments) == 1
    assert prompt.attachments[0].path == native.resources[0]["mount_path"]
    assert prompt.text == "actual question"


async def test_explicit_null_effect_ledger_refuses_before_native_effect() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    row = await repo.get("ops.telegram_files", metadata.file_key)
    assert row is not None
    row["native_effect_receipts"] = None
    await repo.put("ops.telegram_files", metadata.file_key, row)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ValueError):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 0


async def test_retained_mount_pointer_requires_actual_scoped_resource_and_preserves_history() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    stored = await repo.get("ops.telegram_files", metadata.file_key)
    assert stored is not None
    stored.pop("native_preparation")
    pointers: dict[str, JsonValue] = {
        "session": {"file_id": "retained-file", "path": "/workspace/retained/data.csv"},
        "other-session": {"file_id": "other-file", "path": "/workspace/other.csv"},
    }
    stored["model_mounts"] = pointers
    await repo.put("ops.telegram_files", metadata.file_key, stored)
    native = NativeFiles()
    native.resources.append(
        {
            "id": "retained-resource",
            "type": "file",
            "file_id": "retained-file",
            "mount_path": "/workspace/retained/data.csv",
            "created_at": "2026-10-26T00:00:00Z",
            "updated_at": "2026-10-26T00:00:00Z",
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        assert await service(repo, http).prepare(source_scope(metadata)) == "/workspace/retained/data.csv"
        assert await service(repo, http).prepare(source_scope(metadata)) == "/workspace/retained/data.csv"
    after = await repo.get("ops.telegram_files", metadata.file_key)
    assert after is not None and after["model_mounts"] == pointers
    assert (
        PreparationRecord.model_validate(after["native_preparation"]).root.phase == "LEGACY_MOUNT_CONFIRMED"
    )
    assert native.uploads == native.mounts == 0


async def test_duplicate_upload_identity_refuses_without_overwriting_ownership_or_echoing_values() -> None:
    from tank.contracts.json import DuplicateJsonKeyError

    repo = MemoryRepository()
    metadata = await seed_file(repo)
    requests: list[str] = []

    def responding(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return httpx.Response(200, content=b'{"id":"private-first","id":"private-second"}')

    async with httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http:
        with pytest.raises(DuplicateJsonKeyError) as failure:
            await service(repo, http).prepare(source_scope(metadata))
        assert "private-first" not in str(failure.value) and "private-second" not in str(failure.value)
        with pytest.raises(NativePreparationUnavailable, match="automatic repost refused"):
            await service(repo, http).prepare(source_scope(metadata))
    assert requests == ["/v1/files"]


async def test_nonprogressing_resource_cursor_cannot_authorize_mount() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles()
    pages = 0

    def responding(request: httpx.Request) -> httpx.Response:
        nonlocal pages
        if request.method == "GET" and request.url.path.endswith("/resources"):
            pages += 1
            return httpx.Response(200, json={"data": [], "next_page": "same-cursor"})
        return native.respond(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http:
        with pytest.raises(ValueError, match="did not advance"):
            await service(repo, http).prepare(source_scope(metadata))
    assert pages == 2 and native.uploads == 1 and native.mounts == 0


async def test_confirmed_resource_identity_cannot_be_replaced_by_same_path_and_file() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        await service(repo, http).prepare(source_scope(metadata))
        native.resources[0]["id"] = "replacement-resource"
        with pytest.raises(NativePreparationUnavailable, match="resource identity changed"):
            await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 1


async def test_known_upload_recovery_does_not_reconstruct_unused_historical_mime_or_bytes() -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles(lose_mount=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(httpx.ReadError):
            await service(repo, http).prepare(source_scope(metadata))
        row = await repo.get("ops.telegram_files", metadata.file_key)
        assert row is not None
        unused = {
            "reported_mime_type",
            "response_content_type",
            "mime_basis",
            "declared_size",
            "file_unique_id",
            "file_identity_basis",
        }
        for field in unused:
            row.pop(field)
        row["retained_extension"] = {"actual": "unchanged"}
        await repo.put("ops.telegram_files", metadata.file_key, row)
        await repo.delete("ops.telegram_file_payloads", metadata.file_key)
        assert await service(repo, http).prepare(source_scope(metadata)) == native.resources[0]["mount_path"]
    after = await repo.get("ops.telegram_files", metadata.file_key)
    assert after is not None and not unused.intersection(after)
    assert after["retained_extension"] == {"actual": "unchanged"}
    assert native.uploads == native.mounts == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("filename", "other.csv"),
        ("size_bytes", 4),
        ("mime_type", "application/pdf"),
        ("scope", {"type": "session", "id": "foreign-session"}),
        ("size_bytes", True),
    ],
)
async def test_contradictory_file_metadata_preserves_id_and_refuses_mount_then_recovers_read_only(
    field: str,
    value: JsonValue,
) -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    native = NativeFiles()
    response: dict[str, JsonValue] = {
        "id": "file-native",
        "type": "file",
        "filename": metadata.filename,
        "mime_type": metadata.mime,
        "size_bytes": metadata.size,
        "created_at": "2026-10-26T00:00:00Z",
        "scope": None,
    }
    response[field] = value
    native.metadata_override = response
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ValueError):
            await service(repo, http).prepare(source_scope(metadata))
        stored = await repo.get("ops.telegram_files", metadata.file_key)
        assert stored is not None
        observed = PreparationRecord.model_validate(stored["native_preparation"]).root
        assert observed.phase == "FILE_ID_OBSERVED" and observed.file_id == "file-native"
        assert native.uploads == 1 and native.mounts == 0
        native.metadata_override = None
        await service(repo, http).prepare(source_scope(metadata))
    assert native.uploads == native.mounts == 1 and native.metadata_reads == 2


@pytest.mark.parametrize("identity", [".", ".."])
async def test_ambiguous_native_session_segment_refuses_before_any_upload(identity: str) -> None:
    repo = MemoryRepository()
    metadata = await seed_file(repo)
    scope = source_scope(metadata).model_copy(update={"session_id": identity})
    native = NativeFiles()
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(ValueError, match="unambiguous path segment"):
            await service(repo, http).prepare(scope)
    assert native.uploads == native.mounts == 0
