"""Durable native upload and scoped mount recovery without guessed acceptance."""

import asyncio
import base64
import hashlib
from contextlib import AbstractAsyncContextManager
from typing import Annotated, Literal, Protocol, Self
from urllib.parse import quote

import httpx
from pydantic import Field, JsonValue, RootModel, model_validator

from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json
from tank.contracts.native_files import (
    FileIdentity,
    NativeFileIdObserved,
    NativeFileNotRequested,
    NativeFilePreparation,
    NativeFileSourceScope,
    NativeLegacyMountConfirmed,
    NativeMountConfirmed,
    NativeMountIntent,
    NativeMountUnconfirmed,
    NativeUploadIntent,
    NativeUploadUnconfirmed,
)
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.contracts.telegram_state import QuestionPrompt, QuestionPromptAttachment
from tank.contracts.telegram_wire import TelegramAttachmentReference, TelegramCachedFile, TelegramFilePayload
from tank.question_answers import validate_question_identity
from tank.question_prompts import question_prompt
from tank.time_precision import Timestamp, collection_clock
from tank.util import canonical_json


class NativePreparationUnavailable(ValueError):
    """No proved safe transition is available from the retained native phase."""


class PreparationStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    async def put(
        self, collection: str, key: str, record: dict[str, JsonValue], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...

    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...


class PreparationClockSource(Protocol):
    def __call__(self) -> Timestamp: ...


class PreparationRecord(RootModel[NativeFilePreparation]):
    model_config = {"strict": True, "hide_input_in_errors": True}


type NativeObservedFileEffect = Annotated[
    NativeFileIdObserved | NativeMountConfirmed | NativeLegacyMountConfirmed,
    Field(discriminator="phase"),
]


class NativeEffectReceipts(RootModel[dict[str, NativeObservedFileEffect]]):
    model_config = {"strict": True, "hide_input_in_errors": True}

    @model_validator(mode="after")
    def receipt_keys_match_observed_facts(self) -> Self:
        for key, receipt in self.root.items():
            wire = PreparationRecord(receipt).model_dump(mode="json")
            if key != hashlib.sha256(canonical_json(wire).encode()).hexdigest():
                raise ValueError("Native effect receipt key conflicts with its observed facts")
        return self


class NativeFileResource(DomainModel):
    type: Literal["file"]
    id: FileIdentity
    file_id: FileIdentity
    mount_path: str
    created_at: str
    updated_at: str


class NativeBranchCheckout(DomainModel):
    type: Literal["branch"]
    name: FileIdentity


class NativeCommitCheckout(DomainModel):
    type: Literal["commit"]
    sha: FileIdentity


type NativeCheckout = Annotated[NativeBranchCheckout | NativeCommitCheckout, Field(discriminator="type")]


class NativeRepositoryResource(DomainModel):
    type: Literal["github_repository"]
    id: FileIdentity
    mount_path: str
    created_at: str
    updated_at: str
    url: str
    checkout: NativeCheckout | None = None


class NativeMemoryResource(DomainModel):
    type: Literal["memory_store"]
    memory_store_id: FileIdentity
    access: Literal["read_write", "read_only"] | None = None
    description: str | None = None
    instructions: str | None = None
    mount_path: str | None = None
    name: str | None = None


type NativeResource = Annotated[
    NativeFileResource | NativeRepositoryResource | NativeMemoryResource,
    Field(discriminator="type"),
]


class NativeResourcePage(DomainModel):
    data: list[NativeResource]
    next_page: FileIdentity | None


class NativeLegacyPointer(DomainModel):
    path: FileIdentity
    file_id: FileIdentity


class AttachmentReferences(RootModel[list[TelegramAttachmentReference]]):
    model_config = {"strict": True, "hide_input_in_errors": True}


class NativeUploadedIdentity(DomainModel):
    id: FileIdentity


class NativeFileTransport:
    def __init__(self, http: httpx.AsyncClient, base_url: str, api_key: str | None) -> None:
        self.http = http
        self.base_url = base_url.rstrip("/") + "/v1/"
        self.api_key = api_key

    async def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        file: tuple[str, bytes, str] | None = None,
        body: dict[str, JsonValue] | None = None,
        page: str | None = None,
    ) -> dict[str, JsonValue]:
        headers = {
            "anthropic-beta": "files-api-2025-04-14" if path == "files" else "managed-agents-2026-04-01"
        }
        if self.api_key is not None:
            headers["x-api-key"] = self.api_key
        response = await self.http.request(
            method,
            self.base_url + path,
            headers=headers,
            files={"file": file} if file is not None else None,
            json=body,
            params={"page": page} if page is not None else None,
            timeout=30,
        )
        response.raise_for_status()
        wire = decode_unique_json(response.content)
        if not isinstance(wire, dict):
            raise ValueError("Native file endpoint requires an object response")
        return wire

    async def upload(self, scope: NativeFileSourceScope, content: bytes) -> NativeUploadedIdentity:
        wire = await self.request("POST", "files", file=(scope.filename, content, scope.mime))
        return NativeUploadedIdentity.model_validate({"id": wire["id"]})

    async def resources(self, session_id: str) -> list[NativeResource]:
        path = "sessions/" + quote(session_id, safe="") + "/resources"
        resources: list[NativeResource] = []
        seen: set[str] = set()
        cursor = None
        while True:
            page = NativeResourcePage.model_validate(await self.request("GET", path, page=cursor))
            resources.extend(page.data)
            if page.next_page is None:
                return resources
            if page.next_page in seen:
                raise ValueError("Native resource pagination did not advance")
            seen.add(page.next_page)
            cursor = page.next_page

    async def mount(self, session_id: str, file_id: str, path: str) -> NativeFileResource:
        wire = await self.request(
            "POST",
            "sessions/" + quote(session_id, safe="") + "/resources",
            body={"type": "file", "file_id": file_id, "mount_path": path},
        )
        resource = NativeFileResource.model_validate(wire)
        if resource.file_id != file_id or resource.mount_path != path:
            raise ValueError("Native mount response conflicts with requested file ownership")
        return resource


def source_scope(metadata: TelegramCachedFile) -> NativeFileSourceScope:
    return NativeFileSourceScope(
        file_key=metadata.file_key,
        question_id=metadata.question_id,
        session_id=metadata.session_id,
        cached_sha256=metadata.sha256,
        filename=metadata.filename,
        mime=metadata.mime,
        size=metadata.size,
    )


def cached_source(record: dict[str, JsonValue]) -> NativeFileSourceScope:
    return NativeFileSourceScope.model_validate(
        {name: record[name] for name in ("file_key", "question_id", "session_id", "filename", "mime", "size")}
        | {"cached_sha256": record["sha256"]}
    )


def preparation_scope(state: NativeFilePreparation) -> NativeFileSourceScope:
    if isinstance(state, (NativeFileNotRequested, NativeLegacyMountConfirmed)):
        return state.scope
    if isinstance(state, NativeUploadUnconfirmed):
        return state.intent.scope
    if isinstance(state, NativeFileIdObserved):
        return state.intent.scope
    return state.intent.upload.intent.scope


class NativeFilePreparationService:
    def __init__(
        self,
        store: PreparationStore,
        transport: NativeFileTransport,
        versions: ResolvedVersionIdentity,
        *,
        clock: PreparationClockSource = collection_clock,
    ) -> None:
        self.store = store
        self.transport = transport
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.clock = clock

    async def prepare_question(self, target: FollowupRefusalTarget) -> QuestionPrompt:
        target = FollowupRefusalTarget.model_validate(target)
        async with self.store.lock("question:" + target.question_id):
            question = await self.store.get("out.user_questions", target.question_id)
            if question is None:
                raise KeyError(target.question_id)
            validate_question_identity(question, target)
            prompt = question_prompt(question)
            if prompt.attachments is None:
                return prompt
            references = AttachmentReferences.model_validate(question["attachments"]).root
            metadata: list[NativeFileSourceScope] = []
            for reference in references:
                row = await self.store.get("ops.telegram_files", reference.file_key)
                if row is None:
                    raise NativePreparationUnavailable("Attached file cache is unavailable")
                admitted = cached_source(row)
                if (
                    admitted.question_id != target.question_id
                    or admitted.session_id != target.session_id
                    or admitted.file_key != reference.file_key
                    or admitted.filename != reference.filename
                    or admitted.size != reference.size
                    or admitted.mime != reference.mime
                ):
                    raise NativePreparationUnavailable(
                        "Attached file source ownership conflicts with question"
                    )
                metadata.append(admitted)
            attachments: list[QuestionPromptAttachment] = []
            for admitted in metadata:
                path = await self.prepare(admitted)
                attachments.append(
                    QuestionPromptAttachment(filename=admitted.filename, size=admitted.size, path=path)
                )
            return QuestionPrompt(text=prompt.text, reference=prompt.reference, attachments=attachments)

    async def save(
        self,
        scope: NativeFileSourceScope,
        before: JsonValue,
        state: NativeFilePreparation,
    ) -> None:
        state = PreparationRecord.model_validate(state).root
        if isinstance(state, (NativeFileIdObserved, NativeMountConfirmed, NativeLegacyMountConfirmed)):
            async with self.store.lock("telegram-file:" + scope.file_key):
                current = await self.store.get("ops.telegram_files", scope.file_key)
                if current is None:
                    raise NativePreparationUnavailable("Native preparation cache disappeared")
                receipt = PreparationRecord(state).model_dump(mode="json")
                receipt_key = hashlib.sha256(canonical_json(receipt).encode()).hexdigest()
                previous = current.get("native_effect_receipts", {})
                receipts = NativeEffectReceipts.model_validate(previous).root
                receipts[receipt_key] = state
                current["native_effect_receipts"] = NativeEffectReceipts(receipts).model_dump(mode="json")
                await self.store.put("ops.telegram_files", scope.file_key, current)
        async with self.store.lock("telegram-file:" + scope.file_key):
            current = await self.store.get("ops.telegram_files", scope.file_key)
            if current is None:
                raise NativePreparationUnavailable("Native preparation cache disappeared")
            if current.get("native_preparation") != before or (
                before is None and "native_preparation" in current
            ):
                raise NativePreparationUnavailable("Native preparation changed before receipt persistence")
            admitted = cached_source(current)
            if admitted != scope or preparation_scope(state) != scope:
                raise NativePreparationUnavailable("Native preparation source ownership changed")
            current["native_preparation"] = PreparationRecord(state).model_dump(mode="json")
            await self.store.put("ops.telegram_files", scope.file_key, current)

    async def prepare(self, scope: NativeFileSourceScope) -> str:
        """Own and drain a task whose short phase transactions precede native effects."""
        task = asyncio.create_task(self._prepare(scope))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError as cancelled:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            task.result()
            raise cancelled

    async def _prepare(self, scope: NativeFileSourceScope) -> str:
        scope = NativeFileSourceScope.model_validate(scope)
        current = await self.store.get("ops.telegram_files", scope.file_key)
        if current is None:
            raise NativePreparationUnavailable("Attached file cache is unavailable")
        receipts = NativeEffectReceipts.model_validate(current.get("native_effect_receipts", {})).root
        admitted = cached_source(current)
        if admitted != scope:
            raise NativePreparationUnavailable("Attached file cache ownership changed")
        if "native_preparation" not in current:
            mounts = current.get("model_mounts")
            if not isinstance(mounts, dict) or scope.session_id not in mounts:
                raise NativePreparationUnavailable("Historical native file preparation is unreported")
            legacy = NativeLegacyPointer.model_validate(mounts[scope.session_id])
            occupants = [
                row
                for row in await self.transport.resources(scope.session_id)
                if row.mount_path == legacy.path
            ]
            if (
                len(occupants) != 1
                or not isinstance(occupants[0], NativeFileResource)
                or occupants[0].file_id != legacy.file_id
            ):
                raise NativePreparationUnavailable(
                    "Historical native mount lacks matching scoped resource evidence"
                )
            confirmed_legacy = NativeLegacyMountConfirmed(
                phase="LEGACY_MOUNT_CONFIRMED",
                scope=scope,
                versions=self.versions,
                recorded_at=self.clock(),
                file_id=legacy.file_id,
                resource_id=occupants[0].id,
                mount_path=legacy.path,
            )
            await self.save(scope, None, confirmed_legacy)
            return legacy.path
        before = current["native_preparation"]
        state = PreparationRecord.model_validate(before).root
        if preparation_scope(state) != scope:
            raise NativePreparationUnavailable("Native preparation belongs to a different source")
        if isinstance(state, NativeUploadUnconfirmed):
            receipts = NativeEffectReceipts.model_validate(current.get("native_effect_receipts", {})).root
            observed = [
                receipt
                for receipt in receipts.values()
                if isinstance(receipt, NativeFileIdObserved) and receipt.intent == state.intent
            ]
            if not observed or len({receipt.file_id for receipt in observed}) != 1:
                raise NativePreparationUnavailable(
                    "Native upload acceptance is unreported; automatic repost refused"
                )
            state = observed[0]
            await self.save(scope, before, state)
            before = PreparationRecord(state).model_dump(mode="json")
        if isinstance(state, NativeFileNotRequested):
            if any(preparation_scope(receipt) == scope for receipt in receipts.values()):
                raise NativePreparationUnavailable(
                    "Fresh preparation contradicts retained native effect evidence"
                )
            payload_row = await self.store.get("ops.telegram_file_payloads", scope.file_key)
            payload = TelegramFilePayload.model_validate(payload_row)
            content = base64.b64decode(payload.content_base64, validate=True)
            if (
                payload.file_key != scope.file_key
                or len(content) != scope.size
                or hashlib.sha256(content).hexdigest() != scope.cached_sha256
            ):
                raise ValueError("Attached bytes conflict with their admitted cache receipt")
            intent = NativeUploadIntent(scope=scope, versions=self.versions, recorded_at=self.clock())
            pending = NativeUploadUnconfirmed(phase="UPLOAD_UNCONFIRMED", intent=intent)
            await self.save(scope, before, pending)
            before = PreparationRecord(pending).model_dump(mode="json")
            uploaded = await self.transport.upload(scope, content)
            state = NativeFileIdObserved(
                phase="FILE_ID_OBSERVED",
                intent=intent,
                file_id=uploaded.id,
                recorded_at=self.clock(),
            )
            await self.save(scope, before, state)
            before = PreparationRecord(state).model_dump(mode="json")
        if isinstance(state, NativeFileIdObserved):
            digest = hashlib.sha256(canonical_json(scope.model_dump(mode="json")).encode()).hexdigest()
            path = f"/workspace/uploads/{digest}/attachment-1/{quote(scope.filename, safe='')}"
            mount_intent = NativeMountIntent(upload=state, mount_path=path, recorded_at=self.clock())
            pending_mount = NativeMountUnconfirmed(phase="MOUNT_UNCONFIRMED", intent=mount_intent)
            await self.save(scope, before, pending_mount)
            state = pending_mount
            before = PreparationRecord(state).model_dump(mode="json")
        if isinstance(state, NativeLegacyMountConfirmed):
            file_id, path = state.file_id, state.mount_path
        else:
            file_id, path = state.intent.upload.file_id, state.intent.mount_path
        occupants = [
            row for row in await self.transport.resources(scope.session_id) if row.mount_path == path
        ]
        if len(occupants) > 1:
            raise NativePreparationUnavailable("Native mount path has contradictory resource ownership")
        if occupants:
            resource = occupants[0]
            if not isinstance(resource, NativeFileResource) or resource.file_id != file_id:
                raise NativePreparationUnavailable("Native mount path is occupied by a different resource")
        elif isinstance(state, (NativeMountConfirmed, NativeLegacyMountConfirmed)):
            raise NativePreparationUnavailable("Previously confirmed native mount is unavailable")
        else:
            resource = await self.transport.mount(scope.session_id, file_id, path)
        if (
            isinstance(state, (NativeMountConfirmed, NativeLegacyMountConfirmed))
            and resource.id != state.resource_id
        ):
            raise NativePreparationUnavailable("Confirmed native resource identity changed")
        if isinstance(state, NativeMountUnconfirmed):
            confirmed = NativeMountConfirmed(
                phase="MOUNT_CONFIRMED",
                intent=state.intent,
                resource_id=resource.id,
                recorded_at=self.clock(),
            )
            await self.save(scope, before, confirmed)
        return path
