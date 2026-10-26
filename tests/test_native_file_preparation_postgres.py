"""Physical cache receipts recover native mount acknowledgments after restart."""

import httpx
import pytest
from pydantic import JsonValue
from test_native_file_preparation import NativeFiles, seed_file, service
from test_storage import repo as repo

from tank.agents.native_file_preparation import NativePreparationUnavailable, PreparationRecord
from tank.storage import StorageBackend


@pytest.mark.parametrize("lost", ["upload", "mount"])
async def test_actual_storage_restart_respects_native_acceptance_phase(
    repo: StorageBackend, lost: str
) -> None:
    metadata = await seed_file(repo)
    native = NativeFiles(lose_upload=lost == "upload", lose_mount=lost == "mount")
    async with httpx.AsyncClient(transport=httpx.MockTransport(native.respond)) as http:
        with pytest.raises(httpx.ReadError):
            await service(repo, http).prepare(metadata)
        if lost == "upload":
            with pytest.raises(NativePreparationUnavailable, match="automatic repost refused"):
                await service(repo, http).prepare(metadata)
        else:
            await service(repo, http).prepare(metadata)
    assert native.uploads == 1 and native.mounts == (1 if lost == "mount" else 0)
    stored = await repo.get("ops.telegram_files", metadata.file_key)
    assert stored is not None
    state = PreparationRecord.model_validate(stored["native_preparation"]).root
    assert state.phase == ("MOUNT_CONFIRMED" if lost == "mount" else "UPLOAD_UNCONFIRMED")


async def test_http_observes_committed_phases_outside_parent_session_transaction(
    repo: StorageBackend,
) -> None:
    import asyncio

    from tank.contracts.session_followups import FollowupRefusalTarget

    metadata = await seed_file(repo)
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
    observed: list[str] = []

    async def committed_phase() -> str:
        row = await repo.get("ops.telegram_files", metadata.file_key)
        assert row is not None
        return PreparationRecord.model_validate(row["native_preparation"]).root.phase

    async def responding(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            phase = await asyncio.create_task(committed_phase())
            assert phase == ("UPLOAD_UNCONFIRMED" if request.url.path == "/v1/files" else "MOUNT_UNCONFIRMED")
            observed.append(phase)
        return native.respond(request)

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http,
        repo.lock("session:session"),
    ):
        await service(repo, http).prepare_question(
            FollowupRefusalTarget(question_id="question", session_id="session")
        )
    assert observed == ["UPLOAD_UNCONFIRMED", "MOUNT_UNCONFIRMED"]
    assert native.uploads == native.mounts == 1


async def test_concurrent_controller_observes_unconfirmed_upload_without_second_post(
    repo: StorageBackend,
) -> None:
    import asyncio

    metadata = await seed_file(repo)
    native = NativeFiles()
    entered, release = asyncio.Event(), asyncio.Event()

    async def responding(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/files":
            entered.set()
            await release.wait()
        return native.respond(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http:
        first = asyncio.create_task(service(repo, http).prepare(metadata))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with pytest.raises(NativePreparationUnavailable, match="automatic repost refused"):
                await service(repo, http).prepare(metadata)
        finally:
            release.set()
            await first
    assert native.uploads == native.mounts == 1


async def test_cancellation_drains_owned_native_effect_and_commits_observed_receipt(
    repo: StorageBackend,
) -> None:
    import asyncio

    metadata = await seed_file(repo)
    native = NativeFiles()
    entered, release = asyncio.Event(), asyncio.Event()

    async def responding(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/files":
            entered.set()
            await release.wait()
        return native.respond(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http:
        owner = asyncio.create_task(service(repo, http).prepare(metadata))
        await asyncio.wait_for(entered.wait(), 2)
        owner.cancel()
        await asyncio.sleep(0)
        assert not owner.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await owner
        await service(repo, http).prepare(metadata)
    assert native.uploads == native.mounts == 1


async def test_observed_upload_identity_survives_changed_cache_ownership_without_mount(
    repo: StorageBackend,
) -> None:
    from tank.agents.native_file_preparation import NativeEffectReceipts

    metadata = await seed_file(repo)
    native = NativeFiles()

    async def responding(request: httpx.Request) -> httpx.Response:
        response = native.respond(request)
        if request.url.path == "/v1/files":
            async with repo.lock("telegram-file:" + metadata.file_key):
                row = await repo.get("ops.telegram_files", metadata.file_key)
                assert row is not None
                row["question_id"] = "new-question"
                await repo.put("ops.telegram_files", metadata.file_key, row)
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(responding)) as http:
        with pytest.raises(NativePreparationUnavailable, match="source ownership changed"):
            await service(repo, http).prepare(metadata)
    stored = await repo.get("ops.telegram_files", metadata.file_key)
    assert stored is not None and stored["question_id"] == "new-question"
    receipts = NativeEffectReceipts.model_validate(stored["native_effect_receipts"]).root
    assert len(receipts) == 1
    observed = next(iter(receipts.values()))
    assert observed.phase == "FILE_ID_OBSERVED" and observed.file_id == "file-native"
    assert observed.intent.scope.question_id == "question"
    assert native.uploads == 1 and native.mounts == 0
