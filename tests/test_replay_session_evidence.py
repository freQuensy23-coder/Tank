"""Replay evidence updates preserve actual concurrent native-event persistence."""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from postgres_helpers import PostgresTemplate
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from session_followup_fixtures import UnexpectedFollowupRefusals
from source_support import fixture_versions

from tank.agents.sessions import SessionService
from tank.models import Record
from tank.replay_evidence_contracts import EvidenceManifest, SealedReplayEvidence
from tank.replay_session import update_replay_session_evidence
from tank.settings import Settings
from tank.storage import MemoryRepository, StorageBackend
from tank.time_precision import collection_clock


class ReplaySessionSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.fixture(
    params=["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def replay_session_repo(
    request: pytest.FixtureRequest, postgres_template: PostgresTemplate
) -> AsyncIterator[StorageBackend]:
    if request.param == "memory":
        yield MemoryRepository()
    else:
        async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
            await repo.migrate()
            yield repo


def sealed_prefix() -> SealedReplayEvidence:
    return SealedReplayEvidence(
        manifest=EvidenceManifest(
            run_id="authored-run",
            input_sha256="a" * 64,
            cutoff="2026-09-18T17:12:00Z",
            cutoff_prec="SECOND",
            cutoff_prec_reason=None,
            files=[],
            sealed_at="2026-09-18T17:12:01Z",
            sealed_at_prec="COLLECTION_ONLY",
            sealed_at_prec_reason=None,
        ),
        manifest_sha256="b" * 64,
        sealed_root="/authored/sealed-prefix",
    )


async def test_replay_evidence_update_preserves_concurrent_actual_native_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replay_session_repo: StorageBackend
) -> None:
    repo = replay_session_repo
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "last_event_seq": 0,
            "agent_version_reason": "This authored native outcome has no agent snapshot",
            "last_item_at_reason": "This authored native outcome has no source item",
            **collection_clock().fields("opened_at"),
        },
    )
    read = asyncio.Event()
    release = asyncio.Event()
    writer_started = asyncio.Event()
    replay_task: asyncio.Task[None] | None = None
    original_get = repo.get

    async def paused_read(collection: str, key: str) -> Record | None:
        row = await original_get(collection, key)
        if asyncio.current_task() is replay_task and collection == "ops.agent_sessions":
            read.set()
            await release.wait()
        return row

    async def unused_dispatch(
        name: str, arguments: dict[str, JsonValue], context: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        raise AssertionError("The native host outcome does not invoke a tool")

    def unused_http(request: httpx.Request) -> httpx.Response:
        raise AssertionError("The native host outcome does not request HTTP")

    monkeypatch.setattr(repo, "get", paused_read)
    async with httpx.AsyncClient(transport=httpx.MockTransport(unused_http)) as http:
        service = SessionService(
            repo,
            ReplaySessionSettings(data_dir=tmp_path),
            unused_dispatch,
            http=http,
            versions=fixture_versions(),
            refusals=UnexpectedFollowupRefusals(),
        )

        async def persist_native_event() -> None:
            writer_started.set()
            await service.observe_native_event(
                "session", {"id": "native-outcome", "type": "host.analysis_incomplete"}, 7
            )

        replay_task = asyncio.create_task(update_replay_session_evidence(repo, "session", sealed_prefix()))
        await read.wait()
        writer = asyncio.create_task(persist_native_event())
        await writer_started.wait()
        release.set()
        await asyncio.gather(replay_task, writer)
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None
    assert current["replay_evidence_sha256"] == "b" * 64
    assert current["replay_cutoff"] == "2026-09-18T17:12:00Z"
    assert current["processed_events"] == ["native-outcome"]
    assert current["last_event_seq"] == 7
    assert current["host_analysis_outcomes"] == {
        "native-outcome": {
            "id": "native-outcome",
            "type": "host.analysis_incomplete",
            "disposition": "IGNORED_LEGACY_SEMANTIC_GATE",
        }
    }


async def test_replay_evidence_update_refuses_missing_session() -> None:
    repo = MemoryRepository()
    with pytest.raises(KeyError, match="session"):
        await update_replay_session_evidence(repo, "session", sealed_prefix())
    assert await repo.get("ops.agent_sessions", "session") is None


async def test_replay_evidence_update_refuses_different_session_identity() -> None:
    repo = MemoryRepository()
    original: Record = {"session_id": "other", "status": "OPEN"}
    await repo.put("ops.agent_sessions", "session", original)
    with pytest.raises(ValueError, match="Session identity differs"):
        await update_replay_session_evidence(repo, "session", sealed_prefix())
    assert await repo.get("ops.agent_sessions", "session") == original
