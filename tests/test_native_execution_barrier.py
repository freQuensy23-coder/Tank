"""Actual scoped execution-status batches settle only after complete response admission."""

from pathlib import Path
from typing import Literal

import httpx
import pytest
import respx
from pydantic import JsonValue, ValidationError
from runtime_failure_fixtures import runtime_failure_fixture
from test_session_completion_lineage import completion_repo as completion_repo

from tank.contracts.session_completion import CompletionSessionSnapshot
from tank.storage import StorageBackend
from tank.time_precision import collection_clock


async def seed_execution_inputs(repo: StorageBackend, count: int) -> dict[str, JsonValue]:
    identities = [f"input-{index:03}" for index in range(count)]
    journal: list[JsonValue] = [
        {
            "message_id": "owner-" + identity,
            "payload": {"kind": "SCOUT_REVIEW", "proposal_id": "proposal"},
            "remote_event_id": identity,
            "turn_status": "RUNNING",
            "body": {"type": "user.message"},
        }
        for identity in identities
    ]
    session: dict[str, JsonValue] = {
        "session_id": "session",
        "status": "OPEN",
        "runtime_status": "RUNNING",
        "proposal_id": "proposal",
        "agent_version_reason": "Synthetic execution fixture retains no agent version",
        "last_event_seq": 0,
        "last_item_at_reason": "This synthetic proposal session retains no source item timestamp",
        **collection_clock().fields("opened_at"),
        "message_log": journal,
        "active_message_id": "owner-" + identities[-1],
        "native_execution_receipts": {
            "idle": {
                "id": "idle",
                "type": "session.status_idle",
                "execution_id": identities[-1],
                "stop_reason": {"type": "end_turn"},
            }
        },
    }
    await repo.put("ops.agent_sessions", "session", session)
    return session


async def test_real_execution_queries_batch_sixty_four_and_preserve_input_ownership(
    tmp_path: Path, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
    session = await seed_execution_inputs(repo, 65)
    queries: list[list[str]] = []

    def routes(router: respx.MockRouter) -> None:
        def answer(request: httpx.Request) -> httpx.Response:
            identities = request.url.params.get_list("ids[]")
            queries.append(identities)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in identities
                    ]
                },
            )

        router.get("http://native.invalid/v1/sessions/session/executions").mock(side_effect=answer)

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        barrier = await runtime.sessions.execution_barrier(
            CompletionSessionSnapshot.from_record(session), verify_remote=True
        )
    assert [len(query) for query in queries] == [64, 1]
    assert queries[0] == [f"input-{index:03}" for index in range(64)] and queries[1] == ["input-064"]
    assert barrier.complete and barrier.accepted_input_ids == queries[0] + queries[1]
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["message_log"] == session["message_log"]
    assert (
        current["active_message_id"] == session["active_message_id"]
        and current["runtime_status"] == "RUNNING"
    )
    states = current["native_execution_states"]
    assert isinstance(states, dict) and len(states) == 65
    assert "superseded_native_inputs" not in current and await repo.list("ops.jobs") == []


@pytest.mark.parametrize("malformed", ["ORDER", "MISSING", "OUTCOME", "EXTRA", "SECOND_BATCH"])
async def test_unadmitted_execution_page_cannot_partially_persist_or_finalize(
    tmp_path: Path,
    completion_repo: StorageBackend,
    malformed: Literal["ORDER", "MISSING", "OUTCOME", "EXTRA", "SECOND_BATCH"],
) -> None:
    repo = completion_repo
    session = await seed_execution_inputs(repo, 65 if malformed == "SECOND_BATCH" else 2)
    calls: list[list[str]] = []

    def routes(router: respx.MockRouter) -> None:
        def answer(request: httpx.Request) -> httpx.Response:
            ids = request.url.params.get_list("ids[]")
            calls.append(ids)
            rows: list[dict[str, JsonValue]] = [
                {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                for identity in ids
            ]
            if malformed == "ORDER":
                rows.reverse()
            elif malformed == "MISSING" or (malformed == "SECOND_BATCH" and len(calls) == 2):
                rows.pop()
            elif malformed == "OUTCOME":
                rows[0]["state"] = "QUEUED"
            elif malformed == "EXTRA":
                rows[0]["assumed_owner"] = "foreign"
            return httpx.Response(200, json={"data": rows})

        router.get("http://native.invalid/v1/sessions/session/executions").mock(side_effect=answer)

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        with pytest.raises((ValueError, ValidationError)):
            await runtime.sessions.execution_barrier(
                CompletionSessionSnapshot.from_record(session), verify_remote=True
            )
    assert len(calls) == (2 if malformed == "SECOND_BATCH" else 1)
    assert await repo.get("ops.agent_sessions", "session") == session
    assert await repo.list("ops.jobs") == []
