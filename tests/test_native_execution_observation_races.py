"""Concurrent native status reads cannot overwrite newer committed evidence."""

import asyncio
from pathlib import Path

import httpx
import pytest
import respx
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_native_execution_barrier import seed_execution_inputs
from test_session_completion_lineage import completion_repo as completion_repo

from tank.agents.message_dispatch import MessageDispatchRequest
from tank.agents.native_inputs import AcceptedNativeInput
from tank.agents.session_ownership import put_owned_session
from tank.contracts.session_completion import CompletionSessionSnapshot, NativeExecutionState
from tank.storage import StorageBackend


@pytest.mark.parametrize(
    "first_state,new_state,expected_complete", [("RUNNING", "COMPLETED", True), ("FAILED", "QUEUED", False)]
)
async def test_late_status_response_cannot_regress_a_newer_committed_observation(
    tmp_path: Path, completion_repo: StorageBackend, first_state: str, new_state: str, expected_complete: bool
) -> None:
    repo = completion_repo
    session = await seed_execution_inputs(repo, 1)
    captured, release = asyncio.Event(), asyncio.Event()
    requests: list[str] = []

    def routes(router: respx.MockRouter) -> None:
        async def answer(request: httpx.Request) -> httpx.Response:
            state = first_state if not requests else new_state
            requests.append(state)
            response = httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": "input-000",
                            "state": state,
                            "settled_outcome": state
                            if state in {"COMPLETED", "FAILED", "CANCELLED"}
                            else None,
                        }
                    ]
                },
            )
            if len(requests) == 1:
                captured.set()
                await release.wait()
            return response

        router.get("http://native.invalid/v1/sessions/session/executions").mock(side_effect=answer)

    async with (
        runtime_failure_fixture(
            repo, tmp_path / "first", ops_progress_enabled=False, http_routes=routes
        ) as first,
        runtime_failure_fixture(
            repo, tmp_path / "second", ops_progress_enabled=False, http_routes=routes
        ) as second,
    ):
        stale = asyncio.create_task(
            first.sessions.execution_barrier(
                CompletionSessionSnapshot.from_record(session), verify_remote=True
            )
        )
        try:
            await asyncio.wait_for(captured.wait(), 2)
            winner = await second.sessions.execution_barrier(
                CompletionSessionSnapshot.from_record(session), verify_remote=True
            )
            assert winner.complete is expected_complete
            release.set()
            late = await asyncio.wait_for(stale, 2)
        finally:
            release.set()
            await asyncio.gather(stale, return_exceptions=True)
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None
    states = current["native_execution_states"]
    assert isinstance(states, dict)
    observed = NativeExecutionState.model_validate(states["input-000"])
    assert observed.state == new_state
    assert observed.settled_outcome == (
        new_state if new_state in {"COMPLETED", "FAILED", "CANCELLED"} else None
    )
    assert late.execution_states["input-000"].state == new_state
    assert late.complete is expected_complete
    assert current["message_log"] == session["message_log"] and current["runtime_status"] == "RUNNING"
    assert requests == [first_state, new_state]


async def test_accepted_input_change_during_status_read_cannot_finalize_old_input_set(
    tmp_path: Path, completion_repo: StorageBackend
) -> None:
    repo = completion_repo
    await seed_execution_inputs(repo, 1)
    captured, release = asyncio.Event(), asyncio.Event()

    def routes(router: respx.MockRouter) -> None:
        async def answer(request: httpx.Request) -> httpx.Response:
            captured.set()
            await release.wait()
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": "input-000", "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                    ]
                },
            )

        router.get("http://native.invalid/v1/sessions/session/executions").mock(side_effect=answer)

    request = MessageDispatchRequest.from_wire(
        {"kind": "SCOUT_REVIEW", "proposal_id": "proposal", "session_id": "session"}
    )
    acknowledgment = AcceptedNativeInput(status="ACCEPTED", event_id="fresh-input")
    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        settling = asyncio.create_task(runtime.sessions.completed_turn("session"))
        try:
            await asyncio.wait_for(captured.wait(), 2)
            async with repo.lock("session:session"):
                current = await repo.get("ops.agent_sessions", "session")
                assert current is not None
                journal = current["message_log"]
                assert isinstance(journal, list)
                assert all(
                    isinstance(row, dict) and row["message_id"] != request.message_id for row in journal
                )
                fresh: dict[str, JsonValue] = {
                    "message_id": request.message_id,
                    "payload": request.original_wire,
                    "body": {
                        "type": "user.message",
                        "content": [{"type": "text", "text": "Synthetic fresh owned turn"}],
                    },
                    "retry_count": 0,
                    "turn_status": "RUNNING",
                    "remote_event_id": acknowledgment.event_id,
                    "initial_input_acceptance": acknowledgment.model_dump(mode="json"),
                    "native_input_attempts": {acknowledgment.event_id: 0},
                }
                journal.append(fresh)
                current["active_message_id"] = request.message_id
                await put_owned_session(repo, "session", current)
            release.set()
            result = await asyncio.wait_for(settling, 2)
        finally:
            release.set()
            await asyncio.gather(settling, return_exceptions=True)
    assert result.complete is False and result.pending_input_ids == ["fresh-input"]
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["active_message_id"] == request.message_id
    assert current["runtime_status"] == "RUNNING" and "verified_native_completion" not in current
    assert await repo.list("ops.jobs") == []
