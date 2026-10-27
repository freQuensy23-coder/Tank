"""Actual completed-session retention writes preserve clock and input provenance."""

from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import respx
from runtime_failure_fixtures import runtime_failure_fixture
from test_native_execution_barrier import seed_execution_inputs
from test_session_completion_lineage import completion_repo as completion_repo

from tank.contracts.session_completion import NativeExecutionState
from tank.storage import StorageBackend
from tank.util import iso, parse_time, utcnow


@pytest.mark.parametrize("precision", [None, "BUCKET(1s)"])
async def test_actual_retention_write_preserves_source_clock_and_unknown_terminal_attempt(
    tmp_path: Path, completion_repo: StorageBackend, precision: str | None
) -> None:
    repo = completion_repo
    session = await seed_execution_inputs(repo, 1)
    now = utcnow().replace(microsecond=0)
    session.update(
        event_id=None,
        no_event=True,
        opened_at=iso(now - timedelta(days=2)),
        last_event_at=iso(now - timedelta(days=1)),
        archive_after=iso(now + timedelta(hours=1)),
        archive_after_precision="BUCKET(1m)",
        archive_after_bucket_anchor="START",
        native_execution_states={
            "input-000": {"execution_id": "input-000", "state": "COMPLETED", "settled_outcome": "COMPLETED"}
        },
    )
    if precision is not None:
        session["last_event_at_prec"] = precision
        session["last_event_at_bucket_anchor"] = "END"
    await repo.put("ops.agent_sessions", "session", session)
    requests: list[str] = []

    def routes(router: respx.MockRouter) -> None:
        def history(request: httpx.Request) -> httpx.Response:
            requests.append(request.url.path)
            return httpx.Response(200, json={"data": [], "next_page": None})

        def executions(request: httpx.Request) -> httpx.Response:
            requests.append(request.url.path)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"execution_id": identity, "state": "COMPLETED", "settled_outcome": "COMPLETED"}
                        for identity in request.url.params.get_list("ids[]")
                    ]
                },
            )

        router.get("http://native.invalid/v1/sessions/session/events").mock(side_effect=history)
        router.get("http://native.invalid/v1/sessions/session/executions").mock(side_effect=executions)

    async with runtime_failure_fixture(
        repo, tmp_path, ops_progress_enabled=False, http_routes=routes
    ) as runtime:
        keep = runtime.settings.thresholds().session_keep_days
        assert await runtime.sessions.finish_if_idle("session")
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["status"] == "OPEN"
    assert current["last_message_at"] == session["last_event_at"]
    assert current["last_message_at_prec"] == current["archive_after_prec"] == precision
    assert (
        current["last_message_at_prec_reason"]
        == current["archive_after_prec_reason"]
        == (None if precision is not None else "INCOMPLETE")
    )
    assert "archive_after_precision" not in current
    expected_anchor = "END" if precision is not None else None
    assert current.get("last_message_at_bucket_anchor") == expected_anchor
    assert current.get("archive_after_bucket_anchor") == expected_anchor
    due, activity = current["archive_after"], session["last_event_at"]
    assert isinstance(due, str) and isinstance(activity, str)
    assert parse_time(due) == parse_time(activity) + timedelta(days=keep)
    journal = current["message_log"]
    assert isinstance(journal, list) and len(journal) == 1
    message = journal[0]
    assert isinstance(message, dict) and "retry_count" not in message
    assert message["body"] == {"type": "user.message"}
    assert current["active_message_id"] is None and current["runtime_status"] == "IDLE"
    states = current["native_execution_states"]
    assert isinstance(states, dict)
    assert NativeExecutionState.model_validate(states["input-000"]).state == "COMPLETED"
    assert all(path.endswith(("/events", "/executions")) for path in requests)
    assert await repo.list("ops.jobs") == []
