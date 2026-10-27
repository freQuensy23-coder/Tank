"""Real immutable usage commits remain recoverable before frame acknowledgment."""

from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pytest
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_native_execution_barrier import seed_execution_inputs
from test_session_completion_lineage import completion_repo as completion_repo

from tank.agents.native_transport import NativeEventObserved
from tank.storage import StorageBackend
from tank.time_precision import collection_clock
from tank.util import JsonInput


@pytest.mark.parametrize("failure_point", ["USAGE_ACK", "FRAME_WRITE"])
async def test_actual_usage_commit_recovers_without_second_row_or_changed_first_receipt(
    tmp_path: Path,
    completion_repo: StorageBackend,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: Literal["USAGE_ACK", "FRAME_WRITE"],
) -> None:
    repo = completion_repo
    await seed_execution_inputs(repo, 1)
    event: dict[str, JsonValue] = {
        "id": "actual-usage-span",
        "type": "span.model_request_end",
        "model": "actual-reported-model",
        "model_usage": {"input_tokens": 2, "output_tokens": 1},
        **collection_clock().fields("processed_at"),
    }
    original_put = repo.put
    failed = False

    async def lose_ack_or_frame_write(
        collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        nonlocal failed
        if not failed and failure_point == "FRAME_WRITE" and collection == "ops.agent_sessions":
            failed = True
            raise OSError("Explicit synthetic frame write failure after real usage commit")
        stored = await original_put(collection, key, record, immutable=immutable)
        if not failed and failure_point == "USAGE_ACK" and collection == "ops.llm_usage":
            failed = True
            raise OSError("Explicit synthetic lost usage commit acknowledgment")
        return stored

    monkeypatch.setattr(repo, "put", lose_ack_or_frame_write)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        with pytest.raises(OSError, match="Explicit synthetic"):
            await runtime.sessions.observe_native_event("session", event)
        first = await repo.get("ops.llm_usage", "session:actual-usage-span")
        assert first is not None and first["input_tokens"] == 2 and first["output_tokens"] == 1
        session = await repo.get("ops.agent_sessions", "session")
        assert session is not None
        processed = session.get("processed_events")
        assert processed is None or isinstance(processed, list) and "actual-usage-span" not in processed
        recovered = await runtime.sessions.observe_native_event("session", event)
        assert isinstance(recovered, NativeEventObserved)
        assert await repo.get("ops.llm_usage", "session:actual-usage-span") == first
        assert len(await repo.list("ops.llm_usage")) == 1
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["processed_events"] == ["actual-usage-span"]
    journal = current["message_log"]
    assert isinstance(journal, list) and len(journal) == 1 and isinstance(journal[0], dict)
    assert "retry_count" not in journal[0] and journal[0]["body"] == {"type": "user.message"}
    assert await repo.list("ops.jobs") == []
