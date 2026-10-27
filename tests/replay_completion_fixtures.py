"""Authored accepted-input and terminal cache facts for isolated replay actors."""

from pydantic import JsonValue

from tank.agents.message_dispatch import MessageDispatchRequest
from tank.agents.sessions import SessionService
from tank.contracts.json import JsonDocument
from tank.contracts.session_completion import (
    CompletionSessionSnapshot,
    NativeCompletionStop,
    NativeExecutionBarrier,
    NativeExecutionReceipt,
    NativeExecutionState,
)


async def accept_actor_input(
    service: SessionService,
    *,
    session_id: str,
    request: MessageDispatchRequest,
    native_input_id: str,
) -> None:
    async with service.repo.lock("session:" + session_id):
        row = await service.repo.get("ops.agent_sessions", session_id)
        if row is None or row.get("session_id") != session_id:
            raise ValueError("Synthetic native acceptance requires its actual scoped session")
        journal = row.get("message_log")
        if not isinstance(journal, list):
            raise ValueError("Synthetic actor requires its authored input journal")
        if any(
            isinstance(message, dict) and message.get("message_id") == request.message_id
            for message in journal
        ):
            raise ValueError("Synthetic actor input identity already belongs to the journal")
        journal.append(
            {
                "message_id": request.message_id,
                "payload": request.original_wire,
                "remote_event_id": native_input_id,
                "retry_count": 0,
                "turn_status": "RUNNING",
            }
        )
        states = row.get("native_execution_states")
        if states is None:
            states = {}
        if not isinstance(states, dict):
            raise ValueError("Synthetic native execution cache must be an object")
        states[native_input_id] = JsonDocument.validate_value(
            NativeExecutionState(
                execution_id=native_input_id, state="RUNNING", settled_outcome=None
            ).model_dump(mode="json")
        )
        row.update(
            message_log=journal,
            native_execution_states=states,
            active_message_id=request.message_id,
            runtime_status="RUNNING",
        )
        CompletionSessionSnapshot.from_record(row)
        await service.repo.put("ops.agent_sessions", session_id, row)


async def finish_actor_input(
    service: SessionService, *, session_id: str, message_id: str, native_input_id: str
) -> None:
    async with service.repo.lock("session:" + session_id):
        row = await service.repo.get("ops.agent_sessions", session_id)
        if row is None or row.get("session_id") != session_id:
            raise ValueError("Synthetic native completion requires its actual scoped session")
        journal = row.get("message_log")
        if not isinstance(journal, list):
            raise ValueError("Synthetic native completion requires its actual journal")
        matches = [
            message
            for message in journal
            if isinstance(message, dict)
            and message.get("message_id") == message_id
            and message.get("remote_event_id") == native_input_id
        ]
        if len(matches) != 1:
            raise ValueError("Synthetic native completion cannot invent input ownership")
        matches[0]["turn_status"] = "DONE"
        states = row.get("native_execution_states")
        if not isinstance(states, dict) or native_input_id not in states:
            raise ValueError("Synthetic completion requires its previously accepted execution")
        states[native_input_id] = JsonDocument.validate_value(
            NativeExecutionState(
                execution_id=native_input_id, state="COMPLETED", settled_outcome="COMPLETED"
            ).model_dump(mode="json")
        )
        receipts = row.get("native_execution_receipts")
        if receipts is None:
            receipts = {}
        if not isinstance(receipts, dict):
            raise ValueError("Synthetic terminal receipt cache must be an object")
        identity = "idle:" + native_input_id
        receipt = NativeExecutionReceipt(
            id=identity,
            type="session.status_idle",
            execution_id=native_input_id,
            stop_reason=NativeCompletionStop(type="end_turn"),
        )
        receipts[identity] = JsonDocument.validate_value(receipt.model_dump(mode="json", exclude_unset=True))
        row.update(
            message_log=journal,
            native_execution_states=states,
            native_execution_receipts=receipts,
            active_message_id=None,
            runtime_status="IDLE",
        )
        CompletionSessionSnapshot.from_record(row)
        await service.repo.put("ops.agent_sessions", session_id, row)


async def completed_from_actor_cache(service: SessionService, session_id: str) -> NativeExecutionBarrier:
    row: dict[str, JsonValue] | None = await service.repo.get("ops.agent_sessions", session_id)
    if row is None or row.get("session_id") != session_id:
        raise ValueError("Synthetic completion lookup requires its actual scoped session")
    return await service.execution_barrier(CompletionSessionSnapshot.from_record(row))
