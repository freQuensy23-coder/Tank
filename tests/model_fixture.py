"""Adapt scripted actors to the public contract: the host binds the current event, incoming item and proposal."""

from contextvars import ContextVar

from tank.event_tools import SCOPED_TOOLS

DECLARED_EVENT = ContextVar("declared_fixture_event", default=None)

IMPLICIT = {
    **{tool: {"event_id"} for tool in SCOPED_TOOLS},
    "record_event": {"item_id"},
    "record_review": {"proposal_id"},
}


def model_arguments(tool, arguments):
    return {key: value for key, value in arguments.items() if key not in IMPLICIT.get(tool, set())}


async def audited_result(repo, event):
    """Check the public reply, then read host-only lineage for scripted replay bookkeeping."""
    import json

    from tank.agents.context import identifier_field

    public = json.loads(event["content"][0]["text"])

    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                assert not identifier_field(key), key
                check(item)
        elif isinstance(value, list):
            for item in value:
                check(item)

    check(public)
    rows = [
        row
        async for row in repo.iter_all("ops.tool_calls", filters={"call_id": event["custom_tool_use_id"]})
        if row.get("native_result_body", {}).get("content") == event["content"]
    ]
    assert rows, "Reply must match the frozen projected result of a durable call"
    return max(rows, key=lambda row: row.get("requested_at", ""))["result"]


async def process_declared_event(service, sid, event, seq=None):
    if event.get("type") != "agent.custom_tool_use":
        return await service.process_event(sid, event, seq)
    session = await service.repo.get("ops.agent_sessions", sid)
    execution_id = event.get("execution_id")
    owner_id = session.get("active_message_id")
    messages = session.setdefault("message_log", [])
    owner = next((row for row in messages if row["message_id"] == owner_id), None)
    if owner is None and messages:
        owner = messages[-1]
    if owner is None:
        owner = {
            "message_id": "fixture-message",
            "payload": session.get("last_payload", {}),
            "body": {"type": "user.message", "content": []},
            "retry_count": 0,
            "turn_status": "RUNNING",
        }
        messages.append(owner)
    if execution_id is None:
        execution_id = owner.get("remote_event_id") or "fixture-input-" + sid
    if not owner.get("remote_event_id"):
        owner["remote_event_id"] = execution_id
        owner.setdefault("native_input_attempts", {})[execution_id] = owner.get("retry_count", 0)
    session["active_message_id"] = owner["message_id"]
    await service.repo.put("ops.agent_sessions", sid, session)
    event = dict(event, execution_id=execution_id)
    if not hasattr(service, "fixture_native_request"):
        request = service.request

        async def declared(method, path, **kwargs):
            context = DECLARED_EVENT.get()
            if context is None or context[0] is not service:
                return await request(method, path, **kwargs)
            _, declared_sid, declared_event = context
            if method == "GET" and path == f"sessions/{declared_sid}/executions":
                identities = [identity for key, identity in kwargs["params"] if key == "ids[]"]
                return {
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                            "custom_tool_uses": [
                                {"declaration_id": "fixture-declaration-" + declared_event["id"], "event": declared_event}
                            ] if identity == declared_event["execution_id"] else [],
                        }
                        for identity in identities
                    ]
                }
            return await request(method, path, **kwargs)

        service.fixture_native_request = request
        service.request = declared
    token = DECLARED_EVENT.set((service, sid, event))
    try:
        return await service.process_event(sid, event, seq)
    finally:
        DECLARED_EVENT.reset(token)

