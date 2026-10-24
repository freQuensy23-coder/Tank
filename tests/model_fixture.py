"""Adapt existing scripted actors to the public contract without changing captured inputs."""

from tank.agents.context import ModelContext


async def model_arguments(repo, settings, tool, arguments):
    sessions = [row async for row in repo.iter_all("ops.agent_sessions")]
    session = sessions[-1] if sessions else {}
    boundary = ModelContext(repo, settings, session)
    public = await boundary.project(arguments)
    if tool in {"record_event", "add_observation"}:
        public.pop("document", None)
        for entity in public.get("entities", []):
            entity.pop("entity", None)
    await boundary.save()
    return public


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
