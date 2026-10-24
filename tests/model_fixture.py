"""Adapt scripted actors to the public contract: the host binds the current event, incoming item and proposal."""

from tank.event_tools import SCOPED_TOOLS

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
