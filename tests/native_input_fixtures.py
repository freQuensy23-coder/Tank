"""Actual native sent-event responses for synthetic endpoint fixtures."""

import httpx
from pydantic import JsonValue

from tank.agents.native_inputs import NativeInputRequest


def acknowledge_native_input(request: httpx.Request, event_id: str) -> dict[str, JsonValue]:
    posted = NativeInputRequest.model_validate_json(request.content)
    return {"data": [{"id": event_id, **posted.events[0].model_dump(mode="json", exclude_none=True)}]}
