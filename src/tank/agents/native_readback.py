"""Direct native resource lookup observations without inferred lifecycle facts."""

from typing import Literal
from urllib.parse import quote

import httpx
from pydantic import Field, JsonValue

from tank.agents.native_sessions import NativeSessionEndpoint
from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag

type NativeLifecycle = Literal["rescheduling", "running", "idle", "terminated"]


class NativeSessionReference(DomainModel):
    session_id: str = Field(min_length=1)


class NativeSessionState(NativeSessionReference):
    kind: Literal["FOUND"]
    status: NativeLifecycle
    archived_at: str | None


class NativeSessionMissing(NativeSessionReference):
    kind: Literal["MISSING"]
    missing: TrueFlag


type NativeSessionLookup = NativeSessionState | NativeSessionMissing


def observe_native_state(wire: dict[str, JsonValue], requested: NativeSessionReference) -> NativeSessionState:
    observed = NativeSessionState.model_validate(
        {
            "kind": "FOUND",
            "session_id": wire["id"],
            "status": wire["status"],
            "archived_at": wire["archived_at"],
        }
    )
    if observed.session_id != requested.session_id:
        raise ValueError("Native lookup returned a different session identity")
    return observed


class NativeSessionReader:
    def __init__(self, endpoint: NativeSessionEndpoint) -> None:
        self.endpoint = endpoint

    async def state(self, session_id: str) -> NativeSessionLookup:
        requested = NativeSessionReference(session_id=session_id)
        try:
            wire = await self.endpoint.request("GET", f"sessions/{quote(requested.session_id, safe='')}")
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404:
                raise
            return NativeSessionMissing(kind="MISSING", missing=True, session_id=requested.session_id)
        return observe_native_state(wire, requested)
