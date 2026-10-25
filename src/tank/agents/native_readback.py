"""Direct native resource lookup observations without inferred lifecycle facts."""

from urllib.parse import quote

import httpx
from pydantic import JsonValue

from tank.agents.native_sessions import NativeSessionEndpoint
from tank.agents.provenance import NativePromptObservation, observe_resolved_native_prompt
from tank.contracts.native_readback import (
    NativeLifecycle as NativeLifecycle,
)
from tank.contracts.native_readback import (
    NativeSessionLookup as NativeSessionLookup,
)
from tank.contracts.native_readback import (
    NativeSessionMissing as NativeSessionMissing,
)
from tank.contracts.native_readback import (
    NativeSessionReference as NativeSessionReference,
)
from tank.contracts.native_readback import (
    NativeSessionState as NativeSessionState,
)


class NativeSessionPrompt(NativeSessionReference):
    observation: NativePromptObservation


class NativeSessionSnapshot(NativeSessionReference):
    state: NativeSessionState
    observation: NativePromptObservation


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

    async def _lookup(self, requested: NativeSessionReference) -> dict[str, JsonValue] | NativeSessionMissing:
        try:
            return await self.endpoint.request("GET", f"sessions/{quote(requested.session_id, safe='')}")
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404:
                raise
            return NativeSessionMissing(kind="MISSING", missing=True, session_id=requested.session_id)

    async def state(self, session_id: str) -> NativeSessionLookup:
        requested = NativeSessionReference(session_id=session_id)
        wire = await self._lookup(requested)
        if isinstance(wire, NativeSessionMissing):
            return wire
        return observe_native_state(wire, requested)

    async def prompt(
        self, session_id: str, *, prompt_label: str | None
    ) -> NativeSessionPrompt | NativeSessionMissing:
        requested = NativeSessionReference(session_id=session_id)
        wire = await self._lookup(requested)
        if isinstance(wire, NativeSessionMissing):
            return wire
        observed = NativeSessionReference.model_validate({"session_id": wire["id"]})
        if observed.session_id != requested.session_id:
            raise ValueError("Native prompt lookup returned a different session identity")
        return NativeSessionPrompt(
            session_id=requested.session_id,
            observation=observe_resolved_native_prompt(wire, prompt_label=prompt_label),
        )

    async def snapshot(
        self, session_id: str, *, prompt_label: str | None
    ) -> NativeSessionSnapshot | NativeSessionMissing:
        requested = NativeSessionReference(session_id=session_id)
        wire = await self._lookup(requested)
        if isinstance(wire, NativeSessionMissing):
            return wire
        return NativeSessionSnapshot(
            session_id=requested.session_id,
            state=observe_native_state(wire, requested),
            observation=observe_resolved_native_prompt(wire, prompt_label=prompt_label),
        )
