"""Validate journal ownership at the session document persistence boundary."""

from collections.abc import Mapping
from typing import Protocol

from pydantic import JsonValue

from tank.agents.context_binding import ContextBinding
from tank.util import JsonInput


class SessionWriteStore(Protocol):
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...


async def put_owned_session(
    store: SessionWriteStore, session_id: str, record: dict[str, JsonValue]
) -> dict[str, JsonValue]:
    binding = ContextBinding.from_session(record)
    if binding.session_id != session_id:
        raise ValueError("Session identity differs from its persistence key")
    return await store.put("ops.agent_sessions", session_id, record)
