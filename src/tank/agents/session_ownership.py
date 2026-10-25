"""Validate journal ownership at the session document persistence boundary."""

from collections.abc import Mapping
from typing import Protocol

from pydantic import JsonValue

from tank.agents.context_binding import ContextBinding
from tank.contracts.native_readback import NativeSessionReference
from tank.util import JsonInput


class SessionWriteStore(Protocol):
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...


async def put_owned_session(
    store: SessionWriteStore, session_id: str, record: dict[str, JsonValue]
) -> dict[str, JsonValue]:
    require_owned_session_identity(session_id, record)
    ContextBinding.from_session(record)
    return await store.put("ops.agent_sessions", session_id, record)


def require_owned_session_identity(session_id: str, record: dict[str, JsonValue]) -> None:
    identity = NativeSessionReference.model_validate({"session_id": record["session_id"]})
    if identity.session_id != session_id:
        raise ValueError("Session identity differs from its persistence key")
