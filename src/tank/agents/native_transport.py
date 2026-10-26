"""Source-backed native paging, stream admission and durable host refusal receipts."""

import re
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.agents.input_ownership import UnprovedOwnershipReason
from tank.agents.native_retry import UnknownRetryReason
from tank.agents.session_ownership import put_owned_session, require_owned_session_identity
from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scalars import OneInt
from tank.time_precision import Timestamp
from tank.util import JsonInput, jsonable, sha256

type NativeWireEvent = dict[str, JsonValue]
type RefusalDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class NativeSequence(DomainModel):
    value: int = Field(ge=0)


class NativeStoredCursor(DomainModel):
    value: int | None = Field(ge=0)


class NativeFrameIdentity(DomainModel):
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    execution_id: str | None = Field(min_length=1)

    @classmethod
    def from_wire(cls, event: NativeWireEvent) -> Self:
        return cls.model_validate(
            {
                "event_id": event["id"],
                "event_type": event["type"],
                "execution_id": event.get("execution_id"),
            }
        )

    def cache_key(self) -> str:
        return sha256({"event_id": self.event_id, "execution_id": self.execution_id})


class NativeRefusalKey(DomainModel):
    session_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    execution_id: str | None = Field(min_length=1)

    def digest(self) -> str:
        return sha256(self)


class NativeRetryRefusal(DomainModel):
    schema_version: OneInt
    session_id: str = Field(min_length=1)
    frame: NativeFrameIdentity
    reason: UnknownRetryReason | UnprovedOwnershipReason
    sequence: int | None = Field(ge=0)
    recorded_at: Timestamp
    controller: ResolvedVersionIdentity

    @property
    def key(self) -> NativeRefusalKey:
        return NativeRefusalKey(
            session_id=self.session_id,
            event_id=self.frame.event_id,
            execution_id=self.frame.execution_id,
        )

    @model_validator(mode="after")
    def actual_observation_clock(self) -> Self:
        if self.recorded_at.value is None:
            raise ValueError("Host refusal requires its actual observation clock")
        return self


class NativeRetryRefusalLedger(DomainModel):
    session_id: str = Field(min_length=1)
    entries: dict[RefusalDigest, NativeRetryRefusal]

    @model_validator(mode="after")
    def entries_match_their_exact_native_scope(self) -> Self:
        for key, receipt in self.entries.items():
            if receipt.session_id != self.session_id or receipt.key.digest() != key:
                raise ValueError("Host refusal receipt differs from its session or identity key")
        return self

    @classmethod
    def from_record(cls, session_id: str, record: dict[str, JsonValue]) -> Self:
        require_owned_session_identity(session_id, record)
        return cls.model_validate(
            {
                "session_id": session_id,
                "entries": record.get("host_retry_refusals", {}),
            }
        )

    def find(self, frame: NativeFrameIdentity) -> NativeRetryRefusal | None:
        key = NativeRefusalKey(
            session_id=self.session_id, event_id=frame.event_id, execution_id=frame.execution_id
        ).digest()
        receipt = self.entries.get(key)
        if receipt is not None and receipt.frame != frame:
            raise ValueError("Refused native event identity changed its type")
        return receipt


class NativeRefusalStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...

    def lock(self, name: str) -> AbstractAsyncContextManager[Self]: ...


class NativeRetryRefusalJournal:
    def __init__(
        self,
        store: NativeRefusalStore,
        versions: ResolvedVersionIdentity,
        clock: Callable[[], Timestamp],
    ) -> None:
        self.store = store
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.clock = clock

    async def find(self, session_id: str, frame: NativeFrameIdentity) -> NativeRetryRefusal | None:
        record = await self.store.get("ops.agent_sessions", session_id)
        if record is None:
            raise KeyError(session_id)
        return NativeRetryRefusalLedger.from_record(session_id, record).find(frame)

    async def record(
        self,
        session_id: str,
        frame: NativeFrameIdentity,
        reason: UnknownRetryReason | UnprovedOwnershipReason,
        sequence: int | None,
    ) -> NativeRetryRefusal:
        async with self.store.lock("session:" + session_id):
            record = await self.store.get("ops.agent_sessions", session_id)
            if record is None:
                raise KeyError(session_id)
            ledger = NativeRetryRefusalLedger.from_record(session_id, record)
            prior = ledger.find(frame)
            if prior is not None:
                return prior
            receipt = NativeRetryRefusal(
                schema_version=1,
                session_id=session_id,
                frame=frame,
                reason=reason,
                sequence=sequence,
                recorded_at=self.clock(),
                controller=self.versions,
            )
            updated = NativeRetryRefusalLedger(
                session_id=session_id, entries={**ledger.entries, receipt.key.digest(): receipt}
            )
            record["host_retry_refusals"] = jsonable(updated.entries)
            if sequence is not None:
                current = NativeStoredCursor.model_validate({"value": record.get("last_event_seq")}).value
                record["last_event_seq"] = sequence if current is None else max(sequence, current)
            await put_owned_session(self.store, session_id, record)
            return receipt


class NativeEventObserved(DomainModel):
    status: Literal["OBSERVED"]


class NativeEventRetryRefused(DomainModel):
    status: Literal["RETRY_REFUSED"]
    receipt: NativeRetryRefusal


type NativeEventOutcome = NativeEventObserved | NativeEventRetryRefused


class NativeInboundFrame(DomainModel):
    event: NativeWireEvent = Field(repr=False)
    sequence: int | None = Field(ge=0)


def native_event_sequence(event: NativeWireEvent) -> int | None:
    return NativeSequence.model_validate({"value": event["seq"]}).value if "seq" in event else None


def decode_native_stream(raw: str, sse_id: str) -> NativeInboundFrame:
    event = decode_unique_json(raw)
    if not isinstance(event, dict):
        raise ValueError("Native stream requires an object event")
    if "event" in event and "type" not in event:
        raise ValueError("Native stream requires a flat event object")
    sequence = native_event_sequence(event)
    if (
        sequence is not None
        and sse_id
        and (re.fullmatch(r"[0-9]+", sse_id) is None or int(sse_id) != sequence)
    ):
        raise ValueError("Native stream sequence differs from its SSE identity")
    if sequence is None and re.fullmatch(r"[0-9]+", sse_id) is not None:
        raise ValueError("Native stream cursor has no reported event sequence")
    return NativeInboundFrame(event=event, sequence=sequence)


class ManagedEventPage(DomainModel):
    data: list[NativeWireEvent]
    next_page: str | None = Field(min_length=1)


class NodeEventPage(DomainModel):
    data: list[NativeWireEvent]
    has_more: bool


type NativeEventPage = ManagedEventPage | NodeEventPage


class ManagedEventCursor(DomainModel):
    kind: Literal["MANAGED"]
    page: str = Field(min_length=1)


class NodeEventCursor(DomainModel):
    kind: Literal["NODE"]
    after_seq: int = Field(ge=0)


type NativeEventCursor = ManagedEventCursor | NodeEventCursor


def decode_native_page(document: dict[str, JsonValue]) -> NativeEventPage:
    if "next_page" in document and "has_more" in document:
        raise ValueError("Native event page mixes incompatible paging contracts")
    if "next_page" in document:
        return ManagedEventPage.model_validate(document)
    if "has_more" in document:
        return NodeEventPage.model_validate(document)
    raise ValueError("Native event page did not report its paging contract")


def next_native_cursor(page: NativeEventPage, previous: NativeEventCursor | None) -> NativeEventCursor | None:
    if isinstance(page, ManagedEventPage):
        if isinstance(previous, NodeEventCursor):
            raise ValueError("Native event paging contract changed during traversal")
        if page.next_page is None:
            return None
        if previous is not None and page.next_page == previous.page:
            raise ValueError("Native event page did not advance its cursor")
        return ManagedEventCursor(kind="MANAGED", page=page.next_page)
    if isinstance(previous, ManagedEventCursor):
        raise ValueError("Native event paging contract changed during traversal")
    if not page.has_more:
        return None
    if not page.data:
        raise ValueError("Native event page cannot advance without observed events")
    sequences = [native_event_sequence(event) for event in page.data]
    if any(value is None for value in sequences):
        raise ValueError("Native event page cannot advance without reported sequences")
    known = [value for value in sequences if value is not None]
    after = max(known)
    if previous is not None and after <= previous.after_seq:
        raise ValueError("Native event page did not advance its cursor")
    return NodeEventCursor(kind="NODE", after_seq=after)
