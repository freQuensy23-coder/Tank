"""Atomic current-state publication over immutable financial decision records."""

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from hashlib import sha256
from typing import Literal, Protocol, Self

from pydantic import Field, JsonValue, model_validator

from tank.analysis.decision_codec import decode_decision, encode_decision, encode_outlook
from tank.analysis.decision_contracts import DecisionRecord, DecisionStatusWriter, EndedDecisionTracking
from tank.contracts import DomainModel
from tank.contracts.json import decode_json
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import JsonInput, canonical_json, parse_time


class DecisionStateConflict(ValueError):
    pass


class DecisionScope(DomainModel):
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)

    @model_validator(mode="after")
    def nonblank(self) -> Self:
        if not self.event_id.strip() or not self.token_id.strip():
            raise ValueError("Decision scope requires actual identities")
        return self

    def key(self) -> str:
        return canonical_json((self.event_id, self.token_id))


class DecisionStateHead(DecisionScope):
    dec_id: str = Field(min_length=1)
    revision: int = Field(ge=1, le=9223372036854775807)
    previous_head_dec_id: str | None
    recorded_at: Timestamp
    dec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    writer: DecisionStatusWriter

    @model_validator(mode="after")
    def valid_head(self) -> Self:
        if not self.dec_id.strip():
            raise ValueError("Decision head requires an actual decision identity")
        if (self.revision == 1) != (self.previous_head_dec_id is None):
            raise ValueError("Decision head revision and prior identity disagree")
        if self.previous_head_dec_id is not None and not self.previous_head_dec_id.strip():
            raise ValueError("Decision head prior identity cannot be blank")
        if self.recorded_at.value is None or self.recorded_at.precision != "COLLECTION_ONLY":
            raise ValueError("Decision head requires an actual collection write clock")
        return self


class _HeadWire(DecisionScope):
    dec_id: str
    revision: int
    previous_head_dec_id: str | None
    recorded_at: str
    recorded_at_prec: Literal["COLLECTION_ONLY"]
    recorded_at_prec_reason: None
    dec_sha256: str
    writer: DecisionStatusWriter


class DecisionStateStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...
    def iter_metadata(
        self,
        collection: str,
        fields: Sequence[str],
        *,
        filters: dict[str, JsonValue] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[dict[str, JsonValue]]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...


class _LegacyProjection(DecisionScope):
    dec_id: str = Field(min_length=1)
    calculated_at: str | None
    tracking_recorded_at: str | None
    tracking_ended: bool | None
    previous_decision_id: str | None

    def write_clock(self) -> str | None:
        return self.tracking_recorded_at if self.tracking_ended is True else self.calculated_at


class LegacyDecisionSelection(DomainModel):
    basis: Literal["SOLE_LEGACY_RECORD", "REPORTED_WRITE_CLOCK", "EXPLICIT_STATUS_LINEAGE"]
    decision_id: str = Field(min_length=1)
    dec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def _equivalent(left: JsonValue, right: JsonValue) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return Decimal(str(left)) == Decimal(str(right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _equivalent(value, right[key]) for key, value in left.items()
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_equivalent(a, b) for a, b in zip(left, right, strict=True))
    return type(left) is type(right) and left == right


def _digest(document: dict[str, JsonValue]) -> str:
    return sha256(canonical_json(document).encode()).hexdigest()


def _wire(head: DecisionStateHead) -> dict[str, JsonValue]:
    document = decode_json(head.model_dump_json(exclude={"recorded_at"}))
    if not isinstance(document, dict):
        raise TypeError("Decision head must serialize as an object")
    document.update(head.recorded_at.fields("recorded_at"))
    validated = _HeadWire.model_validate(document)
    result = decode_json(validated.model_dump_json())
    if not isinstance(result, dict):
        raise TypeError("Decision head wire must be an object")
    return result


def _head(document: dict[str, JsonValue]) -> DecisionStateHead:
    wire = _HeadWire.model_validate(document)
    return DecisionStateHead(
        event_id=wire.event_id,
        token_id=wire.token_id,
        dec_id=wire.dec_id,
        revision=wire.revision,
        previous_head_dec_id=wire.previous_head_dec_id,
        recorded_at=propagate_timestamp(document, "recorded_at"),
        dec_sha256=wire.dec_sha256,
        writer=wire.writer,
    )


def _scope(record: DecisionRecord) -> DecisionScope:
    identity = record.metadata.identity
    return DecisionScope(event_id=identity.event_id, token_id=identity.token_id)


def _writer(record: DecisionRecord) -> DecisionStatusWriter:
    tracking = record.metadata.tracking
    if isinstance(tracking, EndedDecisionTracking):
        return tracking.writer
    return DecisionStatusWriter(
        versions=record.metadata.identity.versions, generation=record.metadata.identity.generation
    )


def _eligible(current: DecisionRecord | None, candidate: DecisionRecord) -> None:
    tracking = candidate.metadata.tracking
    if isinstance(tracking, EndedDecisionTracking):
        if current is None or tracking.previous_decision_id != current.metadata.identity.decision_id:
            raise DecisionStateConflict("Ended decision must close the actual expected financial source")
        if isinstance(current.metadata.tracking, EndedDecisionTracking):
            raise DecisionStateConflict("An ended state cannot close another ended state")
        if candidate.calculation != current.calculation:
            raise DecisionStateConflict("Ended decision changed its frozen financial calculation")
        unchanged = current.metadata.model_copy(
            update={
                "identity": current.metadata.identity.model_copy(
                    update={"decision_id": candidate.metadata.identity.decision_id}
                ),
                "tracking": candidate.metadata.tracking,
            }
        )
        if candidate.metadata != unchanged or candidate.outlook_id != current.outlook_id:
            raise DecisionStateConflict("Ended decision changed frozen financial provenance")
    elif current is not None:
        cutoff = candidate.metadata.identity.cutoff.value
        old_cutoff = current.metadata.identity.cutoff.value
        if cutoff is None or old_cutoff is None:
            raise ValueError("Financial decisions require actual cutoff clocks")
        if parse_time(cutoff) < parse_time(old_cutoff):
            raise DecisionStateConflict("An older financial input cutoff cannot replace current state")
        calculated = candidate.metadata.calculated_at.value
        previous_calculated = current.metadata.calculated_at.value
        if calculated is None or previous_calculated is None:
            raise ValueError("Financial decisions require actual calculation clocks")
        if parse_time(cutoff) == parse_time(old_cutoff) and parse_time(calculated) < parse_time(
            previous_calculated
        ):
            raise DecisionStateConflict("An older calculation cannot replace current state")


class DecisionStateRepository:
    def __init__(self, store: DecisionStateStore) -> None:
        self._store = store

    async def current(self, scope: DecisionScope) -> tuple[DecisionStateHead, DecisionRecord] | None:
        document = await self._store.get("ana.decision_state_heads", scope.key())
        if document is None:
            return None
        head = _head(document)
        if DecisionScope(event_id=head.event_id, token_id=head.token_id) != scope:
            raise ValueError("Decision head scope does not match its key")
        financial = await self._store.get("ana.decisions", head.dec_id)
        if financial is None or _digest(financial) != head.dec_sha256:
            raise ValueError("Decision head does not match the actual immutable financial document")
        record = decode_decision(financial)
        if _scope(record) != scope or record.metadata.identity.decision_id != head.dec_id:
            raise ValueError("Decision head financial identity does not match")
        if _writer(record) != head.writer:
            raise ValueError("Decision head writer does not match its actual state writer")
        return head, record

    async def legacy_current(self, scope: DecisionScope) -> LegacyDecisionSelection | None:
        fields = tuple(_LegacyProjection.model_fields)
        rows = [
            _LegacyProjection.model_validate(row)
            async for row in self._store.iter_metadata(
                "ana.decisions",
                fields,
                filters={"event_id": scope.event_id, "token_id": scope.token_id},
                batch_size=1000,
            )
        ]
        if not rows:
            return None
        basis: Literal["SOLE_LEGACY_RECORD", "REPORTED_WRITE_CLOCK", "EXPLICIT_STATUS_LINEAGE"]
        if len(rows) == 1:
            selected = rows[0]
            basis = "SOLE_LEGACY_RECORD"
        else:
            clocks = {row.dec_id: row.write_clock() for row in rows}
            if any(clock is None for clock in clocks.values()):
                raise DecisionStateConflict("Legacy decision write order was not captured")
            known = [(row, parse_time(clock)) for row in rows if (clock := clocks[row.dec_id]) is not None]
            latest = max(clock for _, clock in known)
            tied = {row.dec_id: row for row, clock in known if clock == latest}
            if len(tied) == 1:
                selected = next(iter(tied.values()))
                basis = "REPORTED_WRITE_CLOCK"
            else:
                predecessors = {
                    row.previous_decision_id for row in tied.values() if row.tracking_ended is True
                }
                terminals = [row for key, row in tied.items() if key not in predecessors]
                if len(terminals) != 1:
                    raise DecisionStateConflict("Legacy decision equal-clock order is ambiguous")
                selected = terminals[0]
                chain: set[str] = set()
                cursor: _LegacyProjection | None = selected
                while cursor is not None and cursor.dec_id not in chain:
                    chain.add(cursor.dec_id)
                    cursor = (
                        tied.get(cursor.previous_decision_id)
                        if cursor.previous_decision_id is not None
                        else None
                    )
                if chain != tied.keys():
                    raise DecisionStateConflict("Legacy decision equal-clock lineage is incomplete")
                basis = "EXPLICIT_STATUS_LINEAGE"
        wire = await self._store.get("ana.decisions", selected.dec_id)
        if wire is None:
            raise DecisionStateConflict("Legacy decision disappeared during selection")
        if (
            wire["event_id"] != scope.event_id
            or wire["token_id"] != scope.token_id
            or wire["dec_id"] != selected.dec_id
        ):
            raise ValueError("Legacy decision scope does not match its actual record")
        return LegacyDecisionSelection(basis=basis, decision_id=selected.dec_id, dec_sha256=_digest(wire))

    async def advance(
        self,
        scope: DecisionScope,
        *,
        expected: DecisionStateHead | None,
        candidate: DecisionRecord,
        recorded_at: Timestamp,
        effects: Callable[[], Awaitable[None]],
        legacy_source: LegacyDecisionSelection | None = None,
    ) -> DecisionStateHead:
        async with self._store.lock("decision-state:" + scope.key()), self._store.transaction():
            current = await self.current(scope)
            actual = None if current is None else current[0]
            if actual != expected:
                raise DecisionStateConflict("Decision state changed after its inputs were observed")
            if _scope(candidate) != scope:
                raise ValueError("Candidate decision belongs to a different scope")
            encoded = encode_decision(candidate)
            identity = candidate.metadata.identity.decision_id
            existing = await self._store.get("ana.decisions", identity)
            if existing is not None:
                if not _equivalent(existing, encoded):
                    raise DecisionStateConflict("Immutable decision identity has conflicting content")
                if actual is not None and actual.dec_id == identity:
                    return actual
                raise DecisionStateConflict("An existing financial identity cannot be revived as a new state")
            actual_legacy = await self.legacy_current(scope) if current is None else None
            if legacy_source is not None and actual_legacy != legacy_source:
                raise DecisionStateConflict("Legacy financial source changed before adoption")
            financial_source: DecisionRecord | None
            if actual_legacy is not None:
                legacy_wire = await self._store.get("ana.decisions", actual_legacy.decision_id)
                if legacy_wire is None or _digest(legacy_wire) != actual_legacy.dec_sha256:
                    raise DecisionStateConflict("Legacy financial source changed before adoption")
                financial_source = decode_decision(legacy_wire)
            else:
                financial_source = None if current is None else current[1]
            _eligible(financial_source, candidate)
            proposed = DecisionStateHead(
                event_id=scope.event_id,
                token_id=scope.token_id,
                dec_id=identity,
                revision=1 if actual is None else actual.revision + 1,
                previous_head_dec_id=None if actual is None else actual.dec_id,
                recorded_at=recorded_at,
                dec_sha256=_digest(encoded),
                writer=_writer(candidate),
            )
            await effects()
            if not isinstance(candidate.metadata.tracking, EndedDecisionTracking):
                await self._store.put(
                    "ana.outlooks", candidate.outlook_id, encode_outlook(candidate), immutable=True
                )
            await self._store.put("ana.decisions", identity, encoded, immutable=True)
            persisted = await self._store.get("ana.decisions", identity)
            if persisted is None or not _equivalent(persisted, encoded):
                raise ValueError("Persisted financial decision differs from its validated candidate")
            head = DecisionStateHead.model_validate(
                proposed.model_copy(update={"dec_sha256": _digest(persisted)})
            )
            await self._store.put("ana.decision_state_heads", scope.key(), _wire(head))
            return head
