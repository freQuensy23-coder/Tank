"""Commit source-batch intent before advancing its cursor, then drain by target."""

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from datetime import timedelta
from typing import Protocol, Self

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.contracts.jobs import DependencyState
from tank.contracts.reaction_demands import (
    BlockedReactionDemand,
    FreshDemandHistory,
    HistoricalReactionChange,
    LinkedReactionDemand,
    MarkerBlockedReactionDemand,
    NormalizedReactionChange,
    PendingReactionDemand,
    ReactionChangeObservation,
    ReactionDemand,
    ReactionDemandBaseline,
    ReactionDemandFacts,
    ReactionDemandLedger,
    ReactionDiscoveryDisposition,
    UnreportedDemandHistory,
)
from tank.contracts.scheduler_commands import (
    ReactionAnchorFacts,
    ReactionCheckpoint,
    ReactionRevision,
    ReactionTarget,
    RevisedReactionCommand,
)
from tank.contracts.scheduler_results import (
    ReactionChangeTarget,
    ReactionFingerprint,
    SchedulerIdentity,
    applied_reaction_change_receipt,
)
from tank.models import Record
from tank.reaction_timing import late_reaction_schedule
from tank.runtime_reaction import reaction_job_command
from tank.time_precision import Timestamp, offset_schedule, propagate_timestamp
from tank.util import parse_time, sha256

CHECKPOINT_KEY = "reaction-capture-changes"


class DemandStore(Protocol):
    async def get(self, collection: str, key: str) -> Record | None: ...
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonValue], *, immutable: bool = False
    ) -> Record: ...
    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...
    def transaction(self) -> AbstractAsyncContextManager[Self]: ...
    def iter_metadata(
        self, collection: str, fields: Sequence[str], *, filters: Record
    ) -> AsyncIterator[Record]: ...
    def iter_all(self, collection: str, *, filters: Record) -> AsyncIterator[Record]: ...
    async def list(self, collection: str, *, filters: Record, limit: int) -> list[Record]: ...
    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonValue],
        idem_key: str,
        *,
        priority: int,
        max_attempts: int,
        run_after: Timestamp,
        expires_at: Timestamp,
    ) -> Record: ...


class CaptureChangeWire(Protocol):
    def capture_changes(self, since: str | None = None, end: str | None = None) -> AsyncIterator[Record]: ...


class DemandSnapshot(DomainModel):
    cursor: Timestamp | None
    revision: ReactionFingerprint | None
    ledger: ReactionDemandLedger


class FrozenHeader(DomainModel):
    checkpoint: ReactionCheckpoint
    revision: ReactionRevision
    anchor: ReactionAnchorFacts
    captured_at: str


class DemandWork(DependencyState):
    type: str
    idem_key: SchedulerIdentity
    payload: dict[str, JsonValue]


class ReactionDemandCoordinator:
    def __init__(self, repo: DemandStore, reader: CaptureChangeWire) -> None:
        self.repo, self.reader = repo, reader

    async def snapshot(self) -> DemandSnapshot:
        raw = await self.repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY)
        if raw is None:
            return DemandSnapshot(
                cursor=None,
                revision=None,
                ledger=ReactionDemandLedger(
                    contract_version=1,
                    history=FreshDemandHistory(kind="FRESH_CHECKPOINT"),
                    demands={},
                    observations={},
                ),
            )
        if raw["checkpoint_id"] != CHECKPOINT_KEY:
            raise ValueError("Reaction demand checkpoint has another identity")
        cursor = propagate_timestamp(raw, "at")
        if cursor.value is None:
            raise ValueError("Reaction demand cursor occurrence is unreported")
        state = raw["state"]
        if state == {}:
            ledger = ReactionDemandLedger(
                contract_version=1,
                history=UnreportedDemandHistory(
                    kind="LEGACY_DEMANDS_UNREPORTED", cursor=cursor, record_revision=sha256(raw)
                ),
                demands={},
                observations={},
            )
        else:
            ledger = ReactionDemandLedger.model_validate(state)
        return DemandSnapshot(cursor=cursor, revision=sha256(raw), ledger=ledger)

    async def detect(self, observed: Timestamp) -> None:
        observed = Timestamp.model_validate(observed)
        if observed.value is None or observed.precision != "COLLECTION_ONLY":
            raise ValueError("Reaction discovery requires its actual collection clock")
        while True:
            before = await self.snapshot()
            if before.cursor is not None:
                if before.cursor.value is None:
                    raise ValueError("Reaction discovery cursor occurrence is unreported")
                if parse_time(before.cursor.value) > parse_time(observed.value):
                    raise ValueError("Reaction discovery cursor cannot move backwards")
            additions: dict[str, PendingReactionDemand] = {}
            dispositions: dict[str, ReactionDiscoveryDisposition] = {}
            async for wire in self.reader.capture_changes(
                since=None if before.cursor is None else before.cursor.value,
                end=offset_schedule(observed, timedelta(microseconds=1)).value,
            ):
                source = self._source(wire)
                found, disposition = await self._discover(source, sha256(wire), observed)
                additions.update(found)
                if disposition is not None:
                    dispositions[source.change_id] = disposition
            if await self._commit_batch(before, observed, additions, dispositions):
                break
        await self.drain(observed)

    @staticmethod
    def _source(wire: Record) -> ReactionChangeObservation:
        match wire["kind"]:
            case "HISTORICAL_CAPTURE_TERMINAL":
                return HistoricalReactionChange.model_validate(
                    {field: wire[field] for field in HistoricalReactionChange.model_fields if field in wire}
                )
            case "LATE_NORMALIZED_DATA":
                return NormalizedReactionChange.model_validate(
                    {field: wire[field] for field in NormalizedReactionChange.model_fields if field in wire}
                )
            case _:
                raise ValueError("Reaction source change has an unsupported reported kind")

    async def _discover(
        self, source: ReactionChangeObservation, revision: str, observed: Timestamp
    ) -> tuple[dict[str, PendingReactionDemand], ReactionDiscoveryDisposition | None]:
        def disposition(reason: str) -> ReactionDiscoveryDisposition:
            return ReactionDiscoveryDisposition.model_validate(
                {"source": source, "source_revision": revision, "discovered_at": observed, "reason": reason}
            )

        if source.anchor is None:
            return {}, disposition("SOURCE_ANCHOR_UNREPORTED")
        latest: dict[ReactionCheckpoint, tuple[FrozenHeader, Record]] = {}
        async for row in self.repo.iter_metadata(
            "out.reaction_checkpoints",
            [
                "checkpoint",
                "revision",
                "anchor",
                "captured_at",
                "captured_at_prec",
                "captured_at_prec_reason",
                "window_end",
                "window_end_prec",
                "window_end_prec_reason",
            ],
            filters={"event_id": source.event_id, "token_id": source.token_id},
        ):
            header = FrozenHeader.model_validate(
                {field: row[field] for field in FrozenHeader.model_fields if field in row}
            )
            if header.checkpoint not in latest or header.revision > latest[header.checkpoint][0].revision:
                latest[header.checkpoint] = header, row
        if not latest:
            return {}, disposition("NO_CAPTURED_TARGET")
        demands: dict[str, PendingReactionDemand] = {}
        excluded = "SOURCE_ANCHOR_SCOPE_UNMATCHED"
        changed_at = (
            source.finished_at if isinstance(source, HistoricalReactionChange) else source.observed_at
        )
        for label, (header, row) in latest.items():
            if parse_time(source.anchor) != parse_time(header.anchor.ts):
                continue
            if parse_time(changed_at) < parse_time(header.captured_at):
                excluded = "NOT_LATE_AT_DISCOVERY"
                continue
            window_end = (
                propagate_timestamp(row, "window_end")
                if "window_end" in row and row["window_end"] is not None
                else None
            )
            if isinstance(source, NormalizedReactionChange) and window_end is not None:
                if window_end.value is None:
                    excluded = "CAPTURED_WINDOW_UNREPORTED"
                    continue
                if parse_time(window_end.value) < parse_time(source.market_from):
                    excluded = "OUTSIDE_CAPTURED_WINDOW"
                    continue
            command = RevisedReactionCommand(
                event_id=source.event_id,
                token_id=source.token_id,
                checkpoint=label,
                automatic_revision=True,
                change_id=source.change_id + ":" + label,
                reason="Late capture completed: "
                + (source.backfill_id if isinstance(source, HistoricalReactionChange) else source.change_id)
                + " ("
                + source.status
                + ")",
            )
            demands[command.change_id] = PendingReactionDemand(
                phase="PENDING",
                command=command,
                source=source,
                source_revision=revision,
                discovered_at=observed,
                baseline=ReactionDemandBaseline(
                    revision=header.revision,
                    anchor=header.anchor,
                    captured_at=propagate_timestamp(row, "captured_at"),
                    window_end=window_end,
                ),
            )
        return demands, None if demands else disposition(excluded)

    async def _commit_batch(
        self,
        before: DemandSnapshot,
        observed: Timestamp,
        additions: dict[str, PendingReactionDemand],
        dispositions: dict[str, ReactionDiscoveryDisposition],
    ) -> bool:
        async with (
            self.repo.lock("record:ops.scheduler_checkpoints:" + CHECKPOINT_KEY),
            self.repo.transaction(),
        ):
            current = await self.snapshot()
            if current.revision != before.revision:
                return False
            demands = dict(current.ledger.demands)
            for key, addition in additions.items():
                existing = demands.get(key)
                if existing is None:
                    demands[key] = addition
                elif existing.command != addition.command or existing.source != addition.source:
                    raise ValueError("Reaction demand identity has conflicting source facts")
            observations = dict(current.ledger.observations)
            observations.update(dispositions)
            ledger = ReactionDemandLedger(
                contract_version=1, history=current.ledger.history, demands=demands, observations=observations
            )
            await self.repo.put(
                "ops.scheduler_checkpoints",
                CHECKPOINT_KEY,
                {
                    "checkpoint_id": CHECKPOINT_KEY,
                    **observed.fields("at"),
                    "state": ledger.model_dump(mode="json", exclude_unset=True),
                },
            )
        return True

    async def drain(self, observed: Timestamp) -> None:
        snapshot = await self.snapshot()
        targets: dict[tuple[str, str, ReactionCheckpoint], list[str]] = {}
        for key, demand in snapshot.ledger.demands.items():
            command = demand.command
            targets.setdefault((command.event_id, command.token_id, command.checkpoint), []).append(key)
        tasks = [
            asyncio.create_task(
                self._drain_target(
                    ReactionTarget(event_id=event, token_id=token, checkpoint=label), keys, observed
                )
            )
            for (event, token, label), keys in targets.items()
        ]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(result, asyncio.CancelledError) for result in results):
            raise asyncio.CancelledError
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise ExceptionGroup("Reaction target demand drains failed", failures)

    async def _drain_target(self, target: ReactionTarget, keys: list[str], observed: Timestamp) -> None:
        async with AsyncExitStack() as locks:
            await locks.enter_async_context(
                self.repo.lock(f"reaction:{target.event_id}:{target.token_id}:{target.checkpoint}")
            )
            for key in sorted(keys):
                await locks.enter_async_context(self.repo.lock("reaction-change-write:" + key))
            for key in sorted(keys):
                await locks.enter_async_context(self.repo.lock("queue:REACTION_CAPTURE:reaction-late:" + key))
            await locks.enter_async_context(
                self.repo.lock("record:ops.scheduler_checkpoints:" + CHECKPOINT_KEY)
            )
            async with self.repo.transaction():
                current = await self.snapshot()
                demands = dict(current.ledger.demands)
                occupied = await self._occupied(target)
                for key in keys:
                    demand = demands.get(key)
                    if demand is None:
                        continue
                    marker = await self.repo.get("ops.scheduler_checkpoints", "reaction-change:" + key)
                    owner = await self.repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + key)
                    if marker is not None or owner is not None:
                        try:
                            if marker is None or owner is None:
                                raise ValueError("Reaction applied marker ownership is incomplete")
                            applied_reaction_change_receipt(
                                ReactionChangeTarget(
                                    change_id=key,
                                    event_id=target.event_id,
                                    token_id=target.token_id,
                                    checkpoint=target.checkpoint,
                                ),
                                marker,
                                owner,
                            )
                        except ValueError:
                            demands[key] = MarkerBlockedReactionDemand.model_validate(
                                self._facts(demand)
                                | {"phase": "BLOCKED", "reason": "APPLIED_MARKER_UNRESOLVED"}
                            )
                        else:
                            del demands[key]
                        continue
                    if isinstance(demand, MarkerBlockedReactionDemand):
                        continue
                    idem = "reaction-late:" + key
                    if isinstance(demand, (LinkedReactionDemand, BlockedReactionDemand)):
                        raw = await self.repo.get("ops.jobs", demand.job_id)
                    else:
                        rows = await self.repo.list(
                            "ops.jobs", filters={"type": "REACTION_CAPTURE", "idem_key": idem}, limit=2
                        )
                        if len(rows) > 1:
                            raise ValueError("Reaction demand has ambiguous queued work")
                        raw = rows[0] if rows else None
                    if raw is not None:
                        work = self._work(raw, target, key)
                        if work is None:
                            raise ValueError("Reaction demand work conflicts with its stored scope")
                        if work.status in {"QUEUED", "RUNNING"}:
                            demands[key] = LinkedReactionDemand.model_validate(
                                self._facts(demand)
                                | {"phase": "LINKED", "job_id": work.job_id, "idem_key": work.idem_key}
                            )
                            occupied = True
                        else:
                            demands[key] = BlockedReactionDemand.model_validate(
                                self._facts(demand)
                                | {
                                    "phase": "BLOCKED",
                                    "job_id": work.job_id,
                                    "idem_key": work.idem_key,
                                    "reason": "TERMINAL_WORK_UNAPPLIED",
                                }
                            )
                        continue
                    if isinstance(demand, (LinkedReactionDemand, BlockedReactionDemand)):
                        demands[key] = BlockedReactionDemand.model_validate(
                            self._facts(demand)
                            | {
                                "phase": "BLOCKED",
                                "job_id": demand.job_id,
                                "idem_key": demand.idem_key,
                                "reason": "LINKED_WORK_UNREPORTED",
                            }
                        )
                        continue
                    if occupied:
                        continue
                    schedule = late_reaction_schedule(observed)
                    raw = await self.repo.enqueue(
                        "REACTION_CAPTURE",
                        demand.command.storage_fields(),
                        idem,
                        priority=2,
                        max_attempts=5,
                        run_after=schedule.run_after,
                        expires_at=schedule.expires_at,
                    )
                    work = self._work(raw, target, key)
                    if work is None or work.status != "QUEUED":
                        raise ValueError("Reaction demand enqueue did not store its actual requested work")
                    demands[key] = LinkedReactionDemand.model_validate(
                        self._facts(demand)
                        | {"phase": "LINKED", "job_id": work.job_id, "idem_key": work.idem_key}
                    )
                    occupied = True
                raw_checkpoint = await self.repo.get("ops.scheduler_checkpoints", CHECKPOINT_KEY)
                if raw_checkpoint is None:
                    raise ValueError("Reaction demand checkpoint disappeared during its locked commit")
                ledger = ReactionDemandLedger(
                    contract_version=1,
                    history=current.ledger.history,
                    demands=demands,
                    observations=current.ledger.observations,
                )
                await self.repo.put(
                    "ops.scheduler_checkpoints",
                    CHECKPOINT_KEY,
                    raw_checkpoint | {"state": ledger.model_dump(mode="json", exclude_unset=True)},
                )

    @staticmethod
    def _facts(demand: ReactionDemand) -> Record:
        return demand.model_dump(
            mode="json", exclude_unset=True, include=set(ReactionDemandFacts.model_fields)
        )

    @staticmethod
    def _work(raw: Record, target: ReactionTarget, change_id: str | None = None) -> DemandWork | None:
        work = DemandWork.model_validate(
            {field: raw[field] for field in DemandWork.model_fields if field in raw}
        )
        command = reaction_job_command(work.payload)
        if not isinstance(command, RevisedReactionCommand):
            return None
        if (command.event_id, command.token_id, command.checkpoint) != (
            target.event_id,
            target.token_id,
            target.checkpoint,
        ):
            return None
        if work.type != "REACTION_CAPTURE" or change_id is not None and command.change_id != change_id:
            return None
        return work

    async def _occupied(self, target: ReactionTarget) -> bool:
        for status in ("QUEUED", "RUNNING"):
            async for raw in self.repo.iter_all(
                "ops.jobs", filters={"type": "REACTION_CAPTURE", "status": status}
            ):
                payload = raw["payload"]
                if not isinstance(payload, dict):
                    raise ValueError("Reaction queued work has no actual payload object")
                if payload.get("automatic_revision") is not True or (
                    payload.get("event_id"),
                    payload.get("token_id"),
                    payload.get("checkpoint"),
                ) != (target.event_id, target.token_id, target.checkpoint):
                    continue
                if self._work(raw, target) is None:
                    raise ValueError("Reaction queued work has conflicting scope")
                return True
        return False
