"""Durable timer creation, watch evaluation and reaction checkpoints."""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Collection, Mapping
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from threading import Event
from typing import Literal, Protocol

from pydantic import Field, JsonValue, TypeAdapter

from tank.analysis.watch_contracts import (
    CancelTimedReview,
    FireTimedReview,
    MissingTimedReviewEvent,
    TimedReviewCondition,
    TimedReviewEvent,
    TimedReviewEventState,
    UnloadedExpiredReviewEvent,
    encode_watch_condition,
    parse_timed_review_expression,
)
from tank.analysis.watch_policy import evaluate_timed_review
from tank.contracts import DomainModel
from tank.contracts.backup_manifest import BackupManifest
from tank.contracts.configuration import Thresholds
from tank.contracts.json import decode_json
from tank.contracts.milestone_checks import (
    MilestoneCheckCommand,
    MilestoneEventOwner,
    MilestoneExpiryCommand,
    MilestoneKind,
    MilestoneResolutionHistory,
    MilestoneTimerState,
    ScheduledMilestoneCheck,
)
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionAnchorFacts,
    ReactionCaptureCommand,
    ReactionCheckpoint,
    ReactionLookupCommand,
    ReactionRevision,
    ReactionSubject,
    ReactionTarget,
    RevisedReactionCommand,
    reaction_command_from_wire,
)
from tank.contracts.scheduler_results import (
    AppliedReactionChange,
    ConditionEvaluationResult,
    DeferredCondition,
    ReactionCaptureResult,
    ReactionChangeOwnership,
    ReactionChangeState,
    ReactionChangeTarget,
    ReactionFingerprint,
    ReactionPackCheckpoint,
    ReactionPackState,
    UnchangedReaction,
    applied_reaction_change_receipt,
    captured_reaction_receipt,
    reaction_capture_result,
    recorded_reaction_receipt,
)
from tank.reaction_timing import CAPTURE_EXPIRY_S, LATE_REVISION_DEFER_S
from tank.runtime_contracts import TimedReviewFiredMessage
from tank.settings import Settings
from tank.time_precision import (
    Timestamp,
    chosen_boundary,
    collection_clock,
    offset_schedule,
    propagate_timestamp,
    timestamp_owned_fields,
)
from tank.util import iso, new_id, parse_time, sha256, utcnow

log = logging.getLogger(__name__)
CHECKPOINTS = {
    "PRE_24H": -86400,
    "PRE_4H": -14400,
    "PRE_1H": -3600,
    "POST_15M": 900,
    "POST_1H": 3600,
    "POST_4H": 14400,
    "POST_24H": 86400,
    "POST_72H": 259200,
}
BENCHMARK_TOKENS = frozenset({"bitcoin:BTC", "ethereum:ETH"})


REACTION_MEASUREMENT_SLOTS = 2


EVAL_CONDITIONS_BUDGET_S = 15


EVAL_CONDITION_BUDGET_S = 8
REACTION_RETRY_S = 30


CAPTURE_YIELD_POLL_S, CAPTURE_YIELD_MAX_S = 2, 300
CAPTURE_EXPIRY_SAFETY_S = 1200


class _EvalBudgetSpent(Exception):
    """The EVAL_CONDITIONS batch budget ran out inside a market read."""


def field(record, path):
    value = record
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def same(actual, target):
    """Equality that treats numeric text and numbers alike ("0.6" == 0.6) and everything else strictly."""
    if isinstance(actual, bool) or isinstance(target, bool):
        return isinstance(actual, bool) and isinstance(target, bool) and actual == target
    try:
        a, b = Decimal(str(actual)), Decimal(str(target))
    except InvalidOperation:
        return actual == target
    return a == b if a.is_finite() and b.is_finite() else actual == target


def compare(actual, op, target):
    if actual is None:
        return False
    if isinstance(actual, dict) and actual.get("kind") == "EXACT_STRICT_MAJORITY":
        if op == ">" and Decimal(str(target)) == Decimal("0.5"):
            return actual["strict_majority"] is True
        actual = actual.get("value")
        if actual is None:
            return False
    if op in (">", ">=", "<", "<="):
        try:
            a, b = Decimal(str(actual)), Decimal(str(target))
        except InvalidOperation:
            return False
        if not (a.is_finite() and b.is_finite()):
            return False
        return {">": a > b, ">=": a >= b, "<": a < b, "<=": a <= b}[op]
    if op == "==":
        return same(actual, target)
    if op == "!=":
        return not same(actual, target)
    if op == "in":
        return any(same(actual, option) for option in target)
    raise ValueError(f"Unsupported watch operator: {op}")


class BackupFileIdentity(DomainModel):
    device: int
    inode: int
    size: int
    modified_ns: int


class BackupIdentity(DomainModel):
    directory: str
    day: date
    manifest: BackupFileIdentity | None
    complete: BackupFileIdentity | None


class ReactionEventIdentity(DomainModel):
    event_id: str = Field(min_length=1, pattern=r"^\S+$")


_REACTION_REVISION = TypeAdapter(int)


class PriorReactionRevision(DomainModel):
    expected_scope: ReactionTarget
    checkpoint_key: str
    revision: ReactionRevision
    anchor: ReactionAnchorFacts | None
    retro: bool | None
    data_fingerprint: ReactionFingerprint | None


class CompletedReactionPlan(DomainModel):
    result: ReactionCaptureResult


def completed_reaction_from_record(
    target: ReactionTarget, key: str, record: Mapping[str, JsonValue]
) -> CompletedReactionPlan:
    receipt = recorded_reaction_receipt(key, record)
    if (receipt.event_id, receipt.token_id, receipt.checkpoint) != (
        target.event_id,
        target.token_id,
        target.checkpoint,
    ):
        raise ValueError("Stored reaction checkpoint conflicts with its requested scope")
    return CompletedReactionPlan(result=receipt)


class PendingReactionPlan(DomainModel):
    source: InitialReactionCommand | ManualReactionRevisionCommand | RevisedReactionCommand
    capture: InitialReactionCommand
    key: str
    latest: PriorReactionRevision | None
    change: ReactionChangeTarget | None

    def storage_fields(self, captured: InitialReactionCommand) -> dict[str, JsonValue]:
        fields = self.source.storage_fields()
        fields.update(anchor=captured.anchor.source_fields(), revision=captured.revision)
        if self.latest is not None and captured.revision > 1:
            fields.pop("retro", None)
            if self.latest.retro is not None:
                fields["retro"] = self.latest.retro
        return fields


class ReactionWindow(DomainModel):
    anchor_stamp: Timestamp
    start: datetime
    end: datetime
    start_stamp: Timestamp
    end_stamp: Timestamp


class Scheduler:
    def __init__(
        self,
        repo,
        settings: Settings,
        market,
        truth,
        *,
        versions: ResolvedVersionIdentity,
        thresholds: Thresholds | None = None,
    ) -> None:
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.repo, self.settings, self.market, self.truth = repo, settings, market, truth
        self.thresholds = settings.thresholds() if thresholds is None else thresholds
        self.background: dict[str, asyncio.Task[None]] = {}
        self.closing = False
        self._verified_backups: set[BackupIdentity] = set()
        self._reaction_slots = asyncio.Semaphore(REACTION_MEASUREMENT_SLOTS)

    def launch(self, name: str, operation: Callable[[], Awaitable[JsonValue]]) -> None:
        if self.closing:
            return
        previous = self.background.get(name)
        if previous is not None and not previous.done():
            return

        async def guarded() -> None:
            try:
                await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s maintenance failed; next tick will retry", name)

        task = asyncio.create_task(guarded(), name="scheduler-" + name)
        self.background[name] = task

        def completed(finished: asyncio.Task[None]) -> None:

            if self.background.get(name) is finished:
                self.background.pop(name)

        task.add_done_callback(completed)

    async def close(self) -> None:
        self.closing = True
        tasks = tuple(self.background.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.background.clear()

    async def run(self, stop):
        try:
            while not stop.is_set():
                try:
                    await self.tick()
                except Exception:
                    log.exception("Scheduler tick failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=30)
        finally:
            await self.close()

    async def tick(self, now=None):
        now = now or utcnow()
        bucket_stamp = chosen_boundary(now.replace(second=0, microsecond=0), precision="BUCKET(1m)")
        bucket = bucket_stamp.value
        await self.repo.enqueue(
            "EVAL_CONDITIONS",
            {**bucket_stamp.fields("cutoff")},
            bucket,
            priority=2,
            expires_at=offset_schedule(collection_clock(now), timedelta(minutes=5)),
            max_attempts=2,
        )
        async for event in self.active_events(now):
            milestone = event.get("next_milestone")
            if (
                milestone
                and milestone.get("due_at")
                and milestone.get("status") not in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}
                and parse_time(milestone["due_at"]) <= now
            ):
                due_stamp = propagate_timestamp(milestone, "due_at")
                kind = MilestoneKind.from_record(milestone).kind
                expiry_stamp = (
                    propagate_timestamp(milestone, "expires_at")
                    if milestone.get("expires_at")
                    else offset_schedule(due_stamp, timedelta(hours=2))
                )
                expires = expiry_stamp.value
                if parse_time(expires) > now:
                    scheduled = ScheduledMilestoneCheck.model_validate(
                        {
                            "event_id": event["event_id"],
                            "milestone": kind,
                            **due_stamp.fields("due_at"),
                            "source_id": milestone.get("source_id"),
                        }
                    )
                    await self.repo.enqueue(
                        "MILESTONE_CHECK",
                        scheduled.model_dump(mode="json", exclude_unset=True),
                        f"{scheduled.event_id}:{scheduled.milestone}:{scheduled.due_at}",
                        priority=1,
                        expires_at=expiry_stamp,
                    )
            if milestone and milestone.get("due_at"):
                timer = MilestoneTimerState.from_record(milestone)
                await self.expire_milestone(
                    MilestoneExpiryCommand(
                        event_id=event["event_id"], milestone=timer.kind, due_at=timer.due_at
                    ),
                    now,
                )
            assessment = await self.latest_assessment(event)
            if not assessment:
                continue
            if assessment.get("class") not in ("MAJOR", "STRUCTURAL_SHOCK"):
                continue
            await self.ensure_reactions(event, now=now)
        await self.expire_calls(now)
        await self.demote(now)
        await self.lifecycle(now)
        self.launch("scout", lambda: self.detect_scout_hooks(now))
        if self.market is not None:
            self.launch("late-reactions", lambda: self.detect_late_data(now))
        heartbeat_hour, heartbeat_minute = map(int, str(self.thresholds.heartbeat_utc).split(":"))
        if (now.hour, now.minute) >= (heartbeat_hour, heartbeat_minute):
            await self.heartbeat(now)
        self.launch("backup", lambda: self.maintenance(now))

    async def ensure_event_reactions(self, event_id: str) -> None:
        requested = ReactionEventIdentity(event_id=event_id)
        record = await self.repo.get("evt.events", requested.event_id)
        if record is None:
            raise ValueError("Reaction scheduling event does not exist")
        stored = ReactionEventIdentity.model_validate(
            {name: record[name] for name in ("event_id",) if name in record}
        )
        if stored.event_id != requested.event_id:
            raise ValueError("Reaction scheduling event identity differs from its lookup")
        await self.ensure_reactions(record)

    async def ensure_reactions(self, event, *, now=None):
        """Open the pack outside the analyst transaction; only POST windows are jobs.

        PRE windows start as soon as the MAJOR+ assessment commits. Their immutable
        rows are the restart checkpoint: a tick resumes any missing capture, while
        a named task prevents overlapping attempts during normal operation.
        """
        now = now or utcnow()
        assessment = await self.latest_assessment(event)
        if not assessment or assessment.get("class") not in {"MAJOR", "STRUCTURAL_SHOCK"}:
            return
        anchor = self.anchor(event)
        anchor_stamp = propagate_timestamp(anchor, "ts", precision_aliases=("precision",))
        tokens = await self.analysis_tokens(event)
        captured = {
            (row["token_id"], row["checkpoint"])
            async for row in self.repo.iter_metadata(
                "out.reaction_checkpoints",
                ["token_id", "checkpoint"],
                filters={"event_id": event["event_id"]},
            )
        }
        gaps = {
            (row["state"]["token_id"], row["state"]["checkpoint"])
            async for row in self.repo.iter_all(
                "ops.scheduler_checkpoints",
                filters={"event_id": event["event_id"], "kind": "REACTION_CAPTURE_GAP"},
            )
        }

        async def capture_job(key):
            rows = [
                row
                async for row in self.repo.iter_metadata(
                    "ops.jobs",
                    [
                        "job_id",
                        "idem_key",
                        "status",
                        "expires_at",
                        "expires_at_prec",
                        "expires_at_prec_reason",
                    ],
                    filters={"type": "REACTION_CAPTURE", "idem_key": key},
                )
            ]
            return rows[0] if rows else None

        for token in sorted(tokens):
            missing_pre = []
            for label, offset in CHECKPOINTS.items():
                if (token, label) in captured or (token, label) in gaps:
                    continue
                payload = {
                    "event_id": event["event_id"],
                    "token_id": token,
                    "checkpoint": label,
                    "anchor": anchor,
                    **offset_schedule(anchor_stamp, timedelta(seconds=offset)).fields("cutoff"),
                    "retro": offset < 0 or parse_time(anchor_stamp.value) + timedelta(seconds=offset) < now,
                }
                if offset < 0:
                    missing_pre.append(payload)
                    continue
                due_stamp = offset_schedule(anchor_stamp, timedelta(seconds=offset))
                due = parse_time(due_stamp.value)
                key = f"{event['event_id']}:{token}:{label}"
                job = await capture_job(key)
                run_stamp = due_stamp if due >= now else collection_clock(now)
                expiry_stamp = (
                    propagate_timestamp(job, "expires_at")
                    if job and job.get("expires_at")
                    else offset_schedule(run_stamp, timedelta(hours=6))
                )
                terminal = job and job["status"] in {"FAILED", "EXPIRED"}
                expired = bool(job) and job["status"] != "RUNNING" and parse_time(expiry_stamp.value) <= now
                if terminal or expired:
                    await self.record_capture_gap(
                        event["event_id"], token, label, anchor, expiry_stamp, now, job
                    )
                elif not job:
                    await self.repo.enqueue(
                        "REACTION_CAPTURE",
                        payload,
                        key,
                        priority=2,
                        run_after=run_stamp,
                        expires_at=expiry_stamp,
                        max_attempts=5,
                    )
            if missing_pre and self.market is not None:

                async def capture_pre(payloads=missing_pre):

                    for payload in payloads:
                        await self.reaction_capture(reaction_command_from_wire(payload), wait_for_slot=True)

                self.launch(f"reaction-pre:{event['event_id']}:{token}", capture_pre)

    async def expire_milestone(self, command: MilestoneExpiryCommand, now: datetime) -> None:
        """Expire only the locked expected timer, never a successor or source outcome."""
        command = MilestoneExpiryCommand.model_validate(command)
        now = parse_time(now)
        async with self.repo.lock("event:" + command.event_id), self.repo.transaction():
            event = await self.repo.get("evt.events", command.event_id)
            if event is None:
                return
            MilestoneEventOwner.from_record(event, command.event_id)
            milestone = event.get("next_milestone")
            if milestone is None:
                return
            if not isinstance(milestone, dict):
                raise ValueError("Milestone timer state requires an object")
            timer = MilestoneTimerState.from_record(milestone)
            if timer.kind != command.milestone or parse_time(timer.due_at) != parse_time(command.due_at):
                raise ValueError("Milestone expiry conflicts with its expected timer")
            if timer.status in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}:
                return
            expiry = (
                timer.expires if timer.expires is not None else offset_schedule(timer.due, timedelta(hours=2))
            )
            if expiry.value is None:
                raise ValueError("Milestone expiry requires its actual occurrence")
            if parse_time(expiry.value) > now:
                return
            missed = {**milestone, "status": "MISSED", **collection_clock(now).fields("missed_at")}
            await self.repo.put("evt.events", command.event_id, {**event, "next_milestone": missed})
            await self.message(
                "MILESTONE",
                command.event_id,
                "missed:" + timer.kind + ":" + timer.due_at,
                milestone=missed,
                verification_required=True,
                instruction="Milestone verification window elapsed without accepted resolution. Re-score; verify source outcome. Timer expiry is not evidence that the source milestone failed.",
            )

    async def record_capture_gap(self, event_id, token_id, label, anchor, expiry, now, job):
        key = f"reaction-gap:{event_id}:{token_id}:{label}"
        if await self.repo.get("ops.scheduler_checkpoints", key):
            return
        await self.repo.put(
            "ops.scheduler_checkpoints",
            key,
            {
                "checkpoint_id": key,
                "event_id": event_id,
                "kind": "REACTION_CAPTURE_GAP",
                **collection_clock(now).fields("at"),
                "state": {
                    "event_id": event_id,
                    "token_id": token_id,
                    "checkpoint": label,
                    "anchor": anchor,
                    **expiry.fields("expires_at"),
                    "status": job["status"] if job and job["status"] in {"FAILED", "EXPIRED"} else "EXPIRED",
                    "job_status": job.get("status") if job else None,
                    "job_id": job.get("job_id") if job else None,
                    "reason": "Checkpoint window elapsed"
                    if not job
                    else "Capture job terminal: " + job["status"],
                    "captured": False,
                },
            },
        )
        log.error("Reaction capture gap: %s; +72h lifecycle capture requirement remains unsatisfied", key)

    async def active_events(self, now, *, states=None, include_recent_terminal=True):
        states = states or ("DETECTED", "SCORED", "TRACKED", "ANALYZING", "ALERTED", "MONITORING", "COOLDOWN")
        seen = set()
        for state in states:
            async for event in self.repo.iter_all("evt.events", filters={"lifecycle": state}):
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
        if include_recent_terminal:
            async for event in self.repo.iter_range(
                "evt.events",
                "known_to_us_at",
                start=now - timedelta(hours=72),
                end=now + timedelta(microseconds=1),
            ):
                if event["event_id"] not in seen and event.get("lifecycle") != "MERGED":
                    seen.add(event["event_id"])
                    yield event

    async def analysis_tokens(self, event):
        """The reaction pack scope: the analysis set plus the BTC and ETH benchmarks (03 §8.1)."""
        return await self.analysis_set(event) | BENCHMARK_TOKENS

    async def analysis_set(self, event):
        latest = None
        async for row in self.repo.iter_all("evt.beneficiary_maps", filters={"event_id": event["event_id"]}):
            if latest is None or row.get("map_version", 0) > latest.get("map_version", 0):
                latest = row
        if latest is not None:
            tokens = set(latest.get("analysis_set", []))
        else:
            edges = [
                row
                async for row in self.repo.iter_all(
                    "evt.beneficiary_edges", filters={"event_id": event["event_id"]}
                )
            ]
            version = max((row.get("map_version", 0) for row in edges), default=0)
            tokens = set(event.get("analysis_set", [])) | {
                row["token_id"]
                for row in edges
                if row.get("token_id") and row.get("map_version", 0) == version
            }
        return tokens

    async def latest_assessment(self, event):
        if event.get("current_ems_id"):
            current = await self.repo.get("evt.ems_assessments", event["current_ems_id"])
            if current is not None:
                return current
        latest = None
        async for row in self.repo.iter_all("evt.ems_assessments", filters={"event_id": event["event_id"]}):
            if latest is None or row.get("version", 0) > latest.get("version", 0):
                latest = row
        return latest

    async def expire_calls(self, now):
        async for call in self.repo.iter_range(
            "ana.calls",
            "opened_at",
            end=now - timedelta(days=self.thresholds.call_expiry_days) + timedelta(microseconds=1),
            filters={"status": "OPEN"},
        ):
            if parse_time(call["opened_at"]) + timedelta(days=self.thresholds.call_expiry_days) <= now:
                async with self.repo.lock("event:" + call["event_id"]):
                    event = await self.repo.get("evt.events", call["event_id"])
                    if not event:
                        raise ValueError("Expiring call references a missing event")
                    expired_ids = sorted(set(event.get("call_expired_ids", [])) | {call["call_id"]})
                    await self.repo.put(
                        "evt.events",
                        call["event_id"],
                        {**event, "call_expired": True, "call_expired_ids": expired_ids},
                    )
                    closed_call = await self.repo.put(
                        "ana.calls",
                        call["call_id"],
                        {
                            **call,
                            "status": "CLOSED",
                            "closed_by": "EXPIRED",
                            **collection_clock(now).fields("closed_at"),
                        },
                    )
                    await self.message("CALL_EXPIRED", call["event_id"], call["call_id"], call=closed_call)

    @staticmethod
    def anchor(event: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        """Serialize the canonical anchor only at the scheduler job-payload boundary."""
        from tank.market.capture import event_anchor

        return event_anchor(event).to_wire()

    async def message(self, kind, event_id, key, **extra):
        await self.repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": kind, "event_id": event_id, **extra},
            f"{event_id}:{kind}:{key}",
            priority=1,
            expires_at=offset_schedule(collection_clock(), timedelta(days=1)),
        )

    @staticmethod
    def condition_requirements(expression):
        if parse_timed_review_expression(expression) is not None:
            return []
        if "all" in expression:
            return [item for child in expression["all"] for item in Scheduler.condition_requirements(child)]
        metric = expression["metric"]
        if metric.startswith(("event.", "milestone.")):
            return []
        return [{"metric": metric, "window": expression.get("window", "W1H")}]

    async def condition_truth(self, event_id, token_id, cutoff, requirements):
        evaluator = getattr(self.truth, "condition_values", None)
        if not callable(evaluator):
            from tank.analysis.truth import MarketTruthService

            if isinstance(self.truth, MarketTruthService):
                raise RuntimeError("Selective condition evaluator is required")

            return await self.truth.assess(event_id, token_id, cutoff)
        truth = await evaluator(event_id, token_id, cutoff, requirements=requirements)

        return truth

    async def eval_conditions(self, cutoff_stamp: Timestamp) -> ConditionEvaluationResult:
        cutoff_stamp = Timestamp.model_validate(cutoff_stamp)
        cutoff = cutoff_stamp.value
        if cutoff is None:
            raise ValueError("Condition evaluation requires an actual cutoff")
        cache = {}
        conditions = [
            cond async for cond in self.repo.iter_all("ana.watch_conditions", filters={"status": "ARMED"})
        ]
        requirement_sets: dict[tuple[str, str], set[tuple[str, str]]] = {}
        for condition in conditions:
            if parse_timed_review_expression(condition["expr"]) is not None:
                TimedReviewCondition.model_validate(condition)
                continue
            key = (condition["event_id"], condition["token_id"])
            requirement_sets.setdefault(key, set()).update(
                (item["metric"], item["window"]) for item in self.condition_requirements(condition["expr"])
            )

        conditions.sort(
            key=lambda c: (
                bool(self.condition_requirements(c["expr"])),
                max(str(c.get("last_evaluated_at") or ""), str(c.get("last_attempted_at") or "")),
                c.get("role") != "INVALIDATION",
            )
        )
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + EVAL_CONDITIONS_BUDGET_S
        limit = {"until": deadline}
        evaluated: list[str] = []
        fired_conditions: list[str] = []
        deferred: list[DeferredCondition] = []

        async def bounded_truth(key, requirements):

            remaining = limit["until"] - loop.time()
            if remaining <= 0:
                raise _EvalBudgetSpent
            try:
                return await asyncio.wait_for(
                    self.condition_truth(*key, requirements=requirements), timeout=remaining
                )
            except TimeoutError:
                raise _EvalBudgetSpent from None

        event_cache = {}
        for position, cond in enumerate(conditions):
            if loop.time() - started > EVAL_CONDITIONS_BUDGET_S:
                deferred.extend(
                    DeferredCondition(cond_id=c["cond_id"], reason="BATCH_BUDGET")
                    for c in conditions[position:]
                )
                log.warning(
                    "EVAL_CONDITIONS budget of %ss reached; %s of %s conditions continue next minute",
                    EVAL_CONDITIONS_BUDGET_S,
                    len(conditions) - position,
                    len(conditions),
                )
                break
            limit["until"] = min(deadline, loop.time() + EVAL_CONDITION_BUDGET_S)
            async with self.repo.lock("condition:" + cond["cond_id"]):
                cond = await self.repo.get("ana.watch_conditions", cond["cond_id"])
                if cond["status"] != "ARMED":
                    continue
                if parse_timed_review_expression(cond["expr"]) is not None:
                    outcome = await self.eval_timed_review(
                        TimedReviewCondition.model_validate(cond), cutoff_stamp
                    )
                    if outcome == "fired":
                        fired_conditions.append(cond["cond_id"])
                    elif outcome == "evaluated":
                        evaluated.append(cond["cond_id"])
                    continue
                if not cond.get("expires_at"):
                    log.error(
                        "Watch condition %s has no expiry; requires decision reconciliation", cond["cond_id"]
                    )
                    continue
                if parse_time(cond["expires_at"]) <= parse_time(cutoff):
                    await self.repo.put(
                        "ana.watch_conditions", cond["cond_id"], {**cond, "status": "EXPIRED"}
                    )
                    continue
                expr = cond["expr"]
                if "milestone" in expr:
                    continue
                if cond["event_id"] not in event_cache:
                    event_cache[cond["event_id"]] = (
                        await self.repo.get("evt.events", cond["event_id"]),
                        [
                            row
                            async for row in self.repo.iter_all(
                                "ana.contradictions", filters={"event_id": cond["event_id"], "type": "FATAL"}
                            )
                        ],
                    )
                event, contradictions = event_cache[cond["event_id"]]
                event = {
                    **event,
                    "contradiction_fatal": any(
                        r.get("type") == "FATAL"
                        and not r.get("resolved_at")
                        and r.get("token_id") in {None, cond["token_id"]}
                        for r in contradictions
                    ),
                }

                def window_seconds(expression):
                    window = expression.get("window", "W1H").lstrip("W").lower()
                    return int(window[:-1]) * {"m": 60, "h": 3600, "d": 86400}[window[-1]]

                def persistence_windows(expression):
                    if "all" in expression:
                        return [n for child in expression["all"] for n in persistence_windows(child)]
                    if (
                        not expression["metric"].startswith(("event.", "milestone."))
                        and expression.get("persist", {}).get("count", 1) > 1
                    ):
                        return [window_seconds(expression)]
                    return []

                periods = persistence_windows(expr)

                market_cutoff_stamp = cutoff_stamp
                if periods:
                    from datetime import datetime

                    seconds = max(periods)
                    endpoint = int(parse_time(cutoff).timestamp()) // seconds * seconds
                    market_cutoff_stamp = chosen_boundary(
                        datetime.fromtimestamp(endpoint, UTC), precision="EXACT"
                    )
                counters = dict(cond.get("counters", {}))
                unobserved, leaf_matches = [], []

                async def evaluate(expr, path="root"):
                    if "all" in expr:
                        results = [
                            await evaluate(child, path + str(i)) for i, child in enumerate(expr["all"])
                        ]
                        return all(results)
                    metric = expr["metric"]
                    market_metric = not metric.startswith(("event.", "milestone."))
                    required = expr.get("persist", {}).get("count", 1)
                    evaluation_stamp = market_cutoff_stamp if market_metric else cutoff_stamp
                    evaluation_cutoff = evaluation_stamp.value
                    seconds = window_seconds(expr)
                    window_id = int(parse_time(evaluation_cutoff).timestamp()) // seconds
                    previous = counters.get(path, {})
                    if market_metric and required > 1:
                        created = cond.get("created_at")
                        if created and parse_time(evaluation_cutoff) <= parse_time(created):
                            unobserved.append(path)
                            return False
                    truth = {}
                    if market_metric:
                        cache_key = (cond["event_id"], cond["token_id"], evaluation_stamp)
                        if cache_key not in cache:
                            requirements = requirement_sets[(cond["event_id"], cond["token_id"])] | {
                                (item["metric"], item["window"])
                                for item in self.condition_requirements(cond["expr"])
                            }
                            cache[cache_key] = await bounded_truth(
                                cache_key,
                                [
                                    {"metric": metric, "window": window}
                                    for metric, window in sorted(requirements)
                                ],
                            )
                        truth = cache[cache_key]
                    value = metric_value(truth, event, metric, expr.get("window", "W1H"))
                    matched = compare(value, expr["op"], expr["value"])
                    leaf_matches.append(matched)
                    proof = next(
                        (
                            row
                            for row in truth.get("_condition_evaluation", {}).get("proofs", [])
                            if row.get("metric") == metric
                            and row.get("window", "W1H") == expr.get("window", "W1H")
                        ),
                        {},
                    )
                    availability = {
                        "data_available": value is not None,
                        "unavailable_reason": (proof.get("reason") or "INCOMPLETE")
                        if value is None
                        else None,
                    }
                    if required == 1 or not market_metric:
                        counters[path] = {
                            "window": window_id,
                            "count": int(matched),
                            "value": value,
                            **availability,
                            **evaluation_stamp.fields("cutoff"),
                        }
                        return matched

                    history = {row["window"]: dict(row) for row in previous.get("history", [])}
                    if not history and previous.get("window") is not None:
                        history[previous["window"]] = dict(previous)
                    history[window_id] = {
                        "window": window_id,
                        "matched": matched,
                        "value": value,
                        **availability,
                        **evaluation_stamp.fields("cutoff"),
                        "dependency_key": truth.get("_condition_evaluation", {}).get("request_key"),
                    }
                    history = {
                        key: row for key, row in history.items() if window_id - required < key <= window_id
                    }
                    for old_window, old in list(history.items()):
                        if old_window == window_id:
                            continue
                        old_stamp = propagate_timestamp(old, "cutoff")
                        if not old_stamp.value:
                            continue
                        old_key = (cond["event_id"], cond["token_id"], old_stamp)
                        if old_key not in cache:
                            requirements = requirement_sets[(cond["event_id"], cond["token_id"])]
                            try:
                                cache[old_key] = await bounded_truth(
                                    old_key,
                                    [
                                        {"metric": name, "window": window}
                                        for name, window in sorted(requirements)
                                    ],
                                )
                            except _EvalBudgetSpent:
                                continue
                        old_truth = cache[old_key]
                        old_value = metric_value(old_truth, event, metric, expr.get("window", "W1H"))
                        old_proof = next(
                            (
                                row
                                for row in old_truth.get("_condition_evaluation", {}).get("proofs", [])
                                if row.get("metric") == metric
                                and row.get("window", "W1H") == expr.get("window", "W1H")
                            ),
                            {},
                        )
                        history[old_window] = {
                            "window": old_window,
                            "value": old_value,
                            "matched": compare(old_value, expr["op"], expr["value"]),
                            "data_available": old_value is not None,
                            "unavailable_reason": (old_proof.get("reason") or "INCOMPLETE")
                            if old_value is None
                            else None,
                            **old_stamp.fields("cutoff"),
                            "dependency_key": old_truth.get("_condition_evaluation", {}).get("request_key"),
                        }
                    count = 0
                    for endpoint in range(window_id, window_id - required, -1):
                        if endpoint not in history or not history[endpoint].get("matched"):
                            break
                        count += 1
                    counters[path] = {
                        **history[window_id],
                        "count": count,
                        "history": [history[key] for key in sorted(history)],
                    }
                    return count >= required

                try:
                    fired = await evaluate(expr)
                except _EvalBudgetSpent:
                    await self.repo.put(
                        "ana.watch_conditions",
                        cond["cond_id"],
                        {
                            **cond,
                            **cutoff_stamp.fields("last_attempted_at"),
                            "last_attempt_outcome": "READ_TIMEOUT",
                        },
                    )
                    deferred.append(DeferredCondition(cond_id=cond["cond_id"], reason="READ_TIMEOUT"))
                    log.warning(
                        "EVAL_CONDITIONS: market read for %s exceeded its %ss budget; retried next minute",
                        cond["cond_id"],
                        EVAL_CONDITION_BUDGET_S,
                    )
                    continue
                updated = {**cond, "counters": counters, **cutoff_stamp.fields("last_evaluated_at")}
                updated.pop("last_attempt_outcome", None)
                if cond.get("rearmed_from") and not cond.get("cleared_at"):
                    available = all(row.get("data_available", True) for row in counters.values())
                    if available and not unobserved and leaf_matches and not all(leaf_matches):
                        updated.update(cutoff_stamp.fields("cleared_at"))
                    fired = False
                if fired:
                    updated.update(status="FIRED", **cutoff_stamp.fields("fired_at"))
                    async with self.repo.transaction():
                        await self.repo.put("ana.watch_conditions", cond["cond_id"], updated)
                        await self.message(
                            "CONDITION_FIRED",
                            cond["event_id"],
                            cond["cond_id"],
                            condition=updated,
                            token_id=cond["token_id"],
                            **(
                                {"session_id": cond["session_id"]}
                                if cond.get("agent_defined") and cond.get("session_id")
                                else {}
                            ),
                        )
                else:
                    await self.repo.put("ana.watch_conditions", cond["cond_id"], updated)
                if fired:
                    fired_conditions.append(cond["cond_id"])
                else:
                    evaluated.append(cond["cond_id"])

        return ConditionEvaluationResult(evaluated=evaluated, fired=fired_conditions, deferred=deferred)

    async def eval_timed_review(
        self, condition: TimedReviewCondition, cutoff_stamp: Timestamp
    ) -> Literal["fired", "evaluated"] | None:
        snapshot = TimedReviewCondition.model_validate(condition)
        clock = Timestamp.model_validate(cutoff_stamp)
        if clock.value is None:
            raise ValueError("Timed review evaluation requires its actual cutoff")
        async with self.repo.lock("event:" + snapshot.event_id), self.repo.transaction():
            row = await self.repo.get("ana.watch_conditions", snapshot.cond_id)
            if row is None:
                return None
            current = TimedReviewCondition.model_validate(row)
            if current.status != "ARMED":
                return None
            if (current.cond_id, current.event_id) != (snapshot.cond_id, snapshot.event_id):
                raise ValueError("Timed review identity changed while acquiring its event lock")
            event: dict[str, JsonValue] | None = None
            state: TimedReviewEvent
            if parse_time(current.expires_at) <= parse_time(clock.value):
                state = UnloadedExpiredReviewEvent(
                    kind="NOT_LOADED", event_id=current.event_id, reason="REVIEW_EXPIRED"
                )
            else:
                event = await self.repo.get("evt.events", current.event_id)
                state = (
                    MissingTimedReviewEvent(kind="MISSING", event_id=current.event_id)
                    if event is None
                    else TimedReviewEventState.model_validate(
                        {
                            "kind": "PRESENT",
                            "event_id": event["event_id"],
                            "lifecycle": event["lifecycle"],
                        }
                    )
                )
            action = evaluate_timed_review(current, cutoff=clock, event=state)
            updates: dict[str, JsonValue] = {
                "status": action.status,
                "last_evaluated_at": clock.value,
                "last_evaluated_at_prec": clock.precision,
                "last_evaluated_at_prec_reason": clock.precision_reason,
                "last_evaluated_at_bucket_anchor": clock.bucket_anchor,
            }
            if isinstance(action, CancelTimedReview):
                updates.update(
                    cancelled_by=action.reason,
                    cancelled_at=clock.value,
                    cancelled_at_prec=clock.precision,
                    cancelled_at_prec_reason=clock.precision_reason,
                    cancelled_at_bucket_anchor=clock.bucket_anchor,
                )
            elif isinstance(action, FireTimedReview):
                updates.update(
                    fired_at=clock.value,
                    fired_at_prec=clock.precision,
                    fired_at_prec_reason=clock.precision_reason,
                    fired_at_bucket_anchor=clock.bucket_anchor,
                )
                if event is None:
                    raise ValueError("Firing a timed review requires its actual present event")
                owned_clock_fields = timestamp_owned_fields("review_requested_at")
                event_update = {
                    name: value for name, value in event.items() if name not in owned_clock_fields
                }
                event_update.update(clock.fields("review_requested_at"))
                await self.repo.put("evt.events", current.event_id, event_update)
            updated = TimedReviewCondition.model_validate(current.model_copy(update=updates))
            await self.repo.put("ana.watch_conditions", current.cond_id, encode_watch_condition(updated))
            if isinstance(action, FireTimedReview):
                message = TimedReviewFiredMessage(
                    kind="CONDITION_FIRED",
                    event_id=current.event_id,
                    condition=updated,
                    review_at=current.expr.review_at,
                    review_question=current.expr.review_question,
                    session_id=current.session_id,
                )
                payload = decode_json(message.model_dump_json(exclude_unset=True, warnings="error"))
                if not isinstance(payload, dict):
                    raise TypeError("Timed review notification must serialize as an object")
                await self.repo.enqueue(
                    "AGENT_MESSAGE",
                    payload,
                    f"{current.event_id}:CONDITION_FIRED:{current.cond_id}",
                    priority=1,
                    expires_at=offset_schedule(collection_clock(), timedelta(days=1)),
                )
            return "fired" if isinstance(action, FireTimedReview) else "evaluated"

    async def milestone_check(self, command: MilestoneCheckCommand) -> None:
        command = MilestoneCheckCommand.model_validate(command)
        event = await self.repo.get("evt.events", command.event_id)
        if event is None:
            raise ValueError("Milestone event does not exist")
        resolution = MilestoneResolutionHistory.from_record(event, command).latest(command)
        due_key = "" if command.due_at is None else command.due_at
        await self.message(
            "MILESTONE",
            command.event_id,
            command.milestone + ":" + due_key,
            milestone=command.milestone,
            outcome=resolution.outcome if resolution is not None else None,
            quotes=resolution.quotes if resolution is not None else [],
            evidence=resolution.evidence if resolution is not None else [],
            verification_required=resolution is None,
            instruction="Verify outcome against the scheduled official source; retain a quote. Mark a missed window explicitly."
            if resolution is None
            else "Accepted primary evidence resolved the scheduled milestone.",
        )

    def _reaction_window(self, anchor: ReactionAnchorFacts, checkpoint: ReactionCheckpoint) -> ReactionWindow:
        anchor_stamp = anchor.timestamp
        if anchor_stamp.value is None:
            raise ValueError("Reaction measurement requires its actual anchor")
        origin = parse_time(anchor_stamp.value)
        offset = CHECKPOINTS[checkpoint]
        start = origin + timedelta(seconds=offset) if offset < 0 else origin
        end = origin if offset < 0 else origin + timedelta(seconds=offset)
        return ReactionWindow(
            anchor_stamp=anchor_stamp,
            start=start,
            end=end,
            start_stamp=offset_schedule(anchor_stamp, start - origin),
            end_stamp=offset_schedule(anchor_stamp, end - origin),
        )

    async def _benchmark_only(self, event_id: str, token_id: str) -> bool:
        if token_id not in BENCHMARK_TOKENS:
            return False
        event = await self.repo.get("evt.events", event_id)
        if event is None:
            raise ValueError("Benchmark reaction requires its actual event scope")
        return token_id not in await self.analysis_set(event)

    async def reaction_capture(
        self, request: ReactionCaptureCommand, *, wait_for_slot: bool = False
    ) -> ReactionCaptureResult:
        if isinstance(request, ManualReactionRevisionCommand):
            request = ManualReactionRevisionCommand.model_validate(request)
        elif isinstance(request, InitialReactionCommand):
            request = InitialReactionCommand.model_validate(request)
        elif isinstance(request, RevisedReactionCommand):
            request = RevisedReactionCommand.model_validate(request)
        elif isinstance(request, ReactionLookupCommand):
            request = ReactionLookupCommand.model_validate(request)
        else:
            raise ValueError("Reaction execution requires its admitted command")
        lock = f"reaction:{request.event_id}:{request.token_id}:{request.checkpoint}"
        while True:
            async with self.repo.lock(lock):
                plan = await self._reaction_plan(request)
                if isinstance(plan, CompletedReactionPlan):
                    return reaction_capture_result(plan.result)
            if self.market is None:
                raise ValueError("New reaction capture requires its market read service")
            if self._reaction_slots.locked() and not wait_for_slot:
                from tank.delivery import DeliveryDeferred

                raise DeliveryDeferred(
                    offset_schedule(collection_clock(), timedelta(seconds=REACTION_RETRY_S))
                )
            async with self._reaction_slots:
                measured = await self._measure_reaction(plan.capture)
            async with self.repo.lock(lock):
                current = await self._reaction_plan(request)
                if isinstance(current, CompletedReactionPlan):
                    return reaction_capture_result(current.result)
                if current.key != plan.key or current.capture != plan.capture:
                    continue
                return reaction_capture_result(await self._commit_reaction(current, measured))

    async def detect_late_data(self, now):
        marker = await self.repo.get("ops.scheduler_checkpoints", "reaction-capture-changes")
        since = marker["at"] if marker else None

        queued = {
            (job["payload"].get("event_id"), job["payload"].get("token_id"), job["payload"].get("checkpoint"))
            async for job in self.repo.iter_all(
                "ops.jobs", filters={"type": "REACTION_CAPTURE", "status": "QUEUED"}
            )
            if job["payload"].get("automatic_revision")
        }
        async for change in self.market.capture_changes(
            since=since, end=iso(now + timedelta(microseconds=1))
        ):
            changed_at = change.get("finished_at") or change["observed_at"]
            change_id = change.get("change_id") or change["backfill_id"] + ":" + changed_at
            source_id = change.get("backfill_id") or change_id
            latest = {}
            async for row in self.repo.iter_metadata(
                "out.reaction_checkpoints",
                ["checkpoint", "revision", "captured_at", "anchor", "window_end"],
                filters={"event_id": change["event_id"], "token_id": change["token_id"]},
            ):
                label = row["checkpoint"]
                if label not in latest or (row.get("revision") or 1) > (latest[label].get("revision") or 1):
                    latest[label] = row
            for label, row in latest.items():
                if parse_time(changed_at) < parse_time(row["captured_at"]):
                    continue
                if parse_time(change["anchor"]) != parse_time(row["anchor"]["ts"]):
                    continue
                if (
                    change.get("market_from")
                    and row.get("window_end")
                    and parse_time(row["window_end"]) < parse_time(change["market_from"])
                ):
                    continue
                target = (change["event_id"], change["token_id"], label)
                if target in queued:
                    continue
                queued.add(target)
                key = f"{change_id}:{label}"
                await self.repo.enqueue(
                    "REACTION_CAPTURE",
                    {
                        "event_id": change["event_id"],
                        "token_id": change["token_id"],
                        "checkpoint": label,
                        "automatic_revision": True,
                        "change_id": key,
                        "reason": "Late capture completed: "
                        + source_id
                        + " ("
                        + change.get("status", change.get("kind", "CAPTURE_CHANGE"))
                        + ")",
                    },
                    "reaction-late:" + key,
                    priority=2,
                    max_attempts=5,
                    run_after=offset_schedule(
                        collection_clock(now), timedelta(seconds=LATE_REVISION_DEFER_S)
                    ),
                    expires_at=offset_schedule(collection_clock(now), timedelta(seconds=CAPTURE_EXPIRY_S)),
                )
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "reaction-capture-changes",
            {"checkpoint_id": "reaction-capture-changes", **collection_clock(now).fields("at"), "state": {}},
        )

    async def _existing_reaction_pack(self, subject: ReactionSubject) -> ReactionPackCheckpoint | None:
        key = f"reaction-pack:{subject.event_id}:{subject.token_id}"
        record = await self.repo.get("ops.scheduler_checkpoints", key)
        if record is None:
            return None
        pack = ReactionPackCheckpoint.model_validate(
            {field: record[field] for field in ("checkpoint_id", "state") if field in record}
        )
        if (
            pack.checkpoint_id != key
            or pack.state.event_id != subject.event_id
            or pack.state.token_id != subject.token_id
        ):
            raise ValueError("Stored reaction pack conflicts with its requested scope")
        return pack

    async def reaction_pack_id(self, event_id: str, token_id: str) -> str:
        subject = ReactionSubject(event_id=event_id, token_id=token_id)
        key = f"reaction-pack:{subject.event_id}:{subject.token_id}"
        async with self.repo.lock(key):
            pack = await self._existing_reaction_pack(subject)
            if pack is None:
                authored = ReactionPackCheckpoint(
                    checkpoint_id=key,
                    state=ReactionPackState(
                        rwp_id=new_id("RWP"), event_id=subject.event_id, token_id=subject.token_id
                    ),
                )
                await self.repo.put(
                    "ops.scheduler_checkpoints",
                    key,
                    {**authored.model_dump(mode="json"), **collection_clock().fields("at")},
                    immutable=True,
                )
                pack = await self._existing_reaction_pack(subject)
                if pack is None:
                    raise ValueError("Reaction pack is absent after successful storage")
            return pack.state.rwp_id

    async def _latest_reaction_revision(self, target: ReactionTarget) -> PriorReactionRevision | None:
        """Admit only the newest revision header; financial evidence stays in its stored row."""
        rows: list[PriorReactionRevision] = []
        async for wire in self.repo.iter_metadata(
            "out.reaction_checkpoints",
            ["revision", "anchor", "retro", "data_fingerprint"],
            filters={
                "event_id": target.event_id,
                "token_id": target.token_id,
                "checkpoint": target.checkpoint,
            },
        ):
            if "revision" not in wire:
                raise ValueError("Stored reaction requires its actual revision")
            revision = _REACTION_REVISION.validate_python(wire["revision"], strict=True)
            key = f"{target.event_id}:{target.token_id}:{target.checkpoint}:{revision}"
            rows.append(
                PriorReactionRevision.model_validate(
                    {
                        "expected_scope": ReactionTarget(
                            event_id=target.event_id, token_id=target.token_id, checkpoint=target.checkpoint
                        ),
                        "checkpoint_key": key,
                        "revision": revision,
                        "anchor": wire.get("anchor"),
                        "retro": wire.get("retro"),
                        "data_fingerprint": wire.get("data_fingerprint"),
                    }
                )
            )
        return max(rows, key=lambda row: row.revision) if rows else None

    async def _reaction_plan(
        self, request: ReactionCaptureCommand
    ) -> CompletedReactionPlan | PendingReactionPlan:
        if isinstance(request, ReactionLookupCommand):
            key = f"{request.event_id}:{request.token_id}:{request.checkpoint}:{request.revision}"
            existing = await self.repo.get("out.reaction_checkpoints", key)
            if existing is None:
                raise ValueError("Reaction lookup has no captured checkpoint")
            return completed_reaction_from_record(request, key, existing)
        change: ReactionChangeTarget | None = None
        latest: PriorReactionRevision | None
        if isinstance(request, RevisedReactionCommand):
            change = ReactionChangeTarget(
                change_id=request.change_id,
                event_id=request.event_id,
                token_id=request.token_id,
                checkpoint=request.checkpoint,
            )
            marker = await self.repo.get("ops.scheduler_checkpoints", "reaction-change:" + request.change_id)
            if marker is not None:
                return CompletedReactionPlan(result=await self._applied_reaction_change(change, marker))
            latest = await self._latest_reaction_revision(request)
            if latest is None:
                raise ValueError("Automatic revision requires a captured checkpoint")
            if latest.anchor is None:
                raise ValueError("Reaction revision requires the actual prior anchor")
            capture = InitialReactionCommand(
                event_id=request.event_id,
                token_id=request.token_id,
                checkpoint=request.checkpoint,
                anchor=latest.anchor,
                retro=latest.retro,
                revision=latest.revision + 1,
                reason=request.reason,
            )
        else:
            key = f"{request.event_id}:{request.token_id}:{request.checkpoint}:{request.revision}"
            existing = await self.repo.get("out.reaction_checkpoints", key)
            if existing is not None:
                if request.revision > 1 and request.reason != existing.get("reason"):
                    raise ValueError("Immutable reaction revision already exists with another reason")
                return completed_reaction_from_record(request, key, existing)
            latest = await self._latest_reaction_revision(request)
            if request.revision > 1:
                if latest is None or request.revision != latest.revision + 1:
                    raise ValueError(
                        "Reaction revisions must append the next revision after an existing checkpoint"
                    )
                if latest.anchor is None:
                    raise ValueError("Reaction revision requires the actual prior anchor")
                capture = InitialReactionCommand(
                    event_id=request.event_id,
                    token_id=request.token_id,
                    checkpoint=request.checkpoint,
                    anchor=latest.anchor,
                    retro=latest.retro,
                    revision=request.revision,
                    reason=request.reason,
                )
            else:
                if not isinstance(request, InitialReactionCommand):
                    raise ValueError("Initial reaction requires its actual capture command")
                capture = InitialReactionCommand.model_validate(request)
        key = f"{capture.event_id}:{capture.token_id}:{capture.checkpoint}:{capture.revision}"
        await self._existing_reaction_pack(
            ReactionSubject(event_id=capture.event_id, token_id=capture.token_id)
        )
        return PendingReactionPlan(source=request, capture=capture, key=key, latest=latest, change=change)

    async def _prior_reaction_fingerprint(
        self, prior: PriorReactionRevision, fields: Collection[str]
    ) -> str | None:
        if prior.data_fingerprint is not None:
            return prior.data_fingerprint
        record = await self.repo.get("out.reaction_checkpoints", prior.checkpoint_key)
        if record is None:
            raise ValueError("Prior reaction evidence is unavailable for comparison")
        completed_reaction_from_record(prior.expected_scope, prior.checkpoint_key, record)
        if any(field not in record for field in fields):
            return None
        return sha256({field: record[field] for field in fields})

    async def _yield_to_market_truth(self, payload: ReactionTarget, checkpoint: datetime) -> None:
        """Wait while any RUN_MARKET_TRUTH is running, unless the capture nears its expiry."""
        waited = 0
        while waited < CAPTURE_YIELD_MAX_S:
            remaining = (checkpoint + timedelta(seconds=CAPTURE_EXPIRY_S) - utcnow()).total_seconds()
            if remaining <= CAPTURE_EXPIRY_SAFETY_S:
                break
            if not await self.repo.list(
                "ops.jobs", filters={"type": "RUN_MARKET_TRUTH", "status": "RUNNING"}, limit=1
            ):
                break
            await asyncio.sleep(CAPTURE_YIELD_POLL_S)
            waited += CAPTURE_YIELD_POLL_S
        if waited:
            log.info(
                "REACTION_CAPTURE %s %s %s yielded %d s to RUN_MARKET_TRUTH",
                payload.event_id,
                payload.token_id,
                payload.checkpoint,
                waited,
            )

    async def _measure_reaction(self, payload: InitialReactionCommand):
        payload = payload.with_propagated_anchor()
        captured_stamp = collection_clock()
        from tank.analysis.truth import compute_market_truth

        window = self._reaction_window(payload.anchor, payload.checkpoint)
        start, end = window.start, window.end
        start_stamp, end_stamp = window.start_stamp, window.end_stamp
        if await self._benchmark_only(payload.event_id, payload.token_id):
            return await self._measure_benchmark(payload, captured_stamp, start, end, start_stamp, end_stamp)
        await self._yield_to_market_truth(payload, end)

        captured_stamp = collection_clock()

        snapshot = await self.market.snapshot(
            payload.token_id,
            end_stamp,
            anchor={**start_stamp.fields("ts"), "type": payload.anchor.type},
            scope="REACTION",
        )
        snapshot = {
            **snapshot,
            "event_id": payload.event_id,
            "token_id": payload.token_id,
            **end_stamp.fields("cutoff"),
        }
        knobs = self.thresholds.model_dump(mode="json")
        await self._yield_to_market_truth(payload, end)
        truth = await asyncio.to_thread(
            compute_market_truth,
            snapshot,
            config={
                **knobs,
                "config_version": self.versions.config_version,
                "code_version": self.versions.code_version,
            },
        )
        relative = truth.get("relative", {})
        event_window = relative.get("windows", {}).get("EVENT", {})
        spot = truth.get("spot", {}).get("EVENT", {})
        deriv = truth.get("derivatives", {}).get("windows", {}).get("EVENT", {})
        sync = deriv.get("synchronized") or {}
        checkpoint_data = {
            "r_raw": event_window.get("r_raw"),
            "r_btc_adj": event_window.get("r_btc_adj"),
            "pctile": event_window.get("pctile"),
            "spot_cvd": (spot.get("composite_max") or {}).get("cvd"),
            "spot_method": "TRADES" if spot.get("venues") else "NONE",
            "spot_coverage": spot.get("coverage"),
            "oi_native_chg_pct": sync.get("oi_native_chg_pct"),
            "oi_native_start": sync.get("oi_native_start"),
            "oi_native_end": sync.get("oi_native_end"),
            "oi_venues": sync.get("venues"),
            "oi_class": sync.get("oi_class"),
            "funding": deriv.get("per_venue", []),
            "liquidations": deriv.get("liquidations"),
            **start_stamp.fields("window_start"),
            **end_stamp.fields("window_end"),
        }
        return {
            "payload": payload,
            "captured_stamp": captured_stamp,
            "checkpoint_data": checkpoint_data,
            "truth": truth,
        }

    async def _measure_benchmark(
        self,
        payload: InitialReactionCommand,
        captured_stamp: Timestamp,
        start: datetime,
        end: datetime,
        start_stamp: Timestamp,
        end_stamp: Timestamp,
    ):
        """Lead ruling 2026-10-20 (03 §8.1): a benchmark outside the analysis set
        records the raw return from base-tier 1m kline closes of its largest spot
        venue; trade and derivative values are NOT_APPLICABLE. Only those klines
        are read (the window plus the trailing 24 h), never a full snapshot:
        BTC/ETH snapshots over 90 days of every collection cost gigabytes."""
        from tank.analysis.truth import benchmark_reaction_values

        token = payload.token_id
        lower = min(start, end - timedelta(hours=24)) - timedelta(hours=1)
        rows = []
        for iid, instrument in self.market.instruments.items():
            if instrument.get("token_id") != token or ":spot:" not in iid:
                continue
            async for row in self.repo.iter_range(
                "mkt.klines_1m",
                "open_time",
                start=iso(lower),
                end=iso(end + timedelta(microseconds=1)),
                filters={"instrument_id": iid},
            ):
                rows.append(dict(row, token_id=token))
        bench = benchmark_reaction_values({"token_id": token, "klines": rows}, start, end)
        checkpoint_data = {
            "r_raw": bench["r_raw"],
            "r_btc_adj": None,
            "pctile": None,
            "spot_cvd": None,
            "spot_method": "NOT_APPLICABLE",
            "spot_coverage": None,
            "oi_native_chg_pct": None,
            "oi_native_start": None,
            "oi_native_end": None,
            "oi_venues": None,
            "oi_class": None,
            "funding": [],
            "liquidations": None,
            "benchmark": bench,
            **start_stamp.fields("window_start"),
            **end_stamp.fields("window_end"),
        }
        return {
            "payload": payload,
            "captured_stamp": captured_stamp,
            "checkpoint_data": checkpoint_data,
            "truth": {"benchmark": bench},
        }

    async def _applied_reaction_change(
        self, target: ReactionChangeTarget, marker: Mapping[str, JsonValue]
    ) -> AppliedReactionChange:
        owner = await self.repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + target.change_id)
        if owner is None:
            raise ValueError("Reaction change scope is unreported in retained marker")
        return applied_reaction_change_receipt(target, marker, owner)

    async def _record_reaction_change(
        self, target: ReactionChangeTarget, state: ReactionChangeState
    ) -> AppliedReactionChange:
        """The caller holds its reaction/change locks before this atomic marker/owner write."""
        target = ReactionChangeTarget.model_validate(target)
        state = ReactionChangeState.model_validate(state)
        marker_key = "reaction-change:" + target.change_id
        owner_key = "reaction-change-owner:" + target.change_id
        async with self.repo.transaction():
            existing = await self.repo.get("ops.scheduler_checkpoints", marker_key)
            if existing is not None:
                receipt = await self._applied_reaction_change(target, existing)
                if (receipt.changed, receipt.revision, receipt.data_fingerprint) != (
                    state.changed,
                    state.revision,
                    state.data_fingerprint,
                ):
                    raise ValueError("Reaction change scope already owns a different stored result")
                return receipt
            if await self.repo.get("ops.scheduler_checkpoints", owner_key) is not None:
                raise ValueError("Reaction change scope exists without its recorded marker")
            stamp = collection_clock()
            await self.repo.put(
                "ops.scheduler_checkpoints",
                marker_key,
                {"checkpoint_id": marker_key, **stamp.fields("at"), "state": state.model_dump(mode="json")},
                immutable=True,
            )
            marker = await self.repo.get("ops.scheduler_checkpoints", marker_key)
            if marker is None:
                raise ValueError("Reaction change marker is absent after successful storage")
            owner = ReactionChangeOwnership(
                checkpoint_id=owner_key,
                marker_key=marker_key,
                marker_sha256=sha256(marker),
                change_id=target.change_id,
                event_id=target.event_id,
                token_id=target.token_id,
                checkpoint=target.checkpoint,
            )
            await self.repo.put(
                "ops.scheduler_checkpoints",
                owner_key,
                {**owner.model_dump(mode="json"), **stamp.fields("at")},
                immutable=True,
            )
            return await self._applied_reaction_change(target, marker)

    async def _commit_reaction(self, plan: PendingReactionPlan, measured):
        if plan.change is not None:
            async with self.repo.lock("reaction-change-write:" + plan.change.change_id):
                return await self._commit_reaction_owned(plan, measured)
        return await self._commit_reaction_owned(plan, measured)

    async def _commit_reaction_owned(self, plan: PendingReactionPlan, measured):
        payload = InitialReactionCommand.model_validate(measured["payload"])
        target, key, latest = plan.change, plan.key, plan.latest
        revision = payload.revision
        if (payload.event_id, payload.token_id, payload.checkpoint, payload.revision) != (
            plan.capture.event_id,
            plan.capture.token_id,
            plan.capture.checkpoint,
            plan.capture.revision,
        ) or payload.anchor.timestamp != plan.capture.anchor.timestamp:
            raise ValueError("Reaction measurement conflicts with its planned scope")
        captured_stamp = Timestamp.model_validate(measured["captured_stamp"])
        if captured_stamp.value is None or captured_stamp.precision != "COLLECTION_ONLY":
            raise ValueError("Reaction capture requires its actual collection clock")
        checkpoint_data, truth = measured["checkpoint_data"], measured["truth"]
        from tank.util import sha256

        fingerprint_data = {
            name: value
            for name, value in checkpoint_data.items()
            if not name.endswith(("_prec", "_prec_reason"))
        }
        fingerprint = sha256(fingerprint_data)
        prior_fingerprint = (
            await self._prior_reaction_fingerprint(latest, fingerprint_data.keys())
            if latest is not None and target is not None
            else None
        )
        if target is not None and latest is not None and fingerprint == prior_fingerprint:
            receipt = await self._record_reaction_change(
                target,
                ReactionChangeState.model_validate(
                    {"changed": False, "revision": latest.revision, "data_fingerprint": fingerprint}
                ),
            )
            return UnchangedReaction.model_validate(
                {
                    "kind": "UNCHANGED",
                    "change_id": receipt.change_id,
                    "event_id": receipt.event_id,
                    "token_id": receipt.token_id,
                    "checkpoint": receipt.checkpoint,
                    "changed": receipt.changed,
                    "revision": receipt.revision,
                    "data_fingerprint": receipt.data_fingerprint,
                }
            )
        record = {
            "data_fingerprint": fingerprint,
            "rwp_id": await self.reaction_pack_id(payload.event_id, payload.token_id),
            **plan.storage_fields(payload),
            **captured_stamp.fields("captured_at"),
            "revision": revision,
            "supersedes_revision": revision - 1 if revision > 1 else None,
            "market_truth_id": truth.get("mta_id"),
            "market_truth": truth,
            **checkpoint_data,
            "config_version": self.versions.config_version,
            "code_version": self.versions.code_version,
        }
        async with self.repo.transaction():
            await self.repo.put("out.reaction_checkpoints", key, record, immutable=True)
            if target is not None:
                await self._record_reaction_change(
                    target,
                    ReactionChangeState.model_validate(
                        {"changed": True, "revision": revision, "data_fingerprint": fingerprint}
                    ),
                )
        stored = await self.repo.get("out.reaction_checkpoints", key)
        if stored is None:
            raise ValueError("Reaction checkpoint is absent after successful storage")
        return captured_reaction_receipt(key, stored)

    async def scout_proposal(self, hook, concept, evidence, explanation, now):
        """Record an observation, never change the canonical taxonomy or thresholds."""
        from tank.util import sha256

        evidence = sorted(set(evidence))
        if not evidence:
            return None
        async with self.repo.lock("scout:auto:" + sha256(concept.casefold())):
            matching = [
                p
                async for p in self.repo.iter_all("scout.expansion_proposals", filters={"hook": hook})
                if p["concept_name"].casefold() == concept.casefold()
            ]
            known = {ref for p in matching for ref in p.get("evidence", [])}
            if not set(evidence) - known:
                return matching[-1] if matching else None
            existing = next((p for p in matching if not p.get("human_action")), None)
            proposal_id = existing["prp_id"] if existing else new_id("prp")
            proposal = existing or {
                "prp_id": proposal_id,
                "hook": hook,
                "concept_name": concept,
                "definition": explanation,
                "why_existing_fails": explanation,
                "mechanism": "Observed detector evidence requires adversarial review before any canonical change.",
                "overlap": "Existing categories must be considered first by the scout reviewer.",
                "recommendation": "SUBCATEGORY",
                "confidence": "LOW",
                "occurrences": 0,
                "evidence": [],
                **collection_clock(now).fields("created_at"),
                "created_by": "scheduler",
                "config_version": self.versions.config_version,
                "code_version": self.versions.code_version,
            }
            proposal.update(
                occurrences=proposal["occurrences"] + 1,
                evidence=sorted(set(proposal["evidence"]) | set(evidence)),
                **collection_clock(now).fields("last_observed_at"),
            )
            await self.repo.put("scout.expansion_proposals", proposal_id, proposal)
            await self.repo.enqueue(
                "SCOUT_REVIEW",
                {"prp_id": proposal_id},
                f"{proposal_id}:{proposal['occurrences']}",
                priority=4,
                max_attempts=2,
                expires_at=offset_schedule(collection_clock(now), timedelta(days=7)),
            )
            return proposal

    async def detect_scout_hooks(self, now):
        """Bound MTA reads by observation time; resume metadata changes durably."""
        from collections import defaultdict

        from tank.util import sha256

        end = now + timedelta(microseconds=1)
        boundary = now - timedelta(hours=24)
        checkpoint = await self.repo.get("ops.scheduler_checkpoints", "scout_hooks")
        since = parse_time(checkpoint["at"]) if checkpoint else None
        recent = {}
        pairs = defaultdict(set)

        def ref(row):
            return row.get("mta_id") or "truth:" + sha256(row)

        async for truth in self.repo.iter_range(
            "ana.market_truth", "cutoff", start=boundary + timedelta(microseconds=1), end=end, batch_size=100
        ):
            token = truth["token_id"]
            state = recent.setdefault(token, {"all_unknown": True, "evidence": []})
            state["all_unknown"] = (
                state["all_unknown"]
                and truth.get("move_class") == "UNKNOWN"
                and (truth.get("data_confidence") or {}).get("class") in {"MEDIUM", "HIGH"}
            )
            state["evidence"].append(ref(truth))
            if truth.get("event_id"):
                pairs[truth["event_id"]].add(token)
        for token, state in recent.items():
            baseline = await self.repo.latest_before(
                "ana.market_truth",
                "cutoff",
                boundary + timedelta(microseconds=1),
                filters={"token_id": token},
            )
            if (
                baseline
                and state["all_unknown"]
                and baseline.get("move_class") == "UNKNOWN"
                and (baseline.get("data_confidence") or {}).get("class") in {"MEDIUM", "HIGH"}
            ):
                await self.scout_proposal(
                    "MARKET_TRUTH_STATE",
                    "MARKET_TRUTH_STATE:" + token,
                    [ref(baseline), *state["evidence"]],
                    "Every available token analysis across the preceding 24 hours is UNKNOWN despite non-LOW Data Confidence.",
                    now,
                )
        changed = set(pairs)
        if since is not None:
            for collection, time_field in [
                ("evt.ems_assessments", "cutoff"),
                ("ana.contradictions", "detected_at"),
            ]:
                async for row in self.repo.iter_range(
                    collection, time_field, start=since, end=end, batch_size=100
                ):
                    if row.get("event_id"):
                        changed.add(row["event_id"])
        registered = {r["source_id"] for r in self.settings.load_config("sources")["sources"]}
        seen = set()

        async def candidates():
            async for event in self.active_events(now):
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
            if since is None:
                stream = self.repo.iter_all("evt.events")
            else:
                stream = self.repo.iter_range("evt.events", "known_to_us_at", start=since, end=end)
            async for event in stream:
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
            for event_id in changed - seen:
                event = await self.repo.get("evt.events", event_id)
                if event is not None:
                    yield event

        async for event in candidates():
            event_id = event["event_id"]
            contradictions = [
                c
                async for c in self.repo.iter_all("ana.contradictions", filters={"event_id": event_id})
                if not c.get("resolved_at")
                and c.get("type") in {"UNRESOLVED", "DIAGNOSTIC"}
                and (not c.get("detected_at") or parse_time(c["detected_at"]) <= now)
            ]
            tokens = set(pairs.get(event_id, set())) | {
                c["token_id"] for c in contradictions if c.get("token_id")
            }
            tokens.update(t for t in event.get("tokens_mentioned", []) if isinstance(t, str))
            async for edge in self.repo.iter_all("evt.beneficiary_edges", filters={"event_id": event_id}):
                if edge.get("token_id"):
                    tokens.add(edge["token_id"])
            for token in tokens:
                relevant = [c for c in contradictions if c.get("token_id") in {None, token}]
                last_two = await self.repo.list_latest(
                    "ana.market_truth",
                    "cutoff",
                    filters={"event_id": event_id, "token_id": token},
                    end=end,
                    limit=2,
                )
                persistent = len(last_two) == 2 and all(
                    (r.get("divergence") or {}).get("state") == "UNKNOWN" for r in last_two
                )
                evidence = [c["contradiction_id"] for c in relevant] if len(relevant) > 1 else []
                if persistent:
                    evidence += [ref(r) for r in last_two]
                if evidence:
                    await self.scout_proposal(
                        "ALTERNATIVE_EXPLANATION",
                        f"ALTERNATIVE_EXPLANATION:{event_id}:{token}",
                        evidence,
                        "More than one unresolved/diagnostic contradiction or repeated consecutive UNKNOWN divergence requires an alternative-explanation review.",
                        now,
                    )
            assessment = await self.latest_assessment(event)
            major = assessment and assessment.get("class") in {"MAJOR", "STRUCTURAL_SHOCK"}
            evidence = []
            public, known = event.get("public_at"), event.get("known_to_us_at")
            if major and public and known and parse_time(known) - parse_time(public) > timedelta(hours=1):
                evidence.append(event_id)
            observations = self.repo.iter_range(
                "evt.event_observations", "observed_at", filters={"event_id": event_id}, end=end
            )
            first = None
            first_time = None
            primary_refs = []
            async for observation in observations:
                item = await self.repo.get("ingest.source_items", observation["item_id"])
                if item is None:
                    continue
                if first is None:
                    first = item
                    first_time = (
                        item.get("published_at") or observation.get("observed_at") or item.get("observed_at")
                    )
                if (
                    item.get("source_class") == "PRIMARY"
                    and item.get("source_id") in registered
                    and item.get("published_at")
                ):
                    primary_refs.append((item["item_id"], item["published_at"]))
            if first and first.get("source_class") in {"SECONDARY", "SOCIAL_LEAD"} and first_time:
                for item_id, published_at in primary_refs:
                    if parse_time(published_at) < parse_time(first_time):
                        evidence += [first["item_id"], item_id]
            if evidence:
                await self.scout_proposal(
                    "SOURCE_OPPORTUNITY",
                    "SOURCE_OPPORTUNITY:" + event_id,
                    evidence,
                    "A MAJOR+ event arrived more than one hour after publication or a secondary/social observation preceded discovery of an earlier registered primary publication.",
                    now,
                )
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "scout_hooks",
            {
                "checkpoint_id": "scout_hooks",
                **collection_clock(now).fields("at"),
                "state": {"criteria": "TDD10"},
                "config_version": self.versions.config_version,
                "code_version": self.versions.code_version,
            },
        )

    async def demote(self, now):
        hot = [row async for row in self.repo.iter_all("mkt.hot_tokens")]
        if hot:
            await self.market.refresh_hot_expiry(now=now)
        for row in hot:
            protected = await self.market.hot_protected(row["event_id"], row["token_id"], now=now)
            checkpoints = await self.repo.list(
                "out.reaction_checkpoints",
                filters={"event_id": row["event_id"], "token_id": row["token_id"], "checkpoint": "POST_72H"},
                limit=1,
            )
            if checkpoints and not protected:
                await self.market.demote(row["token_id"], row["event_id"])
        hot = [row async for row in self.repo.iter_all("mkt.hot_tokens")]
        tokens = {r["token_id"] for r in hot}
        for row in sorted(hot, key=lambda r: (r.get("priority", 0), r["since"])):
            if len(tokens) <= self.thresholds.hot_token_cap:
                break
            if row["token_id"] in tokens:
                await self.market.demote(row["token_id"])
                tokens.remove(row["token_id"])

    async def heartbeat(self, now):
        notice_id = f"heartbeat_{now.date()}"
        if await self.repo.get("out.ops_notices", notice_id):
            return
        end = now + timedelta(microseconds=1)
        count = 0
        async for row in self.repo.iter_range(
            "out.deliveries", "at", start=now - timedelta(days=1), end=end, filters={"status": "SENT"}
        ):
            count += bool(row.get("alert_id"))
        hot_tokens = {row["token_id"] async for row in self.repo.iter_all("mkt.hot_tokens")}
        lag = 0
        backlog = {"queued": 0, "running": 0}
        analysis_kinds = {
            "FILTER_ITEM",
            "ROUTE_ITEM",
            "AGENT_MESSAGE",
            "ESCALATE_TOKENS",
            "RUN_MARKET_TRUTH",
            "EVAL_CONDITIONS",
            "MILESTONE_CHECK",
            "REACTION_CAPTURE",
            "SCOUT_REVIEW",
        }
        for status in ("QUEUED", "RUNNING"):
            async for row in self.repo.iter_all("ops.jobs", filters={"status": status}):
                backlog[status.lower()] += row["type"] in analysis_kinds
                if status == "QUEUED":
                    lag = max(lag, max(0, (now - parse_time(row["run_after"])).total_seconds()))
        spend, unknown = Decimal(0), 0
        async for row in self.repo.iter_range(
            "ops.llm_usage", "at", start=now.replace(hour=0, minute=0, second=0, microsecond=0), end=end
        ):
            if row.get("cost_usd") is None:
                unknown += 1
            else:
                spend += Decimal(str(row["cost_usd"]))
        spending_label = (
            f"known subtotal ${spend}; unknown cost records={unknown}" if unknown else f"${spend}"
        )
        failed_ops_cards = len(
            [1 async for _ in self.repo.iter_all("out.ops_cards", filters={"delivery_status": "FAILED"})]
        )
        ops_delivery = (
            f"; Ops: не удалось доставить или обновить {failed_ops_cards} карточек"
            if failed_ops_cards
            else ""
        )
        async with self.repo.transaction():
            await self.repo.put(
                "out.ops_notices",
                notice_id,
                {
                    "notice_id": notice_id,
                    "kind": "HEARTBEAT",
                    "text": f"Tank 24h: alerts={count}; hot tokens={len(hot_tokens)}; queue lag={lag:.0f}s; analysis backlog queued={backlog['queued']}, running={backlog['running']}; LLM spending={spending_label}{ops_delivery}",
                    **collection_clock(now).fields("created_at"),
                    "created_by": "scheduler",
                    "analysis_backlog": backlog,
                    "queue_lag_s": lag,
                },
                immutable=True,
            )
            await self.repo.enqueue(
                "DELIVER",
                {"notice_id": notice_id, "channel": "telegram:ops"},
                notice_id,
                priority=0,
                expires_at=offset_schedule(collection_clock(now), timedelta(days=1)),
                max_attempts=5,
            )

    @staticmethod
    def backup_identity(directory: str | Path, day: date) -> BackupIdentity:
        from pathlib import Path

        directory = Path(directory).resolve()

        def identity(name: str) -> BackupFileIdentity | None:
            try:
                stat = (directory / name).stat()
            except FileNotFoundError:
                return None
            return BackupFileIdentity(
                device=stat.st_dev, inode=stat.st_ino, size=stat.st_size, modified_ns=stat.st_mtime_ns
            )

        return BackupIdentity(
            directory=str(directory),
            day=day,
            manifest=identity("manifest.json"),
            complete=identity("COMPLETE"),
        )

    async def maintenance(self, now, *, force=False, prune=True):
        import os
        import shutil
        from pathlib import Path
        from urllib.parse import unquote, urlsplit

        backup_dir = self.settings.backup_dir
        if backup_dir is None:
            raise ValueError("TANK_BACKUP_DIR must point to the operator external backup disk")
        if not force and now.hour < self.settings.backup_hour_utc:
            return {"status": "deferred", "reason": "before configured backup hour"}
        target = Path(backup_dir)
        if not target.is_dir():
            raise ValueError("Configured external backup directory is unavailable")
        final = target / f"tank-{now.date()}"
        cache_key = self.backup_identity(final, now.date())
        self._verified_backups = {key for key in self._verified_backups if key.day == now.date()}
        if (final / "COMPLETE").exists():
            if force or cache_key not in self._verified_backups:
                await asyncio.to_thread(
                    verify_backup, final, protected=protected_evidence_paths(self.settings)
                )
                self._verified_backups.add(cache_key)
            if prune:
                await self.retention(now, final)
            await self.prune_snapshots(target)
            return {"status": "verified", "path": str(final), "retention": prune}
        staging = target / f".tank-{now.date()}.partial"
        if staging.exists():
            await asyncio.to_thread(shutil.rmtree, staging)
        staging.mkdir(exist_ok=True)
        import json

        async def dump_database(url, name):
            connection = urlsplit(url)
            env = os.environ.copy()
            for key, value in {
                "PGHOST": connection.hostname,
                "PGPORT": connection.port,
                "PGUSER": unquote(connection.username or ""),
                "PGPASSWORD": unquote(connection.password or ""),
                "PGDATABASE": unquote(connection.path.lstrip("/")),
            }.items():
                if value:
                    env[key] = str(value)
                else:
                    env.pop(key, None)
            proc = await asyncio.create_subprocess_exec(
                "pg_dump",
                "--format=custom",
                "--file",
                str(staging / name),
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(600):
                    await proc.communicate()
            except (asyncio.CancelledError, TimeoutError):
                proc.terminate()
                await proc.wait()
                raise
            if proc.returncode:
                raise RuntimeError(f"Nightly pg_dump failed with status {proc.returncode}")

        await dump_database(self.settings.database_url, "tank.dump")
        await dump_database(self.settings.openma_database_url, "oma.dump")
        data_root = Path(self.settings.data_dir).resolve()
        if target.resolve().is_relative_to(data_root):
            raise ValueError("Backup destination must be outside live data directory")

        roots = {"bundles": data_root / "bundles"}
        protected = protected_evidence_paths(self.settings)
        import threading

        cancelled = threading.Event()

        def copy_evidence():
            import hashlib

            files = {
                name: file_digest(staging / name, cancelled=cancelled) for name in ("tank.dump", "oma.dump")
            }
            for label, root in roots.items():
                if not root.exists():
                    continue
                if root.is_symlink() or not root.is_dir():
                    raise ValueError(f"Required backup input is not a directory: {label}")
                destination = staging / label
                for source in sorted(root.rglob("*")):
                    if cancelled.is_set():
                        raise RuntimeError("Backup interrupted before completion")
                    if is_protected_evidence_path(source, protected):
                        continue
                    if source.is_symlink():
                        raise ValueError("Backup evidence roots must not contain symlinks")
                    if source.is_file():
                        output = destination / source.relative_to(root)
                        output.parent.mkdir(parents=True, exist_ok=True)
                        digest = hashlib.sha256()
                        with source.open("rb") as source_file, output.open("wb") as destination_file:
                            while chunk := source_file.read(1024 * 1024):
                                if cancelled.is_set():
                                    raise RuntimeError("Backup interrupted before completion")
                                destination_file.write(chunk)
                                digest.update(chunk)
                        shutil.copystat(source, output)
                        files[output.relative_to(staging).as_posix()] = digest.hexdigest()
            manifest = {
                "version": 1,
                **collection_clock(now).fields("created_at"),
                "reference_base": str(Path.cwd()),
                "roots": {label: str(root) for label, root in roots.items()},
                "files": files,
                "database_dumps": ["tank.dump", "oma.dump"],
                "scope": "pg_dump and bundles/ (HLD §§5,8)",
                "excluded": sorted(protected),
                "restore": "Restore bundles to the recorded root or resolve historical file references with resolve_backup_reference. Restore tank/oma dumps into isolated databases; preserve OpenMA root secrets separately.",
            }
            (staging / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2))
            verify_backup(staging, require_complete=False, protected=protected, cancelled=cancelled)
            return manifest

        copy_task = asyncio.create_task(asyncio.to_thread(copy_evidence))
        try:
            await asyncio.shield(copy_task)
        except asyncio.CancelledError:
            cancelled.set()
            await asyncio.gather(copy_task, return_exceptions=True)
            raise
        (staging / "COMPLETE").write_text(iso(now) + "\n")
        (staging / "COMPLETE.json").write_text(json.dumps(collection_clock(now).fields("completed_at")))
        if final.exists():
            raise RuntimeError("Incomplete backup destination already exists")
        staging.rename(final)
        self._verified_backups.add(self.backup_identity(final, now.date()))
        if prune:
            await self.retention(now, final)
        await self.prune_snapshots(target)
        return {"status": "created", "path": str(final), "retention": prune}

    async def prune_snapshots(self, target):
        """Keep the newest verified snapshots only (lead ruling 2026-10-20, thresholds)."""
        from tank.backup import prune_backup_snapshots

        removed = await asyncio.to_thread(
            prune_backup_snapshots, target, int(self.thresholds.backup_snapshots_kept)
        )
        if removed:
            log.info("Removed old backup snapshots: %s", ", ".join(removed))
        return removed

    async def retention(self, now, backup):
        from pathlib import Path

        marker = Path(backup) / "RETENTION_COMPLETE"
        if marker.exists():
            return []
        import json

        market_cutoff = offset_schedule(
            collection_clock(now), -timedelta(days=self.thresholds.base_retention_days)
        )
        jobs_cutoff = offset_schedule(
            collection_clock(now), -timedelta(days=self.thresholds.jobs_retention_days)
        )
        book_cutoff = parse_time(now) - timedelta(hours=self.thresholds.book_summary_retention_hours)
        trades_cutoff = parse_time(now) - timedelta(days=self.thresholds.trades_retention_days)
        await self.repo.prune_market(
            market_cutoff.value, book_before=book_cutoff, trades_before=trades_cutoff
        )
        await self.repo.prune_jobs(jobs_cutoff.value)
        marker.with_suffix(".json").write_text(
            json.dumps(
                {
                    **collection_clock(now).fields("completed_at"),
                    **market_cutoff.fields("market_cutoff"),
                    **jobs_cutoff.fields("jobs_cutoff"),
                }
            )
        )
        marker.write_text(iso(now) + "\n")
        return []

    async def lifecycle(self, now):
        async for candidate in self.active_events(
            now, states=("TRACKED", "COOLDOWN", "MONITORING", "ALERTED"), include_recent_terminal=False
        ):
            event_id = candidate["event_id"]

            async with self.repo.lock("event:" + event_id), self.repo.transaction():
                event = await self.repo.get("evt.events", event_id)
                if event and event.get("lifecycle") in {"TRACKED", "COOLDOWN", "MONITORING", "ALERTED"}:
                    await self._advance_lifecycle(event, now)

    async def _advance_lifecycle(self, event, now):
        state = event["lifecycle"]
        event_id = event["event_id"]
        review_pending = False
        if state in {"TRACKED", "COOLDOWN"}:
            review_pending = any(
                [
                    row.get("expr", {}).get("review_at")
                    and row.get("expires_at")
                    and parse_time(row["expires_at"]) > now
                    async for row in self.repo.iter_all(
                        "ana.watch_conditions", filters={"event_id": event_id, "status": "ARMED"}
                    )
                ]
            )
        observations = await self.repo.list_latest(
            "evt.event_observations",
            "observed_at",
            filters={"event_id": event_id},
            end=now + timedelta(microseconds=1),
            limit=1,
        )
        last = max(
            [event.get("known_to_us_at", iso(now))]
            + ([event["review_requested_at"]] if event.get("review_requested_at") else [])
            + [o["observed_at"] for o in observations if o.get("observed_at")]
        )
        if state == "ALERTED":
            await self.repo.put(
                "evt.events",
                event_id,
                {**event, "lifecycle": "MONITORING", **collection_clock(now).fields("monitoring_at")},
            )
        elif (
            (
                state == "TRACKED"
                and not review_pending
                and (
                    not event.get("next_milestone")
                    or event["next_milestone"].get("status") in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}
                )
                and now - parse_time(last) >= timedelta(days=self.thresholds.event_close_days)
            )
            or state == "COOLDOWN"
            and not review_pending
            and now
            - max(
                parse_time(event.get("cooldown_at", last)),
                parse_time(event.get("review_requested_at", event.get("cooldown_at", last))),
            )
            >= timedelta(days=self.thresholds.event_close_days)
        ):
            await self.repo.put(
                "evt.events",
                event_id,
                {**event, "lifecycle": "CLOSED", **collection_clock(now).fields("closed_at")},
            )
        elif state == "MONITORING":
            calls = await self.repo.list(
                "ana.calls", filters={"event_id": event_id, "status": "OPEN"}, limit=1
            )
            armed = await self.repo.list(
                "ana.watch_conditions", filters={"event_id": event_id, "status": "ARMED"}, limit=1
            )
            checkpoints = [
                row
                async for row in self.repo.iter_all(
                    "out.reaction_checkpoints", filters={"event_id": event_id, "checkpoint": "POST_72H"}
                )
            ]
            targets = await self.analysis_tokens(event)
            captured = {r["token_id"] for r in checkpoints}
            if targets <= captured and not calls and not armed:
                await self.repo.put(
                    "evt.events",
                    event_id,
                    {**event, "lifecycle": "COOLDOWN", **collection_clock(now).fields("cooldown_at")},
                )


def metric_value(truth, event, metric, window):
    spot = truth.get("spot", {}).get(window, {})
    derivative_pack = truth.get("derivatives", {})
    deriv = (
        derivative_pack.get("windows", {}).get(window, {})
        if "windows" in derivative_pack
        else derivative_pack
    )
    sync = deriv.get("synchronized") or {}

    def cohort_share(side):
        value = spot.get("cw_breadth", {}).get(side)
        exact = spot.get("exact_majority", {})
        if side in exact:
            return {"kind": "EXACT_STRICT_MAJORITY", "value": value, "strict_majority": exact[side]}
        return value

    values = {
        "event.state": event.get("event_state"),
        "event.contradiction_fatal": event.get("contradiction_fatal"),
        "milestone.outcome": (event.get("next_milestone") or {}).get("outcome"),
        "spot.cohort.direction": spot.get("direction"),
        "spot.cohort.cwb_buy": cohort_share("buy"),
        "spot.cohort.cwb_sell": cohort_share("sell"),
        "spot.classification": spot.get("classification"),
        "deriv.native_oi_chg_pct": sync.get("oi_native_chg_pct"),
        "deriv.native_oi_vs_normal": sync.get("native_oi_vs_normal"),
        "deriv.oi_class": sync.get("oi_class"),
        "deriv.funding_vs_normal": funding_state(deriv),
        "mt.move_class": truth.get("move_class"),
        "mt.divergence_state": truth.get("divergence", {}).get("state"),
        "dc.class": truth.get("data_confidence", {}).get("class"),
        "price.repricing_band": truth.get("relative", {}).get("repricing_band"),
        "price.close_vs_level": truth.get("relative", {}).get("close_1h_price"),
        "price.vs_anchor_pct": truth.get("relative", {}).get("windows", {}).get("EVENT", {}).get("r_raw"),
        "liq.depth_p1_vs_pre": depth_ratio(truth.get("liquidity", {})),
        "liq.spread_ratio": truth.get("liquidity", {}).get("spread_ratio"),
    }
    return values.get(metric, field(truth, metric))


def depth_ratio(liquidity):
    current, pre = liquidity.get("aggregate_depth_p1_usd"), liquidity.get("pre_event_depth_p1_usd")
    if current is None or pre is None:
        return None
    current, pre = Decimal(str(current)), Decimal(str(pre))
    if not pre:
        return None
    with localcontext() as context:
        context.prec = max(
            128,
            len(current.as_tuple().digits)
            + len(pre.as_tuple().digits)
            + abs(current.adjusted() - pre.adjusted())
            + 32,
        )
        return current / pre


def funding_state(derivatives):
    values = [r.get("funding_vs_normal") for r in derivatives.get("per_venue", [])]
    if not values or any(v is None for v in values):
        return None
    return "ABOVE" if "ABOVE" in values else "BELOW" if all(v == "BELOW" for v in values) else "WITHIN"


def file_digest(path: Path, *, cancelled: Event | None = None) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError("Backup interrupted before completion")
            digest.update(chunk)
    return digest.hexdigest()


def backup_file(root: Path, relative: str) -> Path:
    candidate = root / relative
    if any(parent.is_symlink() for parent in (candidate, *candidate.parents) if parent != root):
        raise ValueError("Backup file contains a symlink")
    path = candidate.resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("Backup file is missing or outside backup root")
    return path


def verify_backup(
    directory: str | Path,
    *,
    require_complete: bool = True,
    protected: Mapping[str, Path] | None = None,
    cancelled: Event | None = None,
) -> BackupManifest:
    """Verify the relative-path recovery manifest before restoring evidence."""
    root = Path(directory).resolve()
    if require_complete and not (root / "COMPLETE").is_file():
        raise ValueError("Backup is incomplete")
    manifest = BackupManifest.from_bytes((root / "manifest.json").read_bytes())
    if protected is not None:
        for relative in manifest.files:
            parts = Path(relative).parts
            if parts[0] in manifest.roots:
                original = Path(manifest.roots[parts[0]]).joinpath(*parts[1:])
                if is_protected_evidence_path(original, protected):
                    raise ValueError(
                        "Backup contains a protected credential path and cannot be used as ordinary evidence"
                    )
    for relative, digest in manifest.files.items():
        if file_digest(backup_file(root, relative), cancelled=cancelled) != digest:
            raise ValueError("Backup file checksum mismatch")
    return manifest


def resolve_backup_reference(
    directory: str | Path, reference: str | Path, *, manifest: BackupManifest | None = None
) -> Path:
    """Map an original evidence path into its checksum-verified recovery tree."""
    root = Path(directory).resolve()
    admitted = (
        BackupManifest.from_bytes((root / "manifest.json").read_bytes())
        if manifest is None
        else BackupManifest.model_validate(manifest)
    )
    original = Path(reference)
    if not original.is_absolute():
        original = Path(admitted.reference_base) / original
    original = original.resolve()
    for label, source_root in admitted.roots.items():
        source = Path(source_root)
        if original.is_relative_to(source):
            relative = (Path(label) / original.relative_to(source)).as_posix()
            if relative not in admitted.files:
                raise ValueError("Evidence reference is missing from backup")
            path = backup_file(root, relative)
            if file_digest(path) != admitted.files[relative]:
                raise ValueError("Backup file checksum mismatch")
            return path
    raise ValueError("Evidence reference is outside backed-up roots")


class BackupProtectionSettings(Protocol):
    codex_auth_path: Path


def protected_evidence_paths(settings: BackupProtectionSettings) -> dict[str, Path]:
    """Credentials are never ordinary evidence/retention inputs."""
    return {"subscription sign-in directory": settings.codex_auth_path.resolve().parent}


def is_protected_evidence_path(path: str | Path, protected: Mapping[str, Path]) -> bool:
    from pathlib import Path

    resolved = Path(path).resolve()
    return any(resolved.is_relative_to(root) for root in protected.values())
