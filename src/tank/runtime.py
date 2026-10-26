"""The single Tank asyncio process and its durable worker pool."""

import asyncio
import contextlib
import logging
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from pydantic import JsonValue

from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.contracts.jobs import JobRetryState
from tank.contracts.progress import JobFailureProgressView
from tank.contracts.question_retirement import (
    QuestionFailureRefusalRequest,
    QuestionFailureRefusalSink,
    UnprovedQuestionRetirement,
)
from tank.contracts.session_admission import ADMITTED_JOB_TYPES, AdmissionDeferred
from tank.delivery import DeliveryDeferred, DeliveryService
from tank.ops_progress import record_job_failure_progress
from tank.question_answers import record_answer
from tank.request_budget import BudgetBackend, RequestBudgetScope, SharedRequestBudgets
from tank.runtime_contracts import (
    EscalateTokensJob,
    FailedAnswerNotification,
    FailedDeliveryNotification,
    FailedQuestionNotification,
    FailureJournalUnavailable,
    FailureReason,
    FailureReconciliationItem,
    JobStateFailureReason,
    MarketTruthJob,
    ScoutReviewJob,
    SuppliedFailureReason,
    TerminalFailureHandled,
    TerminalFailureOutcome,
    TerminalFailureRefused,
    TerminalJobFailure,
    failure_reason_text,
    replace_job_clock,
)
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import Repository
from tank.time_precision import Timestamp, collection_clock, offset_schedule, propagate_timestamp
from tank.util import parse_time, utcnow

if TYPE_CHECKING:
    from tank.agents.pipeline import Pipeline
    from tank.agents.sessions import SessionService
    from tank.market.service import MarketService
    from tank.telegram_inbox import TelegramInbox


class RuntimeFailureRepository(QuestionFailureRefusalSink, Protocol):
    async def execute(
        self, failure: TerminalJobFailure, operation: Callable[[], Awaitable[TerminalFailureOutcome]]
    ) -> TerminalFailureOutcome: ...

    async def mark(self, item: FailureReconciliationItem) -> bool: ...

    def pending(self, *, progress_requested: bool) -> AsyncIterator[FailureReconciliationItem]: ...


class RuntimeService(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...
    async def close(self) -> None: ...


class GatewayPort(Protocol):
    async def preflight(self) -> None: ...
    async def start(self) -> None: ...
    async def close(self) -> None: ...


class MarketTruthPort(Protocol):
    async def assess_for_job(
        self, event_id: str, token_id: str, cutoff: Timestamp | None
    ) -> MarketTruthAssessmentRef: ...


log = logging.getLogger(__name__)

TIMEOUT_SCALE = 15
SPEC_TIMEOUTS = {
    "FILTER_ITEM": 10,
    "ROUTE_ITEM": 20,
    "AGENT_MESSAGE": 10,
    "ESCALATE_TOKENS": 10,
    "RUN_MARKET_TRUTH": 90,
    "DELIVER": 10,
    "EVAL_CONDITIONS": 20,
    "MILESTONE_CHECK": 90,
    "REACTION_CAPTURE": 120,
    "SCOUT_REVIEW": 300,
    "TELEGRAM_ANSWER": 10,
}
TIMEOUTS = {kind: seconds * TIMEOUT_SCALE for kind, seconds in SPEC_TIMEOUTS.items()}


ADMISSION_TIMEOUT_S = 10 * TIMEOUT_SCALE


REPAIR_BUDGET_S = 45
REPAIR_LOOKBACK = timedelta(hours=24)
CAPTURE_WAIT_S = 15


class Runtime:
    def __init__(
        self,
        repo: Repository,
        settings: Settings,
        *,
        sources: RuntimeService,
        market: "MarketService",
        truth: "MarketTruthPort",
        pipeline: "Pipeline",
        sessions: "SessionService",
        delivery: DeliveryService,
        scheduler: Scheduler,
        gateway: GatewayPort,
        inbox: "TelegramInbox",
        budget_scope: RequestBudgetScope,
        budget_backend: BudgetBackend,
        bootstrap: Callable[[], Awaitable[None]],
        validate: Callable[[], None],
        failures: RuntimeFailureRepository,
    ) -> None:
        self.repo, self.settings = repo, settings
        self.sources, self.market, self.truth = sources, market, truth
        self.pipeline, self.sessions = pipeline, sessions
        self.delivery, self.scheduler, self.gateway, self.inbox = delivery, scheduler, gateway, inbox
        self._bootstrap, self._validate = bootstrap, validate
        self.failures = failures
        self._failure_reconcile_attempted_at: datetime | None = None
        self.budget_scope, self.budget_backend = budget_scope, budget_backend
        self.budget_binding: SharedRequestBudgets | None = None
        self.active_jobs: set[str] = set()
        self.handlers = {
            "FILTER_ITEM": pipeline.filter_item_wire,
            "ROUTE_ITEM": pipeline.route_item_wire,
            "AGENT_MESSAGE": sessions.send_message,
            "ESCALATE_TOKENS": self.escalate_wire,
            "RUN_MARKET_TRUTH": self.assess_wire,
            "DELIVER": self.delivery.deliver,
            "EVAL_CONDITIONS": self.scheduler.eval_conditions,
            "MILESTONE_CHECK": self.scheduler.milestone_check,
            "REACTION_CAPTURE": self.scheduler.reaction_capture,
            "SCOUT_REVIEW": self.scout_review,
            "TELEGRAM_ANSWER": self.inbox.answer,
        }

    def validate(self) -> None:
        self._validate()
        self.sessions.config(role="ANALYST", replay=self.settings.replay)
        self.sessions.running_session_cap()

    async def prepare(self) -> None:
        self.validate()
        await self.gateway.preflight()

    async def escalate_wire(self, payload: dict[str, JsonValue]) -> None:
        await self.escalate(EscalateTokensJob.model_validate(payload))

    async def assess_wire(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        assessment = await self.assess(MarketTruthJob.model_validate(payload))
        return assessment.model_dump(mode="json")

    async def escalate(self, payload: EscalateTokensJob) -> None:
        await self.market.escalate(payload.event_id, payload.tokens)

    async def assess(self, payload: MarketTruthJob) -> MarketTruthAssessmentRef:
        from tank.market.capture import event_anchor

        event = await self.repo.get("evt.events", payload.event_id)
        if event is None:
            raise ValueError("Market truth event does not exist")
        anchor = event_anchor(event)
        prerequisite = await self.market.capture_readiness(
            payload.event_id, payload.token_id, anchor=anchor.timestamp
        )
        if prerequisite.status not in {"PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED"}:
            raise RuntimeError("Required durable event capture was not scheduled")
        if prerequisite.status in {"PENDING", "RUNNING"}:
            await self.market.wait_capture(payload.event_id, payload.token_id, timeout_s=CAPTURE_WAIT_S)
        cutoff = (
            Timestamp(
                payload.cutoff, payload.cutoff_prec, payload.cutoff_prec_reason, payload.cutoff_bucket_anchor
            )
            if payload.cutoff is not None
            else None
        )
        anchor_time = parse_time(anchor.value)
        end = parse_time(payload.cutoff) if payload.cutoff is not None else utcnow()
        await self.market.repair_windows(
            payload.token_id, min(anchor_time, end) - REPAIR_LOOKBACK, end, budget_s=REPAIR_BUDGET_S
        )
        return await self.truth.assess_for_job(payload.event_id, payload.token_id, cutoff)

    async def scout_review(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        reference = await self.sessions.scout_review(ScoutReviewJob.model_validate(payload))
        return reference.model_dump(mode="json")

    async def worker(self, stop: asyncio.Event, number: int = 0) -> None:
        if not hasattr(self, "active_jobs"):
            self.active_jobs = set()
        while not stop.is_set():
            try:
                job = await self.repo.claim_job()
            except Exception:
                log.exception("Worker %s failed to claim a job", number)
                await self.pause(stop, 1)
                continue
            if job is None:
                await self.reconcile_delivery_failures()
                await self.pause(stop, 0.25)
                continue
            self.active_jobs.add(job["job_id"])
            try:
                if job.get("expires_at") and parse_time(job["expires_at"]) <= utcnow():
                    expired = await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {
                            **replace_job_clock(job, "finished_at", collection_clock(utcnow())),
                            "status": "EXPIRED",
                        },
                    )
                    await self.terminal_delivery_failure_wire(
                        expired, JobStateFailureReason(kind="STATE", status="EXPIRED")
                    )
                    continue
                dependencies = [
                    await self.repo.get("ops.jobs", dep) for dep in job["payload"].get("_depends_on", [])
                ]
                if any(dep is None or dep["status"] in ("FAILED", "EXPIRED") for dep in dependencies):
                    raise RuntimeError("Durable job dependency failed or is missing")
                if any(dep["status"] != "DONE" for dep in dependencies):
                    await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {
                            **replace_job_clock(
                                job,
                                "run_after",
                                offset_schedule(collection_clock(utcnow()), timedelta(seconds=0.25)),
                            ),
                            "status": "QUEUED",
                            "attempts": max(0, job["attempts"] - 1),
                        },
                    )
                    continue
                kind = job["type"]
                if kind not in self.handlers:
                    raise ValueError(f"Unregistered durable job kind: {kind}")
                if kind in ADMITTED_JOB_TYPES:
                    try:
                        async with asyncio.timeout(ADMISSION_TIMEOUT_S):
                            await self.sessions.admit(job)
                    except TimeoutError as slow:
                        raise AdmissionDeferred(
                            offset_schedule(collection_clock(utcnow()), timedelta(seconds=5))
                        ) from slow
                async with asyncio.timeout(TIMEOUTS[kind]):
                    result = await self.handlers[kind](job["payload"])
                async with self.repo.transaction():
                    await self.repo.put("ops.jobs", job["job_id"], {**job, "result": result})
                    await self.repo.finish_job(job["job_id"])
            except DeliveryDeferred as deferred:
                async with self.repo.transaction():
                    current = await self.repo.get("ops.jobs", job["job_id"])
                    if current and current["status"] == "RUNNING":
                        await self.repo.put(
                            "ops.jobs",
                            job["job_id"],
                            {
                                **replace_job_clock(current, "run_after", deferred.not_before),
                                "status": "QUEUED",
                                "attempts": max(0, current["attempts"] - 1),
                            },
                        )
            except AdmissionDeferred as deferred:
                async with self.repo.transaction():
                    current = await self.repo.get("ops.jobs", job["job_id"])
                    if current and current["status"] == "RUNNING":
                        requeued = replace_job_clock(current, "run_after", deferred.not_before)
                        if current.get("slot_waiting_since") is None:
                            requeued = replace_job_clock(
                                requeued, "slot_waiting_since", propagate_timestamp(current, "created_at")
                            )
                        await self.repo.put(
                            "ops.jobs",
                            job["job_id"],
                            {
                                **requeued,
                                "status": "QUEUED",
                                "attempts": max(0, current["attempts"] - 1),
                            },
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failure = exc.__cause__ if isinstance(exc, TimeoutError) and exc.__cause__ else exc
                frames = " > ".join(
                    f"{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}:{frame.name}"
                    for frame in traceback.extract_tb(failure.__traceback__)
                )

                log.error(
                    "Job %s (%s) failed: %s [%s]", job["job_id"], job["type"], type(exc).__name__, frames
                )
                try:
                    await self.repo.fail_job(
                        job["job_id"],
                        type(exc).__name__,
                        retry_after=getattr(exc, "retry_after", min(60, 2 ** job.get("attempts", 1))),
                    )
                    current = await self.repo.get("ops.jobs", job["job_id"])
                    if current and current["status"] in {"FAILED", "EXPIRED"}:
                        await self.terminal_delivery_failure_wire(
                            current, SuppliedFailureReason(kind="SUPPLIED", value=type(exc).__name__)
                        )
                except Exception:
                    log.exception("Could not persist job failure: %s", job["job_id"])
                    await self.pause(stop, 1)
            finally:
                self.active_jobs.discard(job["job_id"])

    async def terminal_delivery_failure_wire(
        self, job: dict[str, JsonValue], reason: FailureReason
    ) -> TerminalFailureOutcome:
        failure = TerminalJobFailure.from_record(
            job,
            reason=reason,
            progress_requested=self.settings.ops_progress_enabled and not self.settings.replay,
            notifications_requested=True,
        )
        return await self.terminal_delivery_failure(failure)

    async def terminal_delivery_failure(self, failure: TerminalJobFailure) -> TerminalFailureOutcome:
        """Apply only actual terminal queue scope and phase-specific notification commands."""
        failure = TerminalJobFailure.model_validate(failure, by_name=True)

        async def apply() -> TerminalFailureOutcome:
            reason = failure_reason_text(failure.reason)
            if isinstance(failure.progress, JobFailureProgressView):
                await record_job_failure_progress(self.repo, self.settings, failure.progress, reason)
            notification = failure.notification
            if isinstance(notification, FailedDeliveryNotification):
                await self.delivery.terminal_failure(notification.payload, reason, notification.attempt)
            elif isinstance(notification, FailedQuestionNotification):
                try:
                    abandoned = await self.sessions.abandon_question(
                        notification.target.session_id, notification.target.question_id
                    )
                except UnprovedQuestionRetirement as unknown:
                    if unknown.witness.target != notification.target:
                        raise ValueError(
                            "Host refusal differs from its actual queued question target"
                        ) from None
                    request = QuestionFailureRefusalRequest(
                        job_id=failure.header.job_id,
                        job_status=failure.header.status,
                        witness=unknown.witness,
                    )
                    receipt = await self.failures.record(request)
                    if receipt.request != request:
                        raise ValueError("Host refusal journal returned a different question scope") from None
                    return TerminalFailureRefused(status="REFUSED", receipt=receipt)
                if abandoned:
                    await record_answer(self.repo, notification.target, None, status="FAILED")
            elif isinstance(notification, FailedAnswerNotification):
                await self.inbox.answer_failed(notification.payload, reason)
            return TerminalFailureHandled(status="HANDLED")

        return await self.failures.execute(failure, apply)

    async def reconcile_delivery_failures(self) -> None:
        now = utcnow()
        attempted = self._failure_reconcile_attempted_at
        if attempted is not None and now - attempted < timedelta(seconds=30):
            return
        self._failure_reconcile_attempted_at = now
        try:
            async for item in self.failures.pending(
                progress_requested=self.settings.ops_progress_enabled and not self.settings.replay
            ):
                try:
                    outcome = await self.terminal_delivery_failure(item.failure)
                    if isinstance(outcome, TerminalFailureHandled):
                        await self.failures.mark(item)
                except FailureJournalUnavailable:
                    log.warning("Terminal failure reconciliation deferred: persistence unavailable")
        except FailureJournalUnavailable:
            log.warning("Terminal failure scan deferred: persistence unavailable")

    @staticmethod
    async def pause(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), seconds)

    async def supervise(self, name: str, service: RuntimeService, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await service.run(stop)
                if not stop.is_set():
                    raise RuntimeError("Long-lived service returned unexpectedly")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s loop failed; restarting", name)
                await self.pause(stop, 1)

    async def bootstrap(self) -> None:
        await self._bootstrap()

    async def run(self, stop: asyncio.Event) -> None:
        self.validate()
        await self.bootstrap()
        await self.repo.recover_jobs()
        await self.reconcile_delivery_failures()
        try:
            await self.gateway.start()
        except Exception:
            await self.close()
            raise

        self.budget_binding = self.budget_scope.bind(self.budget_backend)
        intake_stop, market_stop, sessions_stop, workers_stop = [asyncio.Event() for _ in range(4)]
        intake_services: tuple[tuple[str, RuntimeService], ...] = (
            ("sources", self.sources),
            ("scheduler", self.scheduler),
            ("telegram", self.inbox),
        )
        support_services: tuple[tuple[str, RuntimeService, asyncio.Event], ...] = (
            ("market", self.market, market_stop),
            ("sessions", self.sessions, sessions_stop),
        )
        producers = [
            asyncio.create_task(self.supervise(name, service, intake_stop), name=name)
            for name, service in intake_services
        ]
        support = [
            asyncio.create_task(self.supervise(name, service, flag), name=name)
            for name, service, flag in support_services
        ]
        workers = [asyncio.create_task(self.worker(workers_stop, n), name=f"worker-{n}") for n in range(8)]
        tasks = producers + support + workers
        try:
            await stop.wait()
        finally:
            intake_stop.set()
            await self.scheduler.close()
            deadline = asyncio.get_running_loop().time() + self.settings.shutdown_grace_s
            drained = False
            try:
                while asyncio.get_running_loop().time() < deadline:
                    now = utcnow()
                    due = False
                    async for job in self.repo.iter_range(
                        "ops.jobs",
                        "run_after",
                        end=now + timedelta(microseconds=1),
                        filters={"status": "QUEUED"},
                    ):
                        drain_state = JobRetryState.from_document(job)
                        if drain_state.expires_at is None or parse_time(drain_state.expires_at) > now:
                            due = True
                            break
                    busy = False
                    async for session in self.repo.iter_all("ops.agent_sessions", filters={"status": "OPEN"}):
                        if session.get("runtime_status") not in {None, "IDLE"}:
                            busy = True
                            break
                    if not due and not self.active_jobs and not busy and all(t.done() for t in producers):
                        drained = True
                        break
                    await asyncio.sleep(0.05)
                if not drained:
                    log.warning(
                        "Drain deadline reached with %s durable jobs still active; unfinished session work and jobs will resume from durable state",
                        len(self.active_jobs),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Drain status check failed; preserving unfinished durable jobs for recovery")
            finally:
                workers_stop.set()
                market_stop.set()
                sessions_stop.set()
                remaining = max(0, deadline - asyncio.get_running_loop().time())
                _, pending = await asyncio.wait(tasks, timeout=remaining)
                interrupted = set(self.active_jobs)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                for job_id in interrupted:
                    try:
                        async with self.repo.lock("job:" + job_id):
                            interrupted_job = await self.repo.get("ops.jobs", job_id)
                            state = (
                                JobRetryState.from_document(interrupted_job)
                                if interrupted_job is not None
                                else None
                            )
                            if (
                                interrupted_job is not None
                                and state is not None
                                and state.status == "RUNNING"
                            ):
                                if state.attempts == 0:
                                    raise RuntimeError("Running job has no consumed attempt to refund")
                                interrupted_at = collection_clock(utcnow())
                                await self.repo.put(
                                    "ops.jobs",
                                    job_id,
                                    {
                                        **replace_job_clock(
                                            replace_job_clock(interrupted_job, "run_after", interrupted_at),
                                            "interrupted_at",
                                            interrupted_at,
                                        ),
                                        "status": "QUEUED",
                                        "attempts": state.attempts - 1,
                                        "interruption_reason": "CONTROLLED_GENERATION_STOP",
                                    },
                                )
                    except Exception:
                        log.exception(
                            "Could not requeue interrupted job %s; startup recovery is required", job_id
                        )
                await self.close()

    async def close(self) -> None:
        for service in (
            self.scheduler,
            self.sources,
            self.market,
            self.sessions,
            self.pipeline,
            self.delivery,
            self.inbox,
            self.gateway,
        ):
            try:
                await service.close()
            except Exception:
                log.exception("Resource close failed")
        binding = self.budget_binding
        if binding is not None:
            self.budget_binding = None
            await self.budget_scope.release(binding)
