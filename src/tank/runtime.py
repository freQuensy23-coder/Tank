"""The single Tank asyncio process and its durable worker pool."""

import asyncio
import contextlib
import logging
import traceback
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from pydantic import JsonValue

from tank.agents.message_dispatch import (
    DeclinedQuestionDispatch,
    MessageDispatchOutcomeRecord,
    MessageDispatchRequest,
)
from tank.agents.session_contracts import ScoutSessionReference
from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.contracts.jobs import InvalidJobClaim, JobRetryState
from tank.contracts.progress import JobFailureProgressView
from tank.contracts.question_retirement import (
    QuestionFailureRefusalRequest,
    QuestionFailureRefusalSink,
    UnprovedQuestionRetirement,
)
from tank.contracts.scheduler_results import ConditionEvaluationResult
from tank.contracts.session_admission import AdmissionDeferred
from tank.delivery import DeliveryDeferred, DeliveryService
from tank.ops_progress import record_job_failure_progress
from tank.question_answers import record_answer
from tank.request_budget import BudgetBackend, RequestBudgetScope, SharedRequestBudgets
from tank.runtime_admission import worker_admission
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
    UserQuestionMessage,
    failure_reason_text,
)
from tank.runtime_dispatch_contracts import (
    AgentMessageCommand,
    ClaimedWork,
    DeliverCommand,
    EscalateTokensCommand,
    EvalConditionsCommand,
    FilterItemCommand,
    MarketTruthCommand,
    MilestoneCheckCommand,
    ReactionCaptureCommand,
    RequeueRequest,
    RouteItemCommand,
    RunningJobClaim,
    ScoutReviewCommand,
    TelegramAnswerCommand,
    WaitingDependencies,
    WorkerFailureRequest,
)
from tank.runtime_evaluation import evaluate_condition_job
from tank.runtime_failure_commands import terminal_work_failure
from tank.runtime_market import RuntimeMarketPort
from tank.runtime_milestone import check_milestone_job
from tank.runtime_reaction import ReactionJobInvocation, capture_reaction_job
from tank.runtime_retry import worker_retry_schedule
from tank.runtime_worker_repository import RepositoryWorkerQueue
from tank.scheduler import REACTION_RETRY_S, Scheduler
from tank.settings import Settings
from tank.storage import Repository
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import parse_time, utcnow

if TYPE_CHECKING:
    from tank.agents.pipeline import Pipeline
    from tank.agents.sessions import SessionService
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
        market: RuntimeMarketPort,
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
        self.sources, self.truth = sources, truth
        self.market: RuntimeMarketPort = market
        self.pipeline, self.sessions = pipeline, sessions
        self.delivery, self.scheduler, self.gateway, self.inbox = delivery, scheduler, gateway, inbox
        self._bootstrap, self._validate = bootstrap, validate
        self.failures = failures
        self._failure_reconcile_attempted_at: datetime | None = None
        self.budget_scope, self.budget_backend = budget_scope, budget_backend
        self.budget_binding: SharedRequestBudgets | None = None
        self.active_claims: dict[tuple[str, str], ClaimedWork] = {}

    @property
    def active_jobs(self) -> frozenset[str]:
        return frozenset(work.job.job_id for work in self.active_claims.values())

    def snapshot_active_claims(self) -> tuple[RunningJobClaim, ...]:
        return tuple(work.scope() for work in self.active_claims.values())

    async def interrupt_claims(
        self, claims: Sequence[RunningJobClaim], observed_at: Timestamp
    ) -> tuple[RunningJobClaim, ...]:
        observed_at = Timestamp.model_validate(observed_at)
        if observed_at.value is None:
            raise ValueError("Controlled stop requires its actual observation clock")
        scopes = tuple(RunningJobClaim.model_validate(claim) for claim in claims)
        queue = RepositoryWorkerQueue(self.repo)
        acknowledged: list[RunningJobClaim] = []
        for claim in scopes:
            try:
                if await queue.interrupt(claim, observed_at):
                    acknowledged.append(claim)
            except Exception:
                log.exception("Interruption acknowledgment unavailable for job %s", claim.job_id)
        return tuple(acknowledged)

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

    async def eval_conditions_wire(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return await evaluate_condition_job(self.scheduler, payload)

    async def reaction_capture_wire(self, invocation: ReactionJobInvocation) -> dict[str, JsonValue]:
        return await capture_reaction_job(
            self.scheduler,
            self.repo,
            invocation,
            clock=collection_clock,
            retry_delay=timedelta(seconds=REACTION_RETRY_S),
        )

    async def milestone_check_wire(self, payload: dict[str, JsonValue]) -> None:
        await check_milestone_job(self.scheduler, payload)

    async def scout_review(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        reference = await self.sessions.scout_review(ScoutReviewJob.model_validate(payload))
        return reference.model_dump(mode="json")

    async def execute_work_wire(self, work: ClaimedWork) -> JsonValue:
        """Execute closed application commands and serialize only the durable result boundary."""
        work = ClaimedWork.model_validate(work, by_name=True)
        command = work.command
        match command:
            case FilterItemCommand():
                item = await self.pipeline.filter_item(command.payload)
                item = type(item).model_validate(item, by_name=True)
                if item.item_id != command.payload.item_id:
                    raise ValueError("Filter result differs from its actual requested source")
                return item.storage_record()
            case RouteItemCommand():
                routed = await self.pipeline.route_item(command.payload)
                routed = type(routed).model_validate(routed, by_name=True)
                if routed.item_id != command.payload.item_id:
                    raise ValueError("Routing result differs from its actual requested source")
                return routed.storage_record()
            case AgentMessageCommand():
                request = MessageDispatchRequest.from_wire(work.job.payload)
                outcome = MessageDispatchOutcomeRecord.model_validate(
                    await self.sessions.dispatch_message(request), by_name=True
                ).root
                if isinstance(outcome, DeclinedQuestionDispatch):
                    message = command.payload
                    if not isinstance(message, UserQuestionMessage) or (
                        outcome.target.question_id != message.question_id
                        or outcome.target.session_id != message.session_id
                    ):
                        raise ValueError("Question refusal differs from its actual requested target")
                elif outcome.message_id != work.payload_revision or (
                    command.payload.session_id is not None
                    and outcome.session_id != command.payload.session_id
                ):
                    raise ValueError("Message result differs from its actual requested input")
                return outcome.model_dump(mode="json")
            case EscalateTokensCommand():
                await self.escalate(command.payload)
                return None
            case MarketTruthCommand():
                assessment = MarketTruthAssessmentRef.model_validate(await self.assess(command.payload))
                if (
                    assessment.event_id != command.payload.event_id
                    or assessment.token_id != command.payload.token_id
                ):
                    raise ValueError("Market truth result differs from its actual requested subject")
                if command.payload.cutoff is not None and parse_time(assessment.cutoff) != parse_time(
                    command.payload.cutoff
                ):
                    raise ValueError("Market truth result differs from its actual requested cutoff")
                return assessment.model_dump(mode="json")
            case DeliverCommand():
                await self.delivery.deliver(command.payload)
                return None
            case EvalConditionsCommand():
                payload = command.payload
                cutoff = Timestamp(
                    payload.cutoff,
                    payload.cutoff_prec,
                    payload.cutoff_prec_reason,
                    payload.cutoff_bucket_anchor,
                )
                result = ConditionEvaluationResult.model_validate(
                    await self.scheduler.eval_conditions(cutoff)
                )
                return result.model_dump(mode="json")
            case MilestoneCheckCommand():
                await self.scheduler.milestone_check(command.payload.command())
                return None
            case ReactionCaptureCommand():
                return await self.reaction_capture_wire(ReactionJobInvocation.from_envelope(work.job))
            case ScoutReviewCommand():
                scout = ScoutSessionReference.model_validate(
                    await self.sessions.scout_review(command.payload)
                )
                if scout.proposal_id != command.payload.proposal_id:
                    raise ValueError("Scout result differs from its actual requested proposal")
                return scout.model_dump(mode="json")
            case TelegramAnswerCommand():
                await self.inbox.answer(command.payload)
                return None
        raise AssertionError("Closed durable command has no executor")

    async def worker(self, stop: asyncio.Event, number: int = 0) -> None:
        queue = RepositoryWorkerQueue(self.repo)
        while not stop.is_set():
            try:
                work = await queue.claim()
            except Exception:
                log.exception("Worker %s failed to claim a job", number)
                await self.pause(stop, 1)
                continue
            if isinstance(work, InvalidJobClaim):
                try:
                    await queue.reject_invalid(work)
                except Exception:
                    log.exception("Worker %s could not persist invalid queue quarantine", number)
                    await self.pause(stop, 1)
                continue
            if work is None:
                await self.reconcile_delivery_failures()
                await self.pause(stop, 0.25)
                continue
            claim = work.scope()
            owner = (claim.job_id, claim.claim_token)
            self.active_claims[owner] = work
            try:
                observed = collection_clock()
                if (
                    work.job.expires_at is not None
                    and observed.value is not None
                    and parse_time(work.job.expires_at) <= parse_time(observed.value)
                ):
                    if await queue.expire(claim, observed):
                        terminal = await queue.terminal(claim)
                        if terminal is not None:
                            await self.terminal_delivery_failure(
                                terminal_work_failure(
                                    work,
                                    terminal,
                                    JobStateFailureReason(kind="STATE", status="EXPIRED"),
                                    progress_requested=self.settings.ops_progress_enabled
                                    and not self.settings.replay,
                                    notifications_requested=True,
                                )
                            )
                    continue
                if not await queue.current(claim):
                    continue
                dependencies = await queue.dependencies(work)
                if isinstance(dependencies, WaitingDependencies):
                    await queue.requeue(
                        RequeueRequest(
                            claim=claim,
                            reason="DEPENDENCY_WAIT",
                            not_before=offset_schedule(collection_clock(), timedelta(seconds=0.25)),
                        )
                    )
                    continue
                candidate = worker_admission(work)
                if candidate is not None:
                    try:
                        async with asyncio.timeout(ADMISSION_TIMEOUT_S):
                            await self.sessions.admit_candidate(candidate)
                    except TimeoutError as slow:
                        raise AdmissionDeferred(
                            offset_schedule(collection_clock(), timedelta(seconds=5))
                        ) from slow
                if not await queue.current(claim):
                    continue
                async with asyncio.timeout(TIMEOUTS[work.command.type]):
                    result_wire = await self.execute_work_wire(work)
                await queue.complete(claim, result_wire, collection_clock())
            except DeliveryDeferred as deferred:
                try:
                    await queue.requeue(
                        RequeueRequest(claim=claim, not_before=deferred.not_before, reason="DELIVERY_WAIT")
                    )
                except Exception:
                    log.exception("Could not persist deferred job %s", claim.job_id)
                    await self.pause(stop, 1)
            except AdmissionDeferred as deferred:
                try:
                    await queue.requeue(
                        RequeueRequest(
                            claim=claim, not_before=deferred.not_before, reason="SESSION_ADMISSION"
                        )
                    )
                except Exception:
                    log.exception("Could not persist admission wait for job %s", claim.job_id)
                    await self.pause(stop, 1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failure = exc.__cause__ if isinstance(exc, TimeoutError) and exc.__cause__ else exc
                frames = " > ".join(
                    f"{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}:{frame.name}"
                    for frame in traceback.extract_tb(failure.__traceback__)
                )
                log.error("Job %s (%s) failed: %s [%s]", claim.job_id, claim.kind, type(exc).__name__, frames)
                try:
                    terminal = await queue.fail(
                        WorkerFailureRequest(
                            claim=claim,
                            error_class=type(exc).__name__,
                            schedule=worker_retry_schedule(exc, claim.attempts),
                            observed_at=collection_clock(),
                        )
                    )
                    if terminal is not None and terminal.status in {"FAILED", "EXPIRED"}:
                        await self.terminal_delivery_failure(
                            terminal_work_failure(
                                work,
                                terminal,
                                SuppliedFailureReason(kind="SUPPLIED", value=type(exc).__name__),
                                progress_requested=self.settings.ops_progress_enabled
                                and not self.settings.replay,
                                notifications_requested=True,
                            )
                        )
                except Exception:
                    log.exception("Could not persist job failure: %s", claim.job_id)
                    await self.pause(stop, 1)
            finally:
                self.active_claims.pop(owner, None)

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
                interrupted = self.snapshot_active_claims()
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                await self.interrupt_claims(interrupted, collection_clock())
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
