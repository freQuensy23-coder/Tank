"""The single Tank asyncio process and its durable worker pool."""

import asyncio
import contextlib
import logging
import traceback
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol

from pydantic import JsonValue

from tank.agents.sessions import ADMITTED_JOB_TYPES, AdmissionDeferred
from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.contracts.jobs import JobRetryState
from tank.delivery import DeliveryDeferred, DeliveryService
from tank.ops_progress import job_failure_progress
from tank.runtime_contracts import EscalateTokensJob, MarketTruthJob
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import Repository
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import parse_time, utcnow

if TYPE_CHECKING:
    from tank.agents.pipeline import Pipeline
    from tank.agents.sessions import SessionService
    from tank.market.service import MarketService
    from tank.telegram_inbox import TelegramInbox


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
        bootstrap: Callable[[], Awaitable[None]],
        validate: Callable[[], None],
    ) -> None:
        self.repo, self.settings = repo, settings
        self.sources, self.market, self.truth = sources, market, truth
        self.pipeline, self.sessions = pipeline, sessions
        self.delivery, self.scheduler, self.gateway, self.inbox = delivery, scheduler, gateway, inbox
        self._bootstrap, self._validate = bootstrap, validate
        self.active_jobs: set[str] = set()
        self.handlers = {
            "FILTER_ITEM": pipeline.filter_item,
            "ROUTE_ITEM": pipeline.route_item,
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
        self.sessions.config()

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

    async def scout_review(self, payload):
        if not hasattr(self.sessions, "scout_review"):
            raise RuntimeError("SessionService.scout_review is required for SCOUT_REVIEW jobs")
        return await self.sessions.scout_review(payload)

    async def worker(self, stop, number=0):
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
                    await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {**job, "status": "EXPIRED", **collection_clock(utcnow()).fields("finished_at")},
                    )
                    await self.terminal_delivery_failure(job, "EXPIRED")
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
                            **job,
                            "status": "QUEUED",
                            "attempts": max(0, job["attempts"] - 1),
                            **offset_schedule(collection_clock(utcnow()), timedelta(seconds=0.25)).fields(
                                "run_after"
                            ),
                        },
                    )
                    continue
                kind = job["type"]
                if kind not in self.handlers:
                    raise ValueError(f"Unregistered durable job kind: {kind}")
                if kind in ADMITTED_JOB_TYPES and hasattr(getattr(self, "sessions", None), "admit"):
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
                                **current,
                                "status": "QUEUED",
                                "attempts": max(0, current["attempts"] - 1),
                                **deferred.not_before.fields("run_after"),
                            },
                        )
            except AdmissionDeferred as deferred:
                async with self.repo.transaction():
                    current = await self.repo.get("ops.jobs", job["job_id"])
                    if current and current["status"] == "RUNNING":
                        await self.repo.put(
                            "ops.jobs",
                            job["job_id"],
                            {
                                **current,
                                "status": "QUEUED",
                                "attempts": max(0, current["attempts"] - 1),
                                "slot_waiting_since": current.get("slot_waiting_since")
                                or current["created_at"],
                                **deferred.not_before.fields("run_after"),
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
                        await self.terminal_delivery_failure(current, type(exc).__name__)
                except Exception:
                    log.exception("Could not persist job failure: %s", job["job_id"])
                    await self.pause(stop, 1)
            finally:
                self.active_jobs.discard(job["job_id"])

    async def terminal_delivery_failure(self, job, reason):
        await job_failure_progress(self.repo, self.settings, job, reason)
        if job["type"] == "DELIVER":
            await self.delivery.terminal_failure(job["payload"], reason, job.get("attempts"))
        if job["type"] == "AGENT_MESSAGE" and job["payload"].get("kind") == "USER_QUESTION":
            from tank.telegram_inbox import record_answer

            abandon = getattr(getattr(self, "sessions", None), "abandon_question", None)
            if abandon is None or await abandon(
                job["payload"].get("session_id"), job["payload"]["question_id"]
            ):
                await record_answer(self.repo, job["payload"]["question_id"], None, status="FAILED")
        if job["type"] == "TELEGRAM_ANSWER" and getattr(self, "inbox", None):
            await self.inbox.answer_failed(job["payload"], reason)

    async def reconcile_delivery_failures(self):
        now = utcnow()
        if getattr(self, "_delivery_reconciled_at", None) and now - self._delivery_reconciled_at < timedelta(
            seconds=30
        ):
            return
        self._delivery_reconciled_at = now

        for status in ("FAILED", "EXPIRED"):
            for kind in ("FILTER_ITEM", "ROUTE_ITEM", "AGENT_MESSAGE", "RUN_MARKET_TRUTH", "ESCALATE_TOKENS"):
                async for job in self.repo.iter_all("ops.jobs", filters={"type": kind, "status": status}):
                    if not job.get("ops_progress_recorded_at"):
                        await job_failure_progress(
                            self.repo, self.settings, job, job.get("last_error") or status
                        )
                        await self.repo.put(
                            "ops.jobs",
                            job["job_id"],
                            {**job, **collection_clock(utcnow()).fields("ops_progress_recorded_at")},
                        )
            async for job in self.repo.iter_all("ops.jobs", filters={"type": "DELIVER", "status": status}):
                if not job.get("delivery_failure_recorded_at"):
                    await self.terminal_delivery_failure(job, status)
                    await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {**job, **collection_clock(utcnow()).fields("delivery_failure_recorded_at")},
                    )
            for kind in ("AGENT_MESSAGE", "TELEGRAM_ANSWER"):
                async for job in self.repo.iter_all("ops.jobs", filters={"type": kind, "status": status}):
                    if job.get("followup_failure_recorded_at"):
                        continue
                    if kind == "TELEGRAM_ANSWER" or job["payload"].get("kind") == "USER_QUESTION":
                        await self.terminal_delivery_failure(job, status)
                        await self.repo.put(
                            "ops.jobs",
                            job["job_id"],
                            {**job, **collection_clock(utcnow()).fields("followup_failure_recorded_at")},
                        )

    @staticmethod
    async def pause(stop, seconds):
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
        from tank.sources.rate_limits import shared_request_budgets

        self.validate()
        await self.bootstrap()
        await self.repo.recover_jobs()
        await self.reconcile_delivery_failures()
        try:
            await self.gateway.start()
        except Exception:
            await self.close()
            raise

        self.request_ledger = shared_request_budgets.configure(self.repo, self.settings)
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
                        if not job.get("expires_at") or parse_time(job["expires_at"]) > now:
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
                            job = await self.repo.get("ops.jobs", job_id)
                            state = JobRetryState.from_document(job) if job is not None else None
                            if job is not None and state is not None and state.status == "RUNNING":
                                if state.attempts == 0:
                                    raise RuntimeError("Running job has no consumed attempt to refund")
                                interrupted_at = collection_clock(utcnow())
                                await self.repo.put(
                                    "ops.jobs",
                                    job_id,
                                    {
                                        **job,
                                        "status": "QUEUED",
                                        "attempts": state.attempts - 1,
                                        **interrupted_at.fields("run_after"),
                                        **interrupted_at.fields("interrupted_at"),
                                        "interruption_reason": "CONTROLLED_GENERATION_STOP",
                                    },
                                )
                    except Exception:
                        log.exception(
                            "Could not requeue interrupted job %s; startup recovery is required", job_id
                        )
                await self.close()

    async def close(self):
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
        from tank.sources.rate_limits import shared_request_budgets

        await shared_request_budgets.release(getattr(self, "request_ledger", None))
