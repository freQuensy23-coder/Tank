"""The single Tank asyncio process and its durable worker pool."""

import asyncio
import contextlib
import logging
import traceback
from datetime import timedelta

from tank.agents.sessions import ADMITTED_JOB_TYPES, AdmissionDeferred
from tank.delivery import DeliveryDeferred, DeliveryService
from tank.scheduler import Scheduler
from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import parse_time, utcnow

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
}
TIMEOUTS = {kind: seconds * TIMEOUT_SCALE for kind, seconds in SPEC_TIMEOUTS.items()}


ADMISSION_TIMEOUT_S = 10 * TIMEOUT_SCALE


REPAIR_BUDGET_S = 45
REPAIR_LOOKBACK = timedelta(hours=24)
CAPTURE_WAIT_S = 15


class Runtime:
    def __init__(
        self,
        repo,
        settings,
        *,
        sources=None,
        market=None,
        truth=None,
        pipeline=None,
        sessions=None,
        event_tools=None,
        delivery=None,
        scheduler=None,
        gateway=None,
    ):
        from tank.logging import configure_logging

        configure_logging(settings)
        self.repo, self.settings = repo, settings
        self.active_jobs = set()
        if gateway is None:
            from tank.codex_gateway import CodexGateway

            gateway = CodexGateway(settings)
        self.gateway = gateway
        if sources is None:
            from tank.sources import SourceService

            sources = SourceService(repo, settings)
        if market is None:
            from tank.market.service import MarketService

            market = MarketService(repo, settings)
        if truth is None:
            from tank.analysis.truth import MarketTruthService

            truth = MarketTruthService(repo, settings, market)
        if pipeline is None:
            from tank.agents.pipeline import Pipeline

            pipeline = Pipeline(repo, settings)
        if event_tools is None:
            from tank.analysis.decisions import DecisionEngine
            from tank.event_tools import EventToolService

            event_tools = EventToolService(repo, settings, truth, DecisionEngine(settings))
        if sessions is None:
            from tank.agents.sessions import SessionService

            sessions = SessionService(repo, settings, event_tools.dispatch)
        self.sources, self.market, self.truth = sources, market, truth
        self.pipeline, self.sessions, self.event_tools = pipeline, sessions, event_tools
        self.delivery = delivery or DeliveryService(repo, settings)
        self.scheduler = scheduler or Scheduler(repo, settings, market, truth)
        if hasattr(self.event_tools, "scheduler"):
            self.event_tools.scheduler = self.scheduler
        self.handlers = {
            "FILTER_ITEM": pipeline.filter_item,
            "ROUTE_ITEM": pipeline.route_item,
            "AGENT_MESSAGE": sessions.send_message,
            "ESCALATE_TOKENS": self.escalate,
            "RUN_MARKET_TRUTH": self.assess,
            "DELIVER": self.delivery.deliver,
            "EVAL_CONDITIONS": self.scheduler.eval_conditions,
            "MILESTONE_CHECK": self.scheduler.milestone_check,
            "REACTION_CAPTURE": self.scheduler.reaction_capture,
            "SCOUT_REVIEW": self.scout_review,
        }

    def validate(self):
        self.validate_settings(self.settings)
        if hasattr(self.sessions, "config"):
            self.sessions.config()

    async def prepare(self):
        self.validate()
        await self.gateway.preflight()

    @staticmethod
    def validate_settings(settings):
        from tank.bootstrap import validate_registry

        if settings.workers != 8:
            raise ValueError("Tank production requires exactly eight workers")
        for name in ("sources", "registry", "semantics", "dependency_map", "thresholds"):
            settings.load_config(name)
        required = (
            "database_url",
            "openma_url",
            "openma_model",
            "telegram_token",
            "telegram_alerts_chat",
            "telegram_ops_chat",
        )
        missing = [name for name in required if not getattr(settings, name, None)]
        if missing:
            raise ValueError("Missing required service settings: " + ", ".join(missing))
        if not getattr(settings, "backup_dir", None):
            raise ValueError("TANK_BACKUP_DIR is required for nightly external backups")
        thresholds = settings.load_config("thresholds")
        if thresholds.get("filter_cutoff") is None or thresholds.get("routing_cutoff") is None:
            raise ValueError("Filter and routing cutoffs require calibration before live service startup")

        validate_registry(settings)

    async def escalate(self, payload):
        tokens = payload.get("tokens") or ([payload["token_id"]] if payload.get("token_id") else None)
        if not tokens:
            raise ValueError("ESCALATE_TOKENS requires tokens")
        await self.market.escalate(payload["event_id"], tokens)

    async def assess(self, payload):
        from tank.market.capture import event_anchor

        event = await self.repo.get("evt.events", payload["event_id"])
        if event is None:
            raise ValueError("Market truth event does not exist")
        prerequisite = await self.market.capture_readiness(
            payload["event_id"], payload["token_id"], anchor=event_anchor(event)["ts"]
        )
        if prerequisite["status"] not in {"PENDING", "RUNNING", "COMPLETE", "PARTIAL", "FAILED"}:
            raise RuntimeError("Required durable event capture was not scheduled")
        if prerequisite["status"] in {"PENDING", "RUNNING"}:
            await self.market.wait_capture(payload["event_id"], payload["token_id"], timeout_s=CAPTURE_WAIT_S)
        cutoff = propagate_timestamp(payload, "cutoff") if payload.get("cutoff") is not None else None
        if hasattr(self.market, "repair_windows"):
            anchor = parse_time(event_anchor(event)["ts"])
            end = parse_time(cutoff.value) if cutoff is not None and cutoff.value else utcnow()
            await self.market.repair_windows(
                payload["token_id"], min(anchor, end) - REPAIR_LOOKBACK, end, budget_s=REPAIR_BUDGET_S
            )
        return await self.truth.assess(payload["event_id"], payload["token_id"], cutoff)

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
        if job["type"] == "DELIVER":
            await self.delivery.terminal_failure(job["payload"], reason, job.get("attempts"))

    async def reconcile_delivery_failures(self):
        now = utcnow()
        if getattr(self, "_delivery_reconciled_at", None) and now - self._delivery_reconciled_at < timedelta(
            seconds=30
        ):
            return
        self._delivery_reconciled_at = now

        for status in ("FAILED", "EXPIRED"):
            async for job in self.repo.iter_all("ops.jobs", filters={"type": "DELIVER", "status": status}):
                if not job.get("delivery_failure_recorded_at"):
                    await self.terminal_delivery_failure(job, status)
                    await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {**job, **collection_clock(utcnow()).fields("delivery_failure_recorded_at")},
                    )

    @staticmethod
    async def pause(stop, seconds):
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), seconds)

    async def supervise(self, name, service, stop):
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

    async def bootstrap(self):
        from tank.bootstrap import sync_registry

        await sync_registry(self.repo, self.settings)
        await self.repo.maintain_partitions(include_history=False)

    async def run(self, stop=None):
        from tank.sources.rate_limits import shared_request_budgets

        stop = stop or asyncio.Event()
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
        producers = [
            asyncio.create_task(self.supervise(name, service, intake_stop), name=name)
            for name, service in [("sources", self.sources), ("scheduler", self.scheduler)]
        ]
        support = [
            asyncio.create_task(self.supervise(name, service, flag), name=name)
            for name, service, flag in [
                ("market", self.market, market_stop),
                ("sessions", self.sessions, sessions_stop),
            ]
        ]
        workers = [asyncio.create_task(self.worker(workers_stop, n), name=f"worker-{n}") for n in range(8)]
        tasks = producers + support + workers
        try:
            await stop.wait()
        finally:
            intake_stop.set()
            if hasattr(self.scheduler, "close"):
                await self.scheduler.close()
            deadline = asyncio.get_running_loop().time() + getattr(self.settings, "shutdown_grace_s", 30)
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
                            if job and job["status"] == "RUNNING":
                                interrupted_at = collection_clock(utcnow())
                                await self.repo.put(
                                    "ops.jobs",
                                    job_id,
                                    {
                                        **job,
                                        "status": "QUEUED",
                                        "attempts": max(0, job["attempts"] - 1),
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
            self.event_tools,
            self.gateway,
        ):
            if hasattr(service, "close"):
                try:
                    await service.close()
                except Exception:
                    log.exception("Resource close failed")
        from tank.sources.rate_limits import shared_request_budgets

        await shared_request_budgets.release(getattr(self, "request_ledger", None))


class RuntimeSupervisor:
    """Validate a complete candidate before draining and replacing the live generation."""

    def __init__(self, repo, settings, *, candidate_loader=None, factory=Runtime, validator=None):
        self.repo, self.settings = repo, settings
        self.factory, self.validator = factory, validator or Runtime.validate_settings
        self.candidate_loader = candidate_loader or self.load_candidate
        self.generation = None

    @staticmethod
    def load_candidate():
        from tank.settings import Settings, resolve_versions

        return resolve_versions(Settings())

    async def run(self, stop, reload_requested):
        self.validator(self.settings)
        runtime = self.factory(self.repo, self.settings)
        try:
            runtime.validate()
            if hasattr(runtime, "prepare"):
                await runtime.prepare()
        except Exception:
            await runtime.close()
            raise
        generation_stop = asyncio.Event()
        task = asyncio.create_task(runtime.run(generation_stop), name="tank-generation")
        self.generation = runtime
        try:
            while not stop.is_set():
                stop_wait = asyncio.create_task(stop.wait())
                reload_wait = asyncio.create_task(reload_requested.wait())
                done, _ = await asyncio.wait(
                    {task, stop_wait, reload_wait}, return_when=asyncio.FIRST_COMPLETED
                )
                for waiter in (stop_wait, reload_wait):
                    if waiter not in done:
                        waiter.cancel()
                await asyncio.gather(stop_wait, reload_wait, return_exceptions=True)
                if task in done:
                    await task
                    if not stop.is_set():
                        raise RuntimeError("Tank generation stopped unexpectedly")
                if stop.is_set():
                    break
                reload_requested.clear()
                candidate_runtime = None
                try:
                    candidate = await asyncio.to_thread(self.candidate_loader)
                    self.validator(candidate)
                    for field in ("database_url", "openma_url", "data_dir"):
                        if getattr(candidate, field, None) != getattr(self.settings, field, None):
                            raise ValueError(f"{field} changes require an explicit process restart")
                    candidate_runtime = self.factory(self.repo, candidate)
                    candidate_runtime.validate()
                    if hasattr(candidate_runtime, "prepare"):
                        await candidate_runtime.prepare()
                except Exception:
                    log.exception("SIGHUP candidate rejected; current generation continues")
                    if candidate_runtime is not None:
                        await candidate_runtime.close()
                    continue
                if stop.is_set():
                    await candidate_runtime.close()
                    break
                generation_stop.set()
                await task
                self.settings = candidate
                runtime = candidate_runtime
                self.generation = runtime
                generation_stop = asyncio.Event()
                task = asyncio.create_task(runtime.run(generation_stop), name="tank-generation")
                log.info("SIGHUP loaded validated configuration version %s", candidate.config_version)
        finally:
            generation_stop.set()
            await task
