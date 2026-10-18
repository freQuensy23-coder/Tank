"""The single Tank asyncio process and its durable worker pool."""

import asyncio
import logging
from datetime import timedelta

from tank.delivery import DeliveryService
from tank.scheduler import Scheduler
from tank.util import iso, parse_time, utcnow

log = logging.getLogger(__name__)
TIMEOUTS = {
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


class PendingBackfill(RuntimeError):
    def __init__(self, backfill_id):
        super().__init__("Durable event capture is still pending")
        self.backfill_id = backfill_id
        self.run_after = iso(utcnow() + timedelta(seconds=0.25))


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
        if prerequisite["status"] in {"PENDING", "RUNNING"}:
            raise PendingBackfill(prerequisite["backfill_id"])
        if prerequisite["status"] not in {"COMPLETE", "PARTIAL", "FAILED"}:
            raise RuntimeError("Required durable event capture was not scheduled")
        return await self.truth.assess(payload["event_id"], payload["token_id"], payload.get("cutoff"))

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
                    # claim_job atomically expires queued jobs; this also protects custom repositories.
                    await self.repo.put(
                        "ops.jobs", job["job_id"], {**job, "status": "EXPIRED", "finished_at": iso()}
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
                            "run_after": iso(utcnow() + timedelta(seconds=0.25)),
                        },
                    )
                    continue
                kind = job["type"]
                if kind not in self.handlers:
                    raise ValueError(f"Unregistered durable job kind: {kind}")
                async with asyncio.timeout(TIMEOUTS[kind]):
                    result = await self.handlers[kind](job["payload"])
                async with self.repo.transaction():
                    await self.repo.put("ops.jobs", job["job_id"], {**job, "result": result})
                    await self.repo.finish_job(job["job_id"])
            except asyncio.CancelledError:
                # Remains RUNNING and is recovered on the next process start.
                raise
            except Exception as exc:
                if getattr(exc, "run_after", None):
                    await self.repo.put(
                        "ops.jobs",
                        job["job_id"],
                        {
                            **job,
                            "status": "QUEUED",
                            "run_after": exc.run_after,
                            "attempts": max(0, job["attempts"] - 1),
                            "last_deferred_reason": type(exc).__name__,
                        },
                    )
                    continue
                log.error("Job %s (%s) failed: %s", job["job_id"], job["type"], type(exc).__name__)
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
        # claim/recovery can terminate a job without entering its handler.
        for status in ("FAILED", "EXPIRED"):
            async for job in self.repo.iter_all("ops.jobs", filters={"type": "DELIVER", "status": status}):
                if not job.get("delivery_failure_recorded_at"):
                    await self.terminal_delivery_failure(job, status)
                    await self.repo.put(
                        "ops.jobs", job["job_id"], {**job, "delivery_failure_recorded_at": iso()}
                    )

    @staticmethod
    async def pause(stop, seconds):
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except TimeoutError:
            pass

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
        await self.repo.maintain_partitions()

    async def run(self, stop=None):
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
                # A controlled generation swap is not a failed execution attempt.
                # Preserve the final retry even when its old handler needed cancellation.
                for job_id in interrupted:
                    try:
                        async with self.repo.lock("job:" + job_id):
                            job = await self.repo.get("ops.jobs", job_id)
                            if job and job["status"] == "RUNNING":
                                from tank.util import iso

                                await self.repo.put(
                                    "ops.jobs",
                                    job_id,
                                    {
                                        **job,
                                        "status": "QUEUED",
                                        "attempts": max(0, job["attempts"] - 1),
                                        "run_after": iso(),
                                        "interrupted_at": iso(),
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
