"""Production composition and validated generation replacement for the runtime core."""

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from tank.agents.pipeline import Pipeline
from tank.agents.sessions import SessionService
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.bootstrap import sync_registry, validate_registry
from tank.codex_gateway import CodexGateway
from tank.delivery import DeliveryService
from tank.event_tools import EventToolService
from tank.logging import configure_logging
from tank.market.service import MarketService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.storage import Repository
from tank.telegram_inbox import TelegramInbox

log = logging.getLogger(__name__)


class RuntimeGeneration(Protocol):
    def validate(self) -> None: ...
    async def prepare(self) -> None: ...
    async def run(self, stop: asyncio.Event) -> None: ...
    async def close(self) -> None: ...


class GenerationConfiguration(Protocol):
    database_url: str
    openma_url: str
    data_dir: Path
    config_version: str


def validate_runtime_settings(settings: Settings) -> None:
    if settings.workers != 8:
        raise ValueError("Tank production requires exactly eight workers")
    for name in ("sources", "registry", "semantics", "dependency_map", "thresholds"):
        settings.load_config(name)
    required = (
        ("database_url", settings.database_url),
        ("openma_url", settings.openma_url),
        ("openma_model", settings.openma_model),
        ("telegram_token", settings.telegram_token),
        ("telegram_alerts_chat", settings.telegram_alerts_chat),
        ("telegram_ops_chat", settings.telegram_ops_chat),
    )
    missing = [name for name, value in required if value is None or value == ""]
    if missing:
        raise ValueError("Missing required service settings: " + ", ".join(missing))
    if settings.backup_dir is None:
        raise ValueError("TANK_BACKUP_DIR is required for nightly external backups")
    settings.thresholds()
    validate_registry(settings)


def build_runtime(repo: Repository, settings: Settings) -> Runtime:
    validate_runtime_settings(settings)
    configure_logging(settings)
    market = MarketService(repo, settings)
    truth = MarketTruthService(repo, settings, market)
    sources = SourceService(repo, settings)
    pipeline = Pipeline(repo, settings)
    tools = EventToolService(repo, settings, truth, DecisionEngine(settings))
    inbox = TelegramInbox(repo, settings)
    sessions = SessionService(repo, settings, tools.dispatch)
    delivery = DeliveryService(repo, settings)
    scheduler = Scheduler(repo, settings, market, truth)
    gateway = CodexGateway(settings)
    sessions.telegram = inbox
    tools.scheduler = scheduler

    async def bootstrap_registry() -> None:
        await sync_registry(repo, settings)

    def validate_generation() -> None:
        validate_runtime_settings(settings)

    return Runtime(
        repo,
        settings,
        sources=sources,
        market=market,
        truth=truth,
        pipeline=pipeline,
        sessions=sessions,
        delivery=delivery,
        scheduler=scheduler,
        gateway=gateway,
        inbox=inbox,
        bootstrap=bootstrap_registry,
        validate=validate_generation,
    )


def load_candidate() -> Settings:
    return resolve_versions(Settings())


def build_supervisor(repo: Repository, settings: Settings) -> "RuntimeSupervisor[Repository, Settings]":
    return RuntimeSupervisor(
        repo,
        settings,
        candidate_loader=load_candidate,
        factory=build_runtime,
        validator=validate_runtime_settings,
    )


class RuntimeSupervisor[Repo, Config: GenerationConfiguration]:
    """Validate a complete candidate before draining and replacing the live generation."""

    def __init__(
        self,
        repo: Repo,
        settings: Config,
        *,
        candidate_loader: Callable[[], Config],
        factory: Callable[[Repo, Config], RuntimeGeneration],
        validator: Callable[[Config], None],
    ) -> None:
        self.repo, self.settings = repo, settings
        self.factory, self.validator, self.candidate_loader = factory, validator, candidate_loader
        self.generation: RuntimeGeneration | None = None

    async def run(self, stop: asyncio.Event, reload_requested: asyncio.Event) -> None:
        self.validator(self.settings)
        runtime = self.factory(self.repo, self.settings)
        try:
            runtime.validate()
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
                candidate_runtime: RuntimeGeneration | None = None
                try:
                    candidate = await asyncio.to_thread(self.candidate_loader)
                    self.validator(candidate)
                    immutable = (
                        ("database_url", candidate.database_url, self.settings.database_url),
                        ("openma_url", candidate.openma_url, self.settings.openma_url),
                        ("data_dir", candidate.data_dir, self.settings.data_dir),
                    )
                    for field, new_value, old_value in immutable:
                        if new_value != old_value:
                            raise ValueError(f"{field} changes require an explicit process restart")
                    candidate_runtime = self.factory(self.repo, candidate)
                    candidate_runtime.validate()
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
