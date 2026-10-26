"""Real Runtime resources with observed loop boundaries for owned-task lifecycle tests."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, Protocol

import pytest
from runtime_failure_fixtures import runtime_failure_fixture

from tank.runtime import Runtime, RuntimeService
from tank.storage import StorageBackend

type ServiceName = Literal[
    "sources", "scheduler", "market", "sessions", "pipeline", "delivery", "inbox", "gateway"
]


class Closable(Protocol):
    async def close(self) -> None: ...


class ServiceLifetime:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.closed = asyncio.Event()

    async def run(self, stop: asyncio.Event) -> None:
        self.started.set()
        await stop.wait()
        self.stopped.set()


class LifecycleRuntime:
    def __init__(self, runtime: Runtime) -> None:
        self.runtime = runtime
        self.gateway_start = asyncio.Event()
        self.services: dict[ServiceName, ServiceLifetime] = {}

    def observe(self, name: ServiceName, service: Closable, patch: pytest.MonkeyPatch) -> ServiceLifetime:
        lifetime = ServiceLifetime()
        self.services[name] = lifetime
        close = service.close

        async def observed_close() -> None:
            await close()
            lifetime.closed.set()

        patch.setattr(service, "close", observed_close)
        return lifetime

    async def start_gateway(self) -> None:
        self.gateway_start.set()


@asynccontextmanager
async def lifecycle_runtime(
    repo: StorageBackend, directory: Path, *, shutdown_grace_s: int
) -> AsyncIterator[LifecycleRuntime]:
    async with runtime_failure_fixture(
        repo, directory, ops_progress_enabled=False, shutdown_grace_s=shutdown_grace_s
    ) as runtime:
        observed = LifecycleRuntime(runtime)
        with pytest.MonkeyPatch.context() as patch:
            services: tuple[tuple[ServiceName, RuntimeService], ...] = (
                ("sources", runtime.sources),
                ("scheduler", runtime.scheduler),
                ("market", runtime.market),
                ("sessions", runtime.sessions),
                ("inbox", runtime.inbox),
            )
            for name, service in services:
                lifetime = observed.observe(name, service, patch)
                patch.setattr(service, "run", lifetime.run)
            observed.observe("pipeline", runtime.pipeline, patch)
            observed.observe("delivery", runtime.delivery, patch)
            observed.observe("gateway", runtime.gateway, patch)
            patch.setattr(runtime.gateway, "start", observed.start_gateway)
            yield observed
