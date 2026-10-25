"""Real reaction-scheduling composition with explicitly owned test resources."""

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

import httpx
from source_support import fixture_http_admission

from tank.analysis.truth import MarketTruthService
from tank.contracts.http_policy import HttpAdmission
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import SourceApiLimitsConfiguration
from tank.market.service import MarketService
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import Repository


@dataclass(frozen=True)
class OperationalReactionServices:
    market: MarketService
    truth: MarketTruthService
    scheduler: Scheduler
    admission: HttpAdmission


@asynccontextmanager
async def operational_reaction_services(
    repo: Repository,
    settings: Settings,
    *,
    versions: ResolvedVersionIdentity,
) -> AsyncIterator[OperationalReactionServices]:
    """Provide actual dependencies; undeclared external HTTP is a fixture failure."""
    requests = 0

    def refuse(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise AssertionError("Operational reaction fixture attempted undeclared external HTTP")

    try:
        async with AsyncExitStack() as resources:
            http = await resources.enter_async_context(
                httpx.AsyncClient(transport=httpx.MockTransport(refuse))
            )
            admission = fixture_http_admission(
                policy=SourceApiLimitsConfiguration.model_validate(settings.load_config("api_limits")),
                versions=versions,
            )
            market = MarketService(repo, settings, versions=versions, http=http, admission=admission)
            resources.push_async_callback(market.close)
            truth = MarketTruthService(repo, settings, market, versions=versions)
            scheduler = Scheduler(
                repo,
                settings,
                market,
                truth,
                thresholds=settings.thresholds(),
                versions=versions,
            )
            resources.push_async_callback(scheduler.close)
            yield OperationalReactionServices(
                market=market, truth=truth, scheduler=scheduler, admission=admission
            )
    finally:
        if requests:
            raise AssertionError("Operational reaction fixture attempted undeclared external HTTP")
