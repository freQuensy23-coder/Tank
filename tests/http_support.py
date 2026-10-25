"""Configured HTTP admission with an explicit backend and owned lifetime."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import SourceApiLimitsConfiguration
from tank.http_admission import ConfiguredHttpAdmission
from tank.request_budget import BudgetBackend, SharedRequestBudgets
from tank.settings import Settings


@asynccontextmanager
async def owned_http_admission(
    settings: Settings,
    versions: ResolvedVersionIdentity,
    backend: BudgetBackend,
) -> AsyncIterator[ConfiguredHttpAdmission]:
    budgets = SharedRequestBudgets(backend)
    try:
        yield ConfiguredHttpAdmission(
            SourceApiLimitsConfiguration.model_validate(settings.load_config("api_limits")),
            versions,
            budgets,
        )
    finally:
        await budgets.shutdown()
