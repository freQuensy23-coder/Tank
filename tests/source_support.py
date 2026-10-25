"""Explicit complete source fixtures derived from the checked-in publisher registry."""

import asyncio
import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from functools import cache

from pydantic import JsonValue

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.sources import (
    HttpFetchPlan,
    SourceApiLimitsConfiguration,
    SourceConfiguration,
    SourceDefinition,
    SourceFeed,
)
from tank.http_admission import ConfiguredHttpAdmission
from tank.request_budget import BudgetClock, MemoryBudgetBackend, RequestBudgets, SharedRequestBudgets
from tank.settings import Settings


@cache
def baseline_sources() -> SourceConfiguration:
    return SourceConfiguration.model_validate(Settings().load_config("sources"))


def source_definition(source: SourceDefinition | Mapping[str, JsonValue]) -> SourceDefinition:
    if isinstance(source, SourceDefinition):
        return source
    sid = source["source_id"]
    if not isinstance(sid, str):
        raise ValueError("Source fixture requires a source ID")
    original = next(row for row in baseline_sources().sources if row.source_id == sid)
    values: dict[str, JsonValue] = original.model_dump(mode="json")
    for field in ("author_organization", "endpoint_verification", "paid_gap_ref", "detail_url"):
        values.pop(field, None)
    values.update(source)
    feeds = values["feeds"]
    if not isinstance(feeds, list):
        raise ValueError("Source fixture feeds must be a list")
    declared: list[JsonValue] = []
    for feed in feeds:
        if not isinstance(feed, dict):
            raise ValueError("Source fixture feed must be an object")
        declared.append({"kind": "rss", "interval_s": original.interval_s, **feed})
    values["feeds"] = declared
    return SourceDefinition.model_validate(values)


def source_configuration(config: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    sources = config["sources"]
    if not isinstance(sources, list):
        raise ValueError("Source fixture registry must be a list")
    definitions: list[JsonValue] = []
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("Source fixture definition must be an object")
        definitions.append(source_definition(source).model_dump(mode="json"))
    return {
        "version": baseline_sources().version,
        "contact_email": baseline_sources().contact_email,
        **config,
        "sources": definitions,
    }


def source_feed(feed: Mapping[str, JsonValue]) -> SourceFeed:
    return SourceFeed.model_validate({"kind": "rss", "interval_s": 60, **feed})


def fetch_plan(plan: Mapping[str, JsonValue]) -> HttpFetchPlan:
    values = dict(plan)
    limits = values.get("source_limits")
    if isinstance(limits, dict):
        values["source_limits"] = {
            "rate_per_min": None,
            "budget_fraction": 0.5,
            "unknown_limit_policy": "Explicit offline test budget",
            **limits,
        }
    return HttpFetchPlan.model_validate(values)


def operational_configuration(name: str) -> dict[str, JsonValue]:
    """Use the actual versioned operational policy for synthetic source fixtures."""
    from tank.settings import Settings

    if name not in {"api_limits", "thresholds"}:
        raise ValueError("No operational fixture declared for " + name)
    return Settings().load_config(name)


def fixture_versions() -> ResolvedVersionIdentity:
    """Declare the known synthetic version identity of these isolated service fixtures."""
    return ResolvedVersionIdentity(config_version="test", code_version="test")


class FixtureBudgetClock:
    """An isolated operational clock; it never assigns event/publication provenance."""

    def __init__(self) -> None:
        self.elapsed = 0.0
        self.origin = datetime(2026, 10, 25, tzinfo=UTC)

    def monotonic(self) -> float:
        return self.elapsed

    def now(self) -> datetime:
        return self.origin + timedelta(seconds=self.elapsed)

    async def sleep(self, delay: float) -> None:
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("Fixture budget waits require finite nonnegative durations")
        self.elapsed += delay
        await asyncio.sleep(0)


def fixture_request_budgets(*, clock: BudgetClock | None = None) -> SharedRequestBudgets:
    """Explicit memory admission with deterministic elapsed time and real quota arithmetic."""
    return SharedRequestBudgets(MemoryBudgetBackend(FixtureBudgetClock() if clock is None else clock))


def fixture_http_admission(
    *,
    policy: SourceApiLimitsConfiguration | None = None,
    budgets: RequestBudgets | None = None,
    versions: ResolvedVersionIdentity | None = None,
) -> ConfiguredHttpAdmission:
    """Use the real configured resolver, never a fabricated admission/provider result."""
    actual = (
        SourceApiLimitsConfiguration.model_validate(Settings().load_config("api_limits"))
        if policy is None
        else policy
    )
    return ConfiguredHttpAdmission(
        actual,
        fixture_versions() if versions is None else versions,
        fixture_request_budgets() if budgets is None else budgets,
    )
