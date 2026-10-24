"""Explicit complete source fixtures derived from the checked-in publisher registry."""

from collections.abc import Mapping
from functools import cache

from pydantic import JsonValue

from tank.contracts.sources import HttpFetchPlan, SourceConfiguration, SourceDefinition, SourceFeed
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
