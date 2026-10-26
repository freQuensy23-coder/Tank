"""Actual native measurement admission and immutable ledger effects."""

import pytest
from pydantic import JsonValue, ValidationError
from test_storage import repo as repo

from tank.agents.native_usage import record_native_usage
from tank.contracts.native_usage import NativeUsageCounters, NativeUsageSession, NativeUsageSpan
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.storage import PostgresRepository, StorageBackend
from tank.time_precision import collection_clock


def usage_session() -> NativeUsageSession:
    return NativeUsageSession.from_record(
        {
            "session_id": "usage-session",
            "model_id": "resolved-session-model",
            "prompt_version": None,
            "event_id": None,
        }
    )


def usage_span(usage: JsonValue) -> NativeUsageSpan:
    return NativeUsageSpan.from_wire({"id": "actual-span", "model_usage": usage})


@pytest.mark.parametrize(
    "measurements",
    [
        {"input_tokens": 10, "output_tokens": 2},
        {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0},
        {"input_tokens": 5, "output_tokens": 2, "cache_read_input_tokens": 8},
        {"input_tokens": 4, "cache_read_tokens": 500, "cache_write_tokens": 501, "web_searches": 17},
        {
            "input_tokens": 8,
            "output_tokens": 3,
            "estimated_cost": 0.12,
            "usage_id": "untrusted-provider-extension",
            "cost_usd": "0.12",
            "billing_basis": "PROVIDER_ESTIMATE",
        },
    ],
)
async def test_actual_partial_usage_preserves_observations_and_declared_subscription_policy(
    repo: StorageBackend, measurements: dict[str, JsonValue]
) -> None:
    stamp = collection_clock()
    await record_native_usage(
        repo,
        usage_session(),
        usage_span(measurements),
        versions=ResolvedVersionIdentity(config_version="synthetic-config", code_version="synthetic-code"),
        collected_at=stamp,
    )
    row = await repo.get("ops.llm_usage", "usage-session:actual-span")
    assert row is not None
    assert row["usage_id"] == "usage-session:actual-span"
    assert row["native_usage_evidence"] == measurements
    parsed = NativeUsageCounters.from_wire(measurements)
    assert row["usage"] == parsed.model_dump(mode="json", exclude_none=True)
    assert row["model_id"] == "resolved-session-model" and row["subject_id"] == "usage-session"
    assert row["cost_usd"] == "0" and row["billing_basis"] == "SUBSCRIPTION"
    assert row["cache_read_tokens"] == measurements.get("cache_read_input_tokens")
    assert row["cache_write_tokens"] is None and "cache_creation_input_tokens" not in row
    assert row["at"] == row["created_at"] == stamp.value and row["processed_at"] is None
    missing = row["missing_fields"]
    assert isinstance(missing, dict)
    assert ("cache_read_tokens" in missing) is ("cache_read_input_tokens" not in measurements)
    assert "cache_write_tokens" in missing
    assert "estimated_cost" not in row
    assert row["input_tokens"] == measurements["input_tokens"]
    if isinstance(repo, PostgresRepository):
        async with repo.transaction():
            cursor = await repo._conn().execute(
                "SELECT cache_read_tokens,cache_write_tokens,web_searches FROM ops.llm_usage WHERE storage_key=%s",
                ("usage-session:actual-span",),
            )
            columns = await cursor.fetchone()
        assert columns is not None
        assert columns["cache_read_tokens"] == measurements.get("cache_read_input_tokens")
        assert columns["cache_write_tokens"] is None and columns["web_searches"] is None


@pytest.mark.parametrize("measurement", [True, -1, 1.5, "1", float("nan"), float("inf"), None])
async def test_invalid_reported_counter_refuses_before_any_usage_write(
    repo: StorageBackend, measurement: JsonValue
) -> None:
    with pytest.raises((ValidationError, ValueError)):
        usage_span({"input_tokens": measurement})
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.parametrize("phase", ["UNREPORTED", "EMPTY"])
async def test_unreported_and_explicit_empty_spans_have_distinct_no_ledger_effects(
    repo: StorageBackend, phase: str
) -> None:
    wire: dict[str, JsonValue] = {"id": "actual-span"}
    if phase == "EMPTY":
        wire["model_usage"] = {}
    span = NativeUsageSpan.from_wire(wire)
    assert span.phase == phase and span.counters is None
    await record_native_usage(
        repo,
        usage_session(),
        span,
        versions=ResolvedVersionIdentity(config_version="synthetic-config", code_version="synthetic-code"),
        collected_at=collection_clock(),
    )
    assert await repo.list("ops.llm_usage") == []


async def test_mutated_measurement_cannot_replace_retained_span_evidence(repo: StorageBackend) -> None:
    span = usage_span({"input_tokens": 2, "output_tokens": 1})
    changed = span.model_copy(update={"counters": NativeUsageCounters(input_tokens=20, output_tokens=1)})
    with pytest.raises(ValidationError):
        await record_native_usage(
            repo,
            usage_session(),
            changed,
            versions=ResolvedVersionIdentity(
                config_version="synthetic-config", code_version="synthetic-code"
            ),
            collected_at=collection_clock(),
        )
    assert await repo.list("ops.llm_usage") == []


def test_reported_search_zero_and_model_are_not_replaced_by_other_sources() -> None:
    span = NativeUsageSpan.from_wire(
        {
            "id": "actual-span",
            "model": "actual-reported-model",
            "web_searches": 9,
            "model_usage": {"server_tool_use": {"web_search_requests": 0}},
        }
    )
    assert span.reported_model == "actual-reported-model" and span.web_searches() == 0
