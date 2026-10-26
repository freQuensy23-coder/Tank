"""Persist admitted span measurements under the declared agent subscription policy."""

from collections.abc import Mapping
from typing import Protocol

from pydantic import JsonValue

from tank.contracts.native_usage import NativeUsageSession, NativeUsageSpan
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.time_precision import Timestamp
from tank.util import JsonInput


class NativeUsageStore(Protocol):
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...


async def record_native_usage(
    store: NativeUsageStore,
    session: NativeUsageSession,
    span: NativeUsageSpan,
    *,
    versions: ResolvedVersionIdentity,
    collected_at: Timestamp,
) -> None:
    session = NativeUsageSession.model_validate(session)
    span = NativeUsageSpan.model_validate(span)
    versions = ResolvedVersionIdentity.model_validate(versions)
    collected_at = Timestamp.model_validate(collected_at)
    if span.counters is None:
        return
    measured_at = span.processed_at if span.processed_at.value is not None else collected_at
    model = span.reported_model if span.reported_model is not None else session.model_id
    key = session.session_id + ":" + span.native_event_id
    searches = span.web_searches()
    measurements = {
        "input_tokens": span.counters.input_tokens,
        "output_tokens": span.counters.output_tokens,
        "cache_read_tokens": span.counters.cache_read_input_tokens,
        "cache_write_tokens": span.counters.cache_creation_input_tokens,
        "effort": session.effort,
        "stop_reason": span.finish_reason,
        "web_searches": searches,
    }
    missing: dict[str, JsonValue] = {
        name: "Provider span or deployed configuration did not supply this measurement"
        for name, value in measurements.items()
        if value is None
    }
    record: dict[str, JsonValue] = {
        "usage_id": key,
        "session_id": session.session_id,
        "event_id": session.event_id,
        "call": "AGENT",
        "model_id": model,
        "prompt_version": session.prompt_version,
        "prompt_version_basis": session.prompt_version_basis,
        "prompt_version_reason": session.prompt_version_reason,
        "prompt_label": session.prompt_label,
        "agent_version": session.agent_version,
        "cache_read_tokens": span.counters.cache_read_input_tokens,
        "cache_write_tokens": span.counters.cache_creation_input_tokens,
        "effort": session.effort,
        "stop_reason": span.finish_reason,
        "web_searches": searches,
        "usage": span.counters.model_dump(mode="json", exclude_none=True),
        "native_usage_evidence": span.raw_usage,
        "missing_fields": missing,
        **span.counters.model_dump(mode="json", exclude_none=True),
        "cost_usd": "0",
        "billing_basis": "SUBSCRIPTION",
        "cost_reason": "ChatGPT subscription: no per-token charge",
        "model": model,
        "subject_id": session.event_id if session.event_id is not None else session.session_id,
        **measured_at.fields("at"),
        **collected_at.fields("created_at"),
        **span.processed_at.fields("processed_at"),
        "config_version": versions.config_version,
        "code_version": versions.code_version,
        "session_config_version": session.config_version,
        "session_code_version": session.code_version,
    }
    await store.put("ops.llm_usage", key, record, immutable=True)
