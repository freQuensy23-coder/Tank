"""Persist admitted span measurements under the declared agent subscription policy."""

from collections.abc import Mapping
from typing import Protocol

from pydantic import JsonValue

from tank.contracts.native_usage import (
    NativeUsageSession,
    NativeUsageSpan,
    select_native_usage_model,
    validate_native_usage_receipt,
)
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.storage import ImmutableRecordError
from tank.time_precision import Timestamp
from tank.util import JsonInput


class NativeUsageStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

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
    key = session.session_id + ":" + span.native_event_id
    existing = await store.get("ops.llm_usage", key)
    if existing is not None:
        validate_native_usage_receipt(existing, session.session_id, span)
        return
    measured_at = span.processed_at if span.processed_at.value is not None else collected_at
    model = select_native_usage_model(session, span)
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
        "model_id": model.model_id,
        "model_id_basis": model.basis,
        "reported_model_id": model.reported_model_id,
        "session_model_id": model.session_model_id,
        "session_model_id_basis": session.model_id_basis,
        "session_model_id_reason": session.model_id_reason,
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
        "native_usage_observation": span.model_dump(mode="json"),
        "missing_fields": missing,
        **span.counters.model_dump(mode="json", exclude_none=True),
        "cost_usd": "0",
        "billing_basis": "SUBSCRIPTION",
        "cost_reason": "ChatGPT subscription: no per-token charge",
        "model": model.model_id,
        "subject_id": session.event_id if session.event_id is not None else session.session_id,
        **measured_at.fields("at"),
        **collected_at.fields("created_at"),
        **span.processed_at.fields("processed_at"),
        "config_version": versions.config_version,
        "code_version": versions.code_version,
        "session_config_version": session.config_version,
        "session_code_version": session.code_version,
    }
    try:
        await store.put("ops.llm_usage", key, record, immutable=True)
    except ImmutableRecordError:
        concurrent = await store.get("ops.llm_usage", key)
        if concurrent is None:
            raise
        validate_native_usage_receipt(concurrent, session.session_id, span)
