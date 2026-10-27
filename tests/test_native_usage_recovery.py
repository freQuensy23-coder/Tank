"""Real immutable usage commits remain recoverable before frame acknowledgment."""

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pytest
from pydantic import JsonValue
from runtime_failure_fixtures import runtime_failure_fixture
from test_native_execution_barrier import seed_execution_inputs
from test_native_usage import usage_session, usage_span
from test_session_completion_lineage import completion_repo as completion_repo

from tank.agents.native_transport import NativeEventObserved
from tank.agents.native_usage import record_native_usage
from tank.contracts.native_usage import NativeUsageReceiptMismatch, NativeUsageSpan
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.storage import ImmutableRecordError, StorageBackend
from tank.time_precision import collection_clock
from tank.util import JsonInput


@pytest.mark.parametrize("failure_point", ["USAGE_ACK", "FRAME_WRITE"])
async def test_actual_usage_commit_recovers_without_second_row_or_changed_first_receipt(
    tmp_path: Path,
    completion_repo: StorageBackend,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: Literal["USAGE_ACK", "FRAME_WRITE"],
) -> None:
    repo = completion_repo
    await seed_execution_inputs(repo, 1)
    event: dict[str, JsonValue] = {
        "id": "actual-usage-span",
        "type": "span.model_request_end",
        "model": "actual-reported-model",
        "model_usage": {"input_tokens": 2, "output_tokens": 1},
        **collection_clock().fields("processed_at"),
    }
    original_put = repo.put
    failed = False

    async def lose_ack_or_frame_write(
        collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        nonlocal failed
        if not failed and failure_point == "FRAME_WRITE" and collection == "ops.agent_sessions":
            failed = True
            raise OSError("Explicit synthetic frame write failure after real usage commit")
        stored = await original_put(collection, key, record, immutable=immutable)
        if not failed and failure_point == "USAGE_ACK" and collection == "ops.llm_usage":
            failed = True
            raise OSError("Explicit synthetic lost usage commit acknowledgment")
        return stored

    monkeypatch.setattr(repo, "put", lose_ack_or_frame_write)
    async with runtime_failure_fixture(repo, tmp_path, ops_progress_enabled=False) as runtime:
        with pytest.raises(OSError, match="Explicit synthetic"):
            await runtime.sessions.observe_native_event("session", event)
        first = await repo.get("ops.llm_usage", "session:actual-usage-span")
        assert first is not None and first["input_tokens"] == 2 and first["output_tokens"] == 1
        session = await repo.get("ops.agent_sessions", "session")
        assert session is not None
        processed = session.get("processed_events")
        assert processed is None or isinstance(processed, list) and "actual-usage-span" not in processed
        recovered = await runtime.sessions.observe_native_event("session", event)
        assert isinstance(recovered, NativeEventObserved)
        assert await repo.get("ops.llm_usage", "session:actual-usage-span") == first
        assert len(await repo.list("ops.llm_usage")) == 1
    current = await repo.get("ops.agent_sessions", "session")
    assert current is not None and current["processed_events"] == ["actual-usage-span"]
    journal = current["message_log"]
    assert isinstance(journal, list) and len(journal) == 1 and isinstance(journal[0], dict)
    assert "retry_count" not in journal[0] and journal[0]["body"] == {"type": "user.message"}
    assert await repo.list("ops.jobs") == []


async def test_actual_concurrent_usage_writes_reconcile_only_the_matching_first_receipt(
    completion_repo: StorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = completion_repo
    original_get, original_put = repo.get, repo.put
    both_empty = asyncio.Event()
    empty_reads = 0
    conflicts: list[str] = []

    async def read_before_either_write(collection: str, key: str) -> dict[str, JsonValue] | None:
        nonlocal empty_reads
        row = await original_get(collection, key)
        if collection == "ops.llm_usage" and row is None:
            empty_reads += 1
            if empty_reads == 2:
                both_empty.set()
            await asyncio.wait_for(both_empty.wait(), 2)
        return row

    async def observe_actual_conflict(
        collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        try:
            return await original_put(collection, key, record, immutable=immutable)
        except ImmutableRecordError:
            conflicts.append(key)
            raise

    monkeypatch.setattr(repo, "get", read_before_either_write)
    monkeypatch.setattr(repo, "put", observe_actual_conflict)
    span = usage_span({"input_tokens": 2, "output_tokens": 1})
    first, second = collection_clock(), collection_clock()
    await asyncio.gather(
        record_native_usage(
            repo,
            usage_session(),
            span,
            versions=ResolvedVersionIdentity(config_version="controller-one", code_version="same-code"),
            collected_at=first,
        ),
        record_native_usage(
            repo,
            usage_session(),
            span,
            versions=ResolvedVersionIdentity(config_version="controller-two", code_version="same-code"),
            collected_at=second,
        ),
    )
    rows = await repo.list("ops.llm_usage")
    assert len(rows) == 1 and empty_reads == 2
    assert conflicts == ["usage-session:actual-span"]
    winner = rows[0]
    expected_clock = first if winner["config_version"] == "controller-one" else second
    assert winner["created_at"] == expected_clock.value
    assert winner["input_tokens"] == 2 and winner["output_tokens"] == 1


@pytest.mark.parametrize("changed", ["measurement", "model", "raw_extension"])
async def test_existing_usage_contradiction_never_overwrites_or_claims_a_retry(
    completion_repo: StorageBackend, changed: str
) -> None:
    repo = completion_repo
    measurements: dict[str, JsonValue] = {"input_tokens": 2, "output_tokens": 1, "extension": True}
    wire: dict[str, JsonValue] = {
        "id": "actual-span",
        "model": "reported-model",
        "model_usage": measurements,
        **collection_clock().fields("processed_at"),
    }
    span = NativeUsageSpan.from_wire(wire)
    versions = ResolvedVersionIdentity(config_version="synthetic-config", code_version="synthetic-code")
    await record_native_usage(repo, usage_session(), span, versions=versions, collected_at=collection_clock())
    first = await repo.get("ops.llm_usage", "usage-session:actual-span")
    assert first is not None
    if changed == "measurement":
        measurements["input_tokens"] = 3
    elif changed == "model":
        wire["model"] = "different-reported-model"
    else:
        measurements["extension"] = 1
    with pytest.raises(NativeUsageReceiptMismatch):
        await record_native_usage(
            repo,
            usage_session(),
            NativeUsageSpan.from_wire(wire),
            versions=versions,
            collected_at=collection_clock(),
        )
    assert await repo.get("ops.llm_usage", "usage-session:actual-span") == first
    assert len(await repo.list("ops.llm_usage")) == 1


async def test_definite_prewrite_failure_does_not_invent_an_existing_usage_receipt(
    completion_repo: StorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = completion_repo
    original = repo.put
    failed = False

    async def refuse_first_write(
        collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        nonlocal failed
        if collection == "ops.llm_usage" and not failed:
            failed = True
            raise OSError("Explicit synthetic definite usage write failure")
        return await original(collection, key, record, immutable=immutable)

    monkeypatch.setattr(repo, "put", refuse_first_write)
    span = usage_span({"input_tokens": 2, "output_tokens": 1})
    versions = ResolvedVersionIdentity(config_version="synthetic-config", code_version="synthetic-code")
    with pytest.raises(OSError, match="definite usage write failure"):
        await record_native_usage(
            repo, usage_session(), span, versions=versions, collected_at=collection_clock()
        )
    assert await repo.list("ops.llm_usage") == []
    await record_native_usage(repo, usage_session(), span, versions=versions, collected_at=collection_clock())
    assert len(await repo.list("ops.llm_usage")) == 1


@pytest.mark.parametrize("schema", ["PUBLISHED_2F", "TYPED_33", "ATTRIBUTED_273"])
async def test_actual_prior_usage_formats_preserve_their_first_recorded_provenance(
    completion_repo: StorageBackend, schema: str
) -> None:
    repo = completion_repo
    stamp = collection_clock()
    raw: dict[str, JsonValue] = {"input_tokens": 2, "output_tokens": 1}
    span = NativeUsageSpan.from_wire(
        {"id": "actual-span", "model_usage": raw, "model": "first-model", **stamp.fields("processed_at")}
    )
    legacy: dict[str, JsonValue] = {
        "usage_id": "usage-session:actual-span",
        "session_id": "usage-session",
        "call": "AGENT",
        "model_id": "first-model",
        "model": "first-model",
        "prompt_version": "first-prompt",
        "input_tokens": 2,
        "output_tokens": 1,
        "cache_read_tokens": None,
        "cache_write_tokens": None,
        "effort": None,
        "stop_reason": None,
        "web_searches": None,
        "usage": raw,
        "cost_usd": "0",
        "billing_basis": "SUBSCRIPTION",
        "config_version": "first-config",
        "code_version": "first-code",
        "missing_fields": {
            "cache_read_tokens": "Provider span supplied no cache read measurement",
            "cache_write_tokens": "Provider span supplied no cache creation measurement",
            "effort": "First session did not report effort",
            "stop_reason": "First span did not report a finish reason",
            "web_searches": "First span did not report search use",
        },
        **stamp.fields("at"),
        **stamp.fields("created_at"),
        **stamp.fields("processed_at"),
    }
    if schema != "PUBLISHED_2F":
        legacy["native_usage_evidence"] = raw
    if schema == "ATTRIBUTED_273":
        legacy.update(
            model_id_basis="NATIVE_SPAN_MODEL", reported_model_id="first-model", session_model_id=None
        )
    await repo.put("ops.llm_usage", "usage-session:actual-span", legacy, immutable=True)
    first = await repo.get("ops.llm_usage", "usage-session:actual-span")
    assert first is not None
    await record_native_usage(
        repo,
        usage_session(),
        span,
        versions=ResolvedVersionIdentity(
            config_version="different-current-config", code_version="different-code"
        ),
        collected_at=collection_clock(),
    )
    assert await repo.get("ops.llm_usage", "usage-session:actual-span") == first
    assert first["config_version"] == "first-config" and first["prompt_version"] == "first-prompt"
    assert "native_usage_observation" not in first
