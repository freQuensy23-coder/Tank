"""Evidence-gated recovery admission and actual transactional queue/link effects."""

import asyncio
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Never, Protocol

import httpx
import pytest
from agent_source_fixtures import source_item_fixture
from pydantic import JsonValue, ValidationError
from runtime_public_fixtures import public_runtime
from source_support import fixture_versions
from test_storage import repo as repo

from tank.contracts import DomainModel
from tank.contracts.jobs import JobEnvelope
from tank.contracts.reaction_recovery import (
    ReactionRecoveryCommand,
    ReactionRecoveryEvidence,
    ReactionRecoveryExecution,
    ReactionRecoveryLink,
    ReactionRecoveryOutcome,
    ReactionRecoveryUnresolved,
    ReactionRecoveryWitnessRequest,
    RetainedCoalescedReaction,
)
from tank.contracts.scheduler_commands import (
    InitialReactionCommand,
    ManualReactionRevisionCommand,
    ReactionAnchorFacts,
    RevisedReactionCommand,
)
from tank.contracts.scheduler_results import (
    CapturedReaction,
    ReactionCaptureResult,
    ReactionCaptureResultDocument,
)
from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.job_repository import JobRepository
from tank.models import Record
from tank.reaction_recovery import (
    ReactionRecovery,
    RecoveryWitnessIndex,
    admit_recovery_witnesses,
    recovery_backfill_from_record,
    recovery_idempotency_key,
    recovery_link_key,
)
from tank.reaction_recovery_policy import recovery_eligibility
from tank.reaction_timing import late_reaction_schedule
from tank.runtime import Runtime
from tank.scheduler import ReactionEventIdentity
from tank.storage import ImmutableRecordError, MemoryRepository, StorageBackend
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import JsonInput, iso, new_id, parse_time, sha256, utcnow


def request_fields() -> dict[str, str]:
    return {
        "enqueue_idempotency_key": "recover:original",
        "original_job_id": "original",
        "peer_job_id": "peer",
        "source_backfill_id": "backfill",
        "checkpoint_key": "event:token:24h:r2",
        "change_marker_key": "reaction-change:change",
        "change_owner_key": "reaction-change-owner:change",
        "recovery_link_key": "reaction-recovery:original",
    }


def test_recovery_witnesses_have_exact_deterministic_addresses() -> None:
    request = ReactionRecoveryWitnessRequest.model_validate(request_fields())
    assert [(row.collection, row.storage_key) for row in request.addresses()] == [
        ("mkt.backfill_runs", "backfill"),
        ("ops.jobs", "original"),
        ("ops.jobs", "peer"),
        ("ops.scheduler_checkpoints", "reaction-change-owner:change"),
        ("ops.scheduler_checkpoints", "reaction-change:change"),
        ("ops.scheduler_checkpoints", "reaction-recovery:original"),
        ("out.reaction_checkpoints", "event:token:24h:r2"),
    ]
    assert ReactionRecoveryWitnessRequest.model_validate_json(request.model_dump_json()) == request


@pytest.mark.parametrize("field", tuple(request_fields()))
@pytest.mark.parametrize("value", [None, "", " \t", 0, False])
def test_recovery_required_identity_cannot_be_invented(field: str, value: object) -> None:
    fields: dict[str, object] = dict(request_fields())
    fields[field] = value
    with pytest.raises(ValidationError):
        ReactionRecoveryWitnessRequest.model_validate(fields)


@pytest.mark.parametrize(
    ("field", "alias"),
    [("peer_job_id", "original_job_id"), ("recovery_link_key", "change_marker_key")],
)
def test_recovery_witness_roles_cannot_alias(field: str, alias: str) -> None:
    fields = request_fields()
    fields[field] = fields[alias]
    with pytest.raises(ValidationError):
        ReactionRecoveryWitnessRequest.model_validate(fields)


@pytest.mark.parametrize(
    "result",
    [{"coalesced_into": "peer"}, {"kind": "COALESCED", "coalesced_into": "peer"}],
)
def test_actual_retained_coalescing_results_preserve_wire_presence(
    result: dict[str, JsonValue],
) -> None:
    admitted = RetainedCoalescedReaction.model_validate(result)
    assert admitted.root.coalesced_into == "peer"
    assert admitted.model_dump(mode="json") == result
    assert RetainedCoalescedReaction.model_validate_json(admitted.model_dump_json()) == admitted


@pytest.mark.parametrize(
    "result",
    [
        {},
        {"coalesced_into": None},
        {"coalesced_into": ""},
        {"coalesced_into": "peer", "revision": 2},
        {"kind": "CAPTURED", "coalesced_into": "peer"},
        {"kind": None, "coalesced_into": "peer"},
        {"kind": "COALESCED", "coalesced_into": "peer", "applied": True},
    ],
)
def test_partial_or_mixed_results_cannot_authorize_recovery(
    result: dict[str, JsonValue],
) -> None:
    with pytest.raises(ValidationError):
        RetainedCoalescedReaction.model_validate(result)


def test_retained_result_validation_does_not_echo_payload_values() -> None:
    with pytest.raises(ValidationError) as failure:
        RetainedCoalescedReaction.model_validate({"coalesced_into": {"secret": "sensitive-value"}})
    assert "sensitive-value" not in str(failure.value)


def recovery_evidence_fields() -> dict[str, JsonValue]:
    source: dict[str, JsonValue] = {
        "backfill_id": "source",
        "status": "FAILED",
        "finished_at": "2026-10-20T12:00:00Z",
    }
    source_change = "CAP-" + sha256(source)
    command: dict[str, JsonValue] = {
        "event_id": "event",
        "token_id": "token",
        "checkpoint": "POST_24H",
        "automatic_revision": True,
        "change_id": source_change + ":POST_24H",
        "reason": "Declared terminal capture failure",
    }
    anchor = Timestamp("2026-10-19T00:00:00Z", "EXACT")
    return {
        "original": {
            "job_id": "original",
            "status": "DONE",
            "command": command,
            "payload_revision": sha256(command),
            "result": {"coalesced_into": "peer"},
        },
        "peer": {
            "job_id": "peer",
            "status": "FAILED",
            "command": command | {"change_id": "different-source:POST_24H"},
        },
        "source": {
            "event_id": "event",
            "token_id": "token",
            "backfill_id": "source",
            "change_id": source_change,
            "status": "FAILED",
            "anchor": anchor.model_dump(mode="json"),
            "finished_at": collection_clock("2026-10-20T12:00:00Z").model_dump(mode="json"),
        },
        "checkpoint": {
            "receipt": {
                "kind": "CAPTURED",
                "checkpoint_key": "event:token:POST_24H:1",
                "event_id": "event",
                "token_id": "token",
                "checkpoint": "POST_24H",
                "revision": 1,
                "data_fingerprint": sha256({"declared_fixture": "checkpoint"}),
            },
            "immutable": True,
            "anchor": {"type": "T0", **anchor.fields("ts")},
            "captured_at": collection_clock("2026-10-20T11:00:00Z").model_dump(mode="json"),
        },
        "marker": {"kind": "ABSENT"},
    }


def test_recovery_needs_failed_peer_source_identity_and_older_bound_checkpoint() -> None:
    evidence = ReactionRecoveryEvidence.model_validate(recovery_evidence_fields())
    result = recovery_eligibility(evidence)
    assert result.kind == "ELIGIBLE"
    assert result.original_job_id == "original"
    assert result.peer_job_id == "peer"
    assert result.payload_revision == evidence.original.payload_revision
    assert result.command == evidence.original.command


@pytest.mark.parametrize(
    ("field", "reason"),
    [("peer", "PEER_ABSENT"), ("source", "SOURCE_ABSENT"), ("checkpoint", "CHECKPOINT_ABSENT")],
)
def test_missing_marker_and_failed_peer_alone_are_not_recovery_evidence(field: str, reason: str) -> None:
    fields = recovery_evidence_fields()
    fields[field] = None
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == reason


@pytest.mark.parametrize("captured_at", ["2026-10-20T12:00:00Z", "2026-10-20T12:00:01Z"])
def test_later_checkpoint_cannot_authorize_replaying_possibly_applied_work(captured_at: str) -> None:
    fields = recovery_evidence_fields()
    checkpoint = fields["checkpoint"]
    assert isinstance(checkpoint, dict)
    checkpoint["captured_at"] = collection_clock(captured_at).model_dump(mode="json")
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == "SOURCE_MAY_ALREADY_BE_CAPTURED"


def test_unbound_marker_is_not_missing_marker() -> None:
    fields = recovery_evidence_fields()
    fields["marker"] = {"kind": "UNRESOLVED"}
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == "MARKER_UNRESOLVED"


@pytest.mark.parametrize(
    ("subject", "field", "value", "reason"),
    [
        ("peer", "job_id", "unrelated", "PEER_SCOPE_MISMATCH"),
        ("source", "event_id", "foreign", "SOURCE_SCOPE_MISMATCH"),
        ("source", "change_id", "foreign", "SOURCE_SCOPE_MISMATCH"),
        (
            "source",
            "finished_at",
            {"value": "2026-10-20T12:00:00Z", "precision": None, "precision_reason": "UNREPORTED"},
            "SOURCE_CLOCK_UNREPORTED",
        ),
        (
            "checkpoint",
            "captured_at",
            {"value": None, "precision": None, "precision_reason": "UNREPORTED"},
            "CHECKPOINT_CLOCK_UNREPORTED",
        ),
    ],
)
def test_recovery_requires_actual_identity_and_collection_chronology(
    subject: str, field: str, value: JsonValue, reason: str
) -> None:
    fields = recovery_evidence_fields()
    witness = fields[subject]
    assert isinstance(witness, dict)
    witness[field] = value
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == reason


def test_validated_actual_marker_supersedes_failed_peer_recovery() -> None:
    fields = recovery_evidence_fields()
    original = fields["original"]
    assert isinstance(original, dict)
    command = original["command"]
    assert isinstance(command, dict)
    fields["marker"] = {
        "kind": "APPLIED",
        "receipt": {
            "kind": "CHANGE_ALREADY_APPLIED",
            "event_id": command["event_id"],
            "token_id": command["token_id"],
            "checkpoint": command["checkpoint"],
            "change_id": command["change_id"],
            "changed": False,
            "revision": 1,
            "data_fingerprint": sha256({"declared_fixture": "checkpoint"}),
        },
    }
    fields["peer"] = None
    fields["source"] = None
    fields["checkpoint"] = None
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "ALREADY_APPLIED"
    assert result.receipt.changed is False


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING", "DONE"])
def test_nonterminal_or_successful_peer_is_not_a_failed_peer_witness(status: str) -> None:
    fields = recovery_evidence_fields()
    peer = fields["peer"]
    assert isinstance(peer, dict)
    peer["status"] = status
    with pytest.raises(ValidationError):
        ReactionRecoveryEvidence.model_validate(fields)


def test_source_identity_uses_actual_offset_wire_not_normalized_timestamp() -> None:
    source: dict[str, JsonValue] = {
        "backfill_id": "backfill",
        "event_id": "event",
        "token_id": "token",
        "status": "FAILED",
        "anchor": None,
        "finished_at": "2026-10-20T15:00:00+03:00",
        "finished_at_prec": "COLLECTION_ONLY",
        "reason": "Original retained provider diagnostic",
    }
    admitted = recovery_backfill_from_record(source)
    assert admitted.change_id == "CAP-" + sha256(
        {
            "backfill_id": "backfill",
            "status": "FAILED",
            "finished_at": "2026-10-20T15:00:00+03:00",
        }
    )
    assert admitted.finished_at.value == "2026-10-20T12:00:00Z"
    assert admitted.anchor.value is None
    assert source["finished_at"] == "2026-10-20T15:00:00+03:00"
    assert source["reason"] == "Original retained provider diagnostic"


@pytest.mark.parametrize("field", ["backfill_id", "event_id", "token_id", "status", "finished_at"])
def test_source_admission_does_not_invent_missing_required_facts(field: str) -> None:
    source: dict[str, JsonValue] = {
        "backfill_id": "backfill",
        "event_id": "event",
        "token_id": "token",
        "status": "FAILED",
        "anchor": None,
        "finished_at": "2026-10-20T12:00:00Z",
        "finished_at_prec": "COLLECTION_ONLY",
    }
    del source[field]
    with pytest.raises(ValidationError):
        recovery_backfill_from_record(source)


async def recovery_witness_rows() -> tuple[ReactionRecoveryWitnessRequest, RecoveryWitnessIndex]:
    fields = recovery_evidence_fields()
    original = fields["original"]
    assert isinstance(original, dict)
    payload = original["command"]
    assert isinstance(payload, dict)
    payload = payload | {"_depends_on": []}
    repo = MemoryRepository()
    original_job = await repo.enqueue("REACTION_CAPTURE", payload, "original", max_attempts=5)
    peer = await repo.enqueue(
        "REACTION_CAPTURE", payload | {"change_id": "other-change:POST_24H"}, "peer", max_attempts=5
    )
    original_id, peer_id = original_job["job_id"], peer["job_id"]
    assert isinstance(original_id, str) and isinstance(peer_id, str)
    identity = recovery_idempotency_key(original_id, sha256(payload), peer_id)
    change_id = payload["change_id"]
    assert isinstance(change_id, str)
    request = ReactionRecoveryWitnessRequest(
        enqueue_idempotency_key=identity,
        original_job_id=original_id,
        peer_job_id=peer_id,
        source_backfill_id="source",
        checkpoint_key="event:token:POST_24H:1",
        change_marker_key="reaction-change:" + change_id,
        change_owner_key="reaction-change-owner:" + change_id,
        recovery_link_key=recovery_link_key(original_id),
    )
    checkpoint = fields["checkpoint"]
    assert isinstance(checkpoint, dict)
    checkpoint_receipt = checkpoint["receipt"]
    assert isinstance(checkpoint_receipt, dict)
    checkpoint_record = {
        name: value for name, value in checkpoint_receipt.items() if name not in {"kind", "checkpoint_key"}
    } | {
        "anchor": checkpoint["anchor"],
        **collection_clock("2026-10-20T11:00:00Z").fields("captured_at"),
    }
    return request, {
        ("ops.jobs", original_id): ImmutableDocument(
            document=original_job
            | {
                "status": "DONE",
                "attempts": 1,
                "result": {"coalesced_into": peer_id},
                **collection_clock().fields("finished_at"),
            },
            immutable=False,
        ),
        ("ops.jobs", peer_id): ImmutableDocument(
            document=peer | {"status": "FAILED", "attempts": 5, **collection_clock().fields("finished_at")},
            immutable=False,
        ),
        ("mkt.backfill_runs", "source"): NativeBackfillWitness(
            record={
                "backfill_id": "source",
                "event_id": "event",
                "token_id": "token",
                "status": "FAILED",
                **Timestamp("2026-10-19T00:00:00Z", "EXACT").fields("anchor"),
                **collection_clock("2026-10-20T12:00:00Z").fields("finished_at"),
            }
        ),
        ("out.reaction_checkpoints", request.checkpoint_key): ImmutableDocument(
            document=checkpoint_record, immutable=True
        ),
        ("ops.scheduler_checkpoints", request.change_marker_key): None,
        ("ops.scheduler_checkpoints", request.change_owner_key): None,
        ("ops.scheduler_checkpoints", request.recovery_link_key): None,
    }


async def test_recovery_admits_actual_queue_payload_with_metadata_and_distinct_native_row() -> None:
    request, rows = await recovery_witness_rows()
    before = rows[("ops.jobs", request.original_job_id)]
    assert isinstance(before, ImmutableDocument)
    result = admit_recovery_witnesses(request, rows)
    assert isinstance(result, ReactionRecoveryEvidence)
    eligibility = recovery_eligibility(result)
    assert eligibility.kind == "ELIGIBLE"
    payload = before.document["payload"]
    assert isinstance(payload, dict)
    assert payload["_depends_on"] == []
    assert result.original.payload_revision == sha256(payload)
    assert before.document["result"] == {"coalesced_into": request.peer_job_id}


async def test_native_backfill_cannot_borrow_document_immutability_metadata() -> None:
    request, admitted_rows = await recovery_witness_rows()
    rows = dict(admitted_rows)
    source = rows[("mkt.backfill_runs", request.source_backfill_id)]
    assert isinstance(source, NativeBackfillWitness)
    rows[("mkt.backfill_runs", request.source_backfill_id)] = ImmutableDocument(
        document=source.record, immutable=True
    )
    result = admit_recovery_witnesses(request, rows)
    assert isinstance(result, ReactionRecoveryUnresolved)
    assert result.reason == "WITNESS_UNADMITTED"


@pytest.mark.parametrize("orientation", [None, "UNSPECIFIED", "END"])
def test_unknown_or_different_bucket_orientation_cannot_establish_source_scope(
    orientation: str | None,
) -> None:
    fields = recovery_evidence_fields()
    source = fields["source"]
    checkpoint = fields["checkpoint"]
    assert isinstance(source, dict) and isinstance(checkpoint, dict)
    source["anchor"] = Timestamp("2026-10-19T00:00:00Z", "BUCKET(1h)", bucket_anchor=orientation).model_dump(
        mode="json"
    )
    checkpoint["anchor"] = {
        "type": "T0",
        **Timestamp("2026-10-19T00:00:00Z", "BUCKET(1h)", bucket_anchor="START").fields("ts"),
    }
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "UNRESOLVED"
    assert result.reason == (
        "CHECKPOINT_SCOPE_MISMATCH" if orientation == "END" else "SOURCE_CLOCK_UNREPORTED"
    )


def test_source_projection_preserves_reported_bucket_orientation() -> None:
    source = recovery_backfill_from_record(
        {
            "event_id": "event",
            "token_id": "token",
            "backfill_id": "source",
            "status": "FAILED",
            "anchor": "2026-10-19T00:00:00Z",
            "anchor_prec": "BUCKET(1h)",
            "anchor_bucket_anchor": "END",
            **collection_clock("2026-10-20T12:00:00Z").fields("finished_at"),
        }
    )
    assert source.anchor.bucket_anchor == "END"


def test_equivalent_known_bucket_units_preserve_the_same_declared_scope() -> None:
    fields = recovery_evidence_fields()
    source = fields["source"]
    checkpoint = fields["checkpoint"]
    assert isinstance(source, dict) and isinstance(checkpoint, dict)
    source["anchor"] = Timestamp("2026-10-19T00:00:00Z", "BUCKET(60m)", bucket_anchor="START").model_dump(
        mode="json"
    )
    checkpoint["anchor"] = {
        "type": "T0",
        **Timestamp("2026-10-19T00:00:00Z", "BUCKET(1h)", bucket_anchor="START").fields("ts"),
    }
    result = recovery_eligibility(ReactionRecoveryEvidence.model_validate(fields))
    assert result.kind == "ELIGIBLE"


async def seed_recovery_history(backend: StorageBackend) -> ReactionRecoveryCommand:
    request, rows = await recovery_witness_rows()
    source = rows[("mkt.backfill_runs", request.source_backfill_id)]
    assert isinstance(source, NativeBackfillWitness)
    await backend.put("mkt.backfill_runs", request.source_backfill_id, source.record)
    stored_source = await backend.get("mkt.backfill_runs", request.source_backfill_id)
    assert stored_source is not None
    change = recovery_backfill_from_record(stored_source)
    for (collection, key), row in rows.items():
        if isinstance(row, ImmutableDocument):
            record = dict(row.document)
            if key == request.original_job_id:
                payload = record["payload"]
                assert isinstance(payload, dict)
                record["payload"] = payload | {"change_id": change.change_id + ":POST_24H"}
            await backend.put(collection, key, record, immutable=row.immutable)
    return ReactionRecoveryCommand(
        original_job_id=request.original_job_id, source_backfill_id=request.source_backfill_id
    )


async def test_recovery_atomically_links_one_current_budget_and_preserves_originals(
    repo: StorageBackend,
) -> None:
    command = await seed_recovery_history(repo)
    original = await repo.get("ops.jobs", command.original_job_id)
    source = await repo.get("mkt.backfill_runs", command.source_backfill_id)
    assert original is not None
    old_result = original["result"]
    assert isinstance(old_result, dict)
    peer_id = old_result["coalesced_into"]
    assert isinstance(peer_id, str)
    peer_before = await repo.get("ops.jobs", peer_id)
    checkpoint_before = await repo.get("out.reaction_checkpoints", "event:token:POST_24H:1")
    operation = collection_clock()
    recovery = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=False),
        clock=lambda: operation,
    )
    outcome = await recovery.recover(command)
    assert outcome.kind == "QUEUED"
    queued = await repo.get("ops.jobs", outcome.job_id)
    assert queued is not None
    assert queued["payload"] == original["payload"]
    assert queued["priority"] == original["priority"]
    assert queued["max_attempts"] == original["max_attempts"]
    assert queued["status"] == "QUEUED"
    assert "result" not in queued
    due = propagate_timestamp(queued, "run_after")
    expiry = propagate_timestamp(queued, "expires_at")
    assert operation.value is not None and due.value is not None and expiry.value is not None
    assert (parse_time(due.value) - parse_time(operation.value)).total_seconds() == 1800
    assert (parse_time(expiry.value) - parse_time(operation.value)).total_seconds() == 21600
    wire = await repo.get("ops.scheduler_checkpoints", outcome.recovery_link_key)
    assert wire is not None
    link = ReactionRecoveryLink.model_validate(wire)
    assert link.state.recovered_job_id == outcome.job_id
    assert link.state.original_payload_revision == sha256(original["payload"])
    assert link.at == operation.value
    assert await repo.get("ops.jobs", command.original_job_id) == original
    assert await repo.get("mkt.backfill_runs", command.source_backfill_id) == source
    assert await repo.get("ops.jobs", peer_id) == peer_before
    assert await repo.get("out.reaction_checkpoints", "event:token:POST_24H:1") == checkpoint_before
    with pytest.raises(ImmutableRecordError):
        await repo.put(
            "ops.scheduler_checkpoints",
            outcome.recovery_link_key,
            wire | {"at": "2026-10-27T00:00:00Z"},
        )
    restarted = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=False),
        clock=lambda: collection_clock("2030-01-01T00:00:00Z"),
    )
    again = await restarted.recover(command)
    assert again.kind == "EXISTING_WORK"
    assert again.work.job_id == outcome.job_id
    assert await repo.get("ops.jobs", outcome.job_id) == queued
    assert await repo.get("ops.scheduler_checkpoints", outcome.recovery_link_key) == wire


async def test_concurrent_recovery_allocates_only_one_generation(repo: StorageBackend) -> None:
    command = await seed_recovery_history(repo)
    operation = collection_clock()
    recovery = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=False),
        clock=lambda: operation,
    )
    results = await asyncio.gather(recovery.recover(command), recovery.recover(command))
    assert sorted(result.kind for result in results) == ["EXISTING_WORK", "QUEUED"]
    linked = [result for result in results if result.kind == "QUEUED"]
    assert len(linked) == 1
    assert len(await repo.list("ops.jobs")) == 3


async def test_replay_execution_does_not_recover_into_ordinary_runtime(repo: StorageBackend) -> None:
    command = await seed_recovery_history(repo)
    before = await repo.list("ops.jobs")
    recovery = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=True),
        clock=collection_clock,
    )
    result = await recovery.recover(command)
    assert result.kind == "UNRESOLVED"
    assert result.reason == "SOURCE_MODE_UNSUPPORTED"
    assert await repo.list("ops.jobs") == before
    assert await repo.get("ops.scheduler_checkpoints", recovery_link_key(command.original_job_id)) is None


@pytest.mark.parametrize("changed", ["payload", "peer", "source", "policy"])
async def test_changed_witness_cannot_allocate_a_second_recovery_generation(
    repo: StorageBackend, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    command = await seed_recovery_history(repo)
    operation = collection_clock()
    recovery = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=False),
        clock=lambda: operation,
    )
    first = await recovery.recover(command)
    assert first.kind == "QUEUED"
    link_before = await repo.get("ops.scheduler_checkpoints", first.recovery_link_key)
    original = await repo.get("ops.jobs", command.original_job_id)
    assert original is not None
    if changed == "payload":
        payload = original["payload"]
        assert isinstance(payload, dict)
        await repo.put(
            "ops.jobs",
            command.original_job_id,
            original | {"payload": payload | {"reason": "Different retained request"}},
        )
    elif changed == "peer":
        payload = original["payload"]
        assert isinstance(payload, dict)
        peer = await repo.enqueue(
            "REACTION_CAPTURE",
            payload | {"change_id": "changed-peer:POST_24H"},
            "changed-peer",
            max_attempts=1,
        )
        peer_id = peer["job_id"]
        assert isinstance(peer_id, str)
        await repo.put(
            "ops.jobs",
            peer_id,
            peer | {"status": "FAILED", "attempts": 1, **collection_clock().fields("finished_at")},
        )
        await repo.put(
            "ops.jobs", command.original_job_id, original | {"result": {"coalesced_into": peer_id}}
        )
    elif changed == "source":
        source = await repo.get("mkt.backfill_runs", command.source_backfill_id)
        assert source is not None
        await repo.put(
            "mkt.backfill_runs",
            command.source_backfill_id,
            source | {"reason": "Known later source diagnostic"},
        )
    else:
        monkeypatch.setattr(
            "tank.reaction_recovery.recovery_policy_revision", lambda: sha256("different-policy")
        )
    jobs_before = await repo.list("ops.jobs")
    original_before = await repo.get("ops.jobs", command.original_job_id)
    again = await recovery.recover(command)
    assert again.kind == "UNRESOLVED"
    assert again.reason == "RECOVERY_LINK_UNADMITTED"
    assert await repo.list("ops.jobs") == jobs_before
    assert await repo.get("ops.jobs", command.original_job_id) == original_before
    assert await repo.get("ops.scheduler_checkpoints", first.recovery_link_key) == link_before


async def test_recovery_enqueue_and_link_failure_roll_back_as_one_effect(
    repo: StorageBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    command = await seed_recovery_history(repo)
    before = await repo.list("ops.jobs")
    original_put = repo.put
    key = recovery_link_key(command.original_job_id)

    async def fail_link(
        collection: str,
        storage_key: str,
        record: Mapping[str, JsonInput],
        *,
        immutable: bool = False,
    ) -> Record:
        if collection == "ops.scheduler_checkpoints" and storage_key == key:
            raise RuntimeError("Injected recovery link failure")
        return await original_put(collection, storage_key, record, immutable=immutable)

    recovery = ReactionRecovery(
        repo,
        execution=ReactionRecoveryExecution(versions=fixture_versions(), replay=False),
        clock=collection_clock,
    )
    monkeypatch.setattr(repo, "put", fail_link)
    with pytest.raises(RuntimeError, match="Injected recovery link failure"):
        await recovery.recover(command)
    assert await repo.list("ops.jobs") == before
    assert await repo.get("ops.scheduler_checkpoints", key) is None
    monkeypatch.setattr(repo, "put", original_put)
    recovered = await recovery.recover(command)
    assert recovered.kind == "QUEUED"
    assert len(await repo.list("ops.jobs")) == 3


async def observed_terminal_job(
    jobs: JobRepository, job_id: str, expected: Literal["DONE", "FAILED"]
) -> JobEnvelope:
    async with asyncio.timeout(5):
        while True:
            job = await jobs.get(job_id)
            assert job is not None
            if job.status == expected:
                return job
            assert job.status not in {"DONE", "FAILED", "EXPIRED"}
            await asyncio.sleep(0)


async def test_recovery_due_work_runs_actual_worker_and_measured_checkpoint_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current = utcnow().replace(second=0, microsecond=0)
    for module in ("tank.util", "tank.storage", "tank.runtime", "tank.scheduler"):
        monkeypatch.setattr(module + ".utcnow", lambda: current)
    repo = MemoryRepository()
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, _):
        event_id, token_id = "recovery-benchmark-event", "bitcoin:BTC"
        anchor = current - timedelta(hours=25)
        await repo.put("evt.events", event_id, {"event_id": event_id})
        await repo.put(
            "evt.beneficiary_maps",
            event_id + ":1",
            {"event_id": event_id, "map_version": 1, "analysis_set": []},
        )
        instruments = [
            row.instrument_id
            for row in runtime.settings.registry().instruments
            if row.token_id == token_id
            and row.market_type == "SPOT"
            and row.active
            and ":spot:" in row.instrument_id
        ]
        assert instruments
        instrument_id = instruments[0]
        final_key = instrument_id + ":" + iso(anchor + timedelta(hours=24) - timedelta(minutes=1))
        for minute in range(1441):
            opened = anchor - timedelta(minutes=1) + timedelta(minutes=minute)
            key = instrument_id + ":" + iso(opened)
            close = "110" if minute == 1440 else "100"
            await repo.put(
                "mkt.klines_1m",
                key,
                {
                    "instrument_id": instrument_id,
                    "market_type": "SPOT",
                    "open_time": iso(opened),
                    "interval_s": 60,
                    "complete": True,
                    "o": close,
                    "h": close,
                    "l": close,
                    "c": close,
                    "volume_native": "10",
                    "quote_volume_usd": "1100" if minute == 1440 else "1000",
                    **collection_clock(current - timedelta(seconds=1)).fields("recorded_at"),
                },
            )
        initial = await runtime.scheduler.reaction_capture(
            InitialReactionCommand(
                event_id=event_id,
                token_id=token_id,
                checkpoint="POST_24H",
                anchor=ReactionAnchorFacts(type="T0", ts=iso(anchor), ts_prec="EXACT"),
            )
        )
        assert initial.kind == "CAPTURED"
        original_checkpoint = await repo.get("out.reaction_checkpoints", initial.checkpoint_key)
        assert original_checkpoint is not None and original_checkpoint["r_raw"] == pytest.approx(0.1)
        current += timedelta(minutes=1)
        final = await repo.get("mkt.klines_1m", final_key)
        assert final is not None
        await repo.put(
            "mkt.klines_1m",
            final_key,
            final
            | {
                "o": "120",
                "h": "120",
                "l": "120",
                "c": "120",
                "quote_volume_usd": "1200",
                **collection_clock().fields("recorded_at"),
            },
        )
        backfill_id = new_id("backfill")
        await repo.put(
            "mkt.backfill_runs",
            backfill_id,
            {
                "backfill_id": backfill_id,
                "event_id": event_id,
                "token_id": token_id,
                "status": "FAILED",
                **Timestamp(iso(anchor), "EXACT").fields("anchor"),
                **collection_clock().fields("finished_at"),
            },
        )
        source_wire = await repo.get("mkt.backfill_runs", backfill_id)
        assert source_wire is not None
        source = recovery_backfill_from_record(source_wire)
        original_command = RevisedReactionCommand(
            event_id=event_id,
            token_id=token_id,
            checkpoint="POST_24H",
            automatic_revision=True,
            change_id=source.change_id + ":POST_24H",
            reason="Late capture completed: " + backfill_id + " (FAILED)",
        )
        schedule = late_reaction_schedule(collection_clock())
        old = await repo.enqueue(
            "REACTION_CAPTURE",
            original_command.storage_fields(),
            "reaction-late:" + original_command.change_id,
            priority=2,
            max_attempts=5,
            run_after=schedule.run_after,
            expires_at=schedule.expires_at,
        )
        current += timedelta(seconds=1)
        peer = await repo.enqueue(
            "REACTION_CAPTURE",
            original_command.storage_fields() | {"change_id": "declared-fault-peer:POST_24H"},
            "declared-fault-peer",
            priority=2,
            max_attempts=1,
            run_after=late_reaction_schedule(collection_clock()).run_after,
            expires_at=late_reaction_schedule(collection_clock()).expires_at,
        )
        old_id, peer_id = old["job_id"], peer["job_id"]
        assert isinstance(old_id, str) and isinstance(peer_id, str)
        assert schedule.run_after.value is not None
        current = parse_time(schedule.run_after.value)
        claimed = await repo.claim_job()
        assert claimed is not None and claimed["job_id"] == old_id
        await repo.put(
            "ops.jobs",
            old_id,
            claimed
            | {
                "status": "DONE",
                "result": {"coalesced_into": peer_id},
                **collection_clock().fields("finished_at"),
            },
        )
        peer_due = propagate_timestamp(peer, "run_after")
        assert peer_due.value is not None
        current = parse_time(peer_due.value)
        entered = asyncio.Event()

        async def failed_read(payload: InitialReactionCommand) -> Never:
            entered.set()
            raise RuntimeError("Declared peer measurement read failure")

        jobs = JobRepository(repo)
        with monkeypatch.context() as failure:
            failure.setattr(runtime.scheduler, "_measure_reaction", failed_read)
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))
            try:
                async with asyncio.timeout(5):
                    await entered.wait()
                    failed = await observed_terminal_job(jobs, peer_id, "FAILED")
                assert failed.attempts == 1
            finally:
                stop.set()
                await worker
        old_before = await repo.get("ops.jobs", old_id)
        recovery = ReactionRecovery(
            repo,
            execution=ReactionRecoveryExecution(
                versions=runtime.settings.require_versions(), replay=runtime.settings.replay
            ),
            clock=collection_clock,
        )
        request = ReactionRecoveryCommand(original_job_id=old_id, source_backfill_id=backfill_id)
        recovered = await recovery.recover(request)
        assert recovered.kind == "QUEUED"
        assert await repo.claim_job() is None
        work = await jobs.get(recovered.job_id)
        assert work is not None
        due = propagate_timestamp(work.model_dump(mode="json"), "run_after")
        assert due.value is not None
        current = parse_time(due.value)
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            completed = await observed_terminal_job(jobs, recovered.job_id, "DONE")
        finally:
            stop.set()
            await worker
        result = ReactionCaptureResultDocument.model_validate(completed.result).root
        assert result.kind == "CAPTURED" and result.revision == 2
        captured = await repo.get("out.reaction_checkpoints", result.checkpoint_key)
        assert captured is not None and captured["r_raw"] == pytest.approx(0.2)
        assert await repo.get("out.reaction_checkpoints", initial.checkpoint_key) == original_checkpoint
        assert await repo.get("ops.jobs", old_id) == old_before
        again = await recovery.recover(request)
        assert again.kind == "ALREADY_APPLIED"
        assert len(await repo.list("out.reaction_checkpoints")) == 2
        assert len(await repo.list("ops.jobs")) == 3


class RecordEventWire(Protocol):
    async def __call__(
        self, tool: Literal["record_event"], arguments: Record, context: Record
    ) -> JsonValue: ...


class MeasuredRecoveryHistory(DomainModel):
    request: ReactionRecoveryCommand
    original_command: RevisedReactionCommand
    checkpoint: CapturedReaction


async def measured_recovery_history(
    repo: StorageBackend, runtime: Runtime, now: datetime
) -> MeasuredRecoveryHistory:
    anchor = now - timedelta(hours=25)
    item_id = new_id("item")
    text = "Unverified synthetic benchmark reference at " + iso(anchor)
    item = source_item_fixture(
        item_id,
        text=text,
        source_class="SOCIAL_LEAD",
        published_at=iso(now),
        published_prec="EXACT",
        observed_at=iso(utcnow()),
    )
    await repo.put("ingest.source_items", item_id, item.storage_record())
    dispatch: RecordEventWire = runtime.sessions.tool_dispatch
    result = await dispatch(
        "record_event",
        {
            "item_id": item_id,
            "is_event": True,
            "statement": text,
            "certainty": "LOW",
            "certainty_reason": "Unverified authored social lead",
            "timing_confidence": "LOW",
            "entities": [],
            "token_ids": ["bitcoin:BTC"],
            "event_time": {"value": iso(anchor), "precision": "EXACT", "quote": text},
            "public_at": {
                "value": iso(now),
                "precision": "EXACT",
                "basis": "Authored social publication",
                "quote": text,
            },
            "freshness": "FRESH",
            "claims": [],
            "next_milestone": None,
            "checked_sources": [],
        },
        {},
    )
    assert isinstance(result, dict)
    event = ReactionEventIdentity.model_validate(
        {field: result[field] for field in ReactionEventIdentity.model_fields if field in result}
    )
    assert result["accepted_claims"] == []
    event_row = await repo.get("evt.events", event.event_id)
    assert event_row is not None
    assert event_row["event_state"] == "RUMOR" and event_row["state_verified"] is False
    assert event_row["certainty"] == "LOW" and event_row["best_source_class"] == "SOCIAL_LEAD"
    await repo.put(
        "evt.beneficiary_maps",
        event.event_id + ":1",
        {"event_id": event.event_id, "map_version": 1, "analysis_set": []},
    )
    instruments = [
        row.instrument_id
        for row in runtime.settings.registry().instruments
        if row.token_id == "bitcoin:BTC"
        and row.market_type == "SPOT"
        and row.active
        and ":spot:" in row.instrument_id
    ]
    assert instruments
    instrument_id = instruments[0]
    records: list[Record] = []
    keys: list[str] = []
    for minute in range(1441):
        opened = anchor - timedelta(minutes=1) + timedelta(minutes=minute)
        close = "110" if minute == 1440 else "100"
        keys.append(instrument_id + ":" + iso(opened))
        records.append(
            {
                "instrument_id": instrument_id,
                "market_type": "SPOT",
                "open_time": iso(opened),
                "interval_s": 60,
                "complete": True,
                "o": close,
                "h": close,
                "l": close,
                "c": close,
                "volume_native": "10",
                "quote_volume_usd": "1100" if minute == 1440 else "1000",
                **collection_clock(now - timedelta(seconds=1)).fields("recorded_at"),
            }
        )
    await repo.put_market_batch("mkt.klines_1m", records, keys=keys)
    first = await runtime.scheduler.reaction_capture(
        InitialReactionCommand(
            event_id=event.event_id,
            token_id="bitcoin:BTC",
            checkpoint="POST_24H",
            anchor=ReactionAnchorFacts(type="T0", ts=iso(anchor), ts_prec="EXACT"),
        )
    )
    assert first.kind == "CAPTURED"
    final_key = instrument_id + ":" + iso(anchor + timedelta(hours=24) - timedelta(minutes=1))
    final = await repo.get("mkt.klines_1m", final_key)
    assert final is not None
    await repo.put(
        "mkt.klines_1m",
        final_key,
        final
        | {
            "o": "120",
            "h": "120",
            "l": "120",
            "c": "120",
            "quote_volume_usd": "1200",
            **collection_clock().fields("recorded_at"),
        },
    )
    backfill_id = new_id("backfill")
    await repo.put(
        "mkt.backfill_runs",
        backfill_id,
        {
            "backfill_id": backfill_id,
            "event_id": event.event_id,
            "token_id": "bitcoin:BTC",
            "status": "FAILED",
            **Timestamp(iso(anchor), "EXACT").fields("anchor"),
            **collection_clock().fields("finished_at"),
        },
    )
    source_wire = await repo.get("mkt.backfill_runs", backfill_id)
    assert source_wire is not None
    source = recovery_backfill_from_record(source_wire)
    command = RevisedReactionCommand(
        event_id=event.event_id,
        token_id="bitcoin:BTC",
        checkpoint="POST_24H",
        automatic_revision=True,
        change_id=source.change_id + ":POST_24H",
        reason="Late capture completed: " + backfill_id + " (FAILED)",
    )
    original = await repo.enqueue(
        "REACTION_CAPTURE", command.storage_fields(), "retained-original:" + command.change_id, max_attempts=5
    )
    peer = await repo.enqueue(
        "REACTION_CAPTURE",
        command.storage_fields() | {"change_id": "retained-other-source:POST_24H"},
        "retained-peer:" + command.change_id,
        max_attempts=1,
    )
    original_id, peer_id = original["job_id"], peer["job_id"]
    assert isinstance(original_id, str) and isinstance(peer_id, str)
    await repo.put(
        "ops.jobs",
        original_id,
        original
        | {
            "status": "DONE",
            "attempts": 1,
            "result": {"coalesced_into": peer_id},
            **collection_clock().fields("finished_at"),
        },
    )
    await repo.put(
        "ops.jobs",
        peer_id,
        peer | {"status": "FAILED", "attempts": 1, **collection_clock().fields("finished_at")},
    )
    return MeasuredRecoveryHistory(
        request=ReactionRecoveryCommand(original_job_id=original_id, source_backfill_id=backfill_id),
        original_command=command,
        checkpoint=first,
    )


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("repo", ["postgres"], indirect=True)
@pytest.mark.parametrize("writer_kind", ["latest", "marker"])
async def test_recovery_snapshot_is_held_against_actual_checkpoint_and_marker_writers(
    repo: StorageBackend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    writer_kind: Literal["latest", "marker"],
) -> None:
    async with public_runtime(repo, tmp_path, monkeypatch) as (runtime, router):
        endpoint = httpx.URL(os.environ["CLICKHOUSE_TEST_URL"])
        router.route(
            method="POST", scheme=endpoint.scheme, host=endpoint.host, port=endpoint.port, path="/"
        ).pass_through()
        now = utcnow().replace(second=0, microsecond=0)
        history = await measured_recovery_history(repo, runtime, now)
        independent_history = await measured_recovery_history(repo, runtime, now)
        original_before = await repo.get("ops.jobs", history.request.original_job_id)
        target = history.original_command
        target_key = f"reaction:{target.event_id}:{target.token_id}:{target.checkpoint}"
        selected, release = asyncio.Event(), asyncio.Event()
        writer_attempted, writer_acquired = asyncio.Event(), asyncio.Event()
        recovery_task: asyncio.Task[ReactionRecoveryOutcome] | None = None
        writer_task: asyncio.Task[ReactionCaptureResult] | None = None
        read_metadata, acquire_lock = repo.iter_metadata, repo.lock

        async def observed_metadata(
            collection: str, fields: Sequence[str], *, filters: Record | None = None, batch_size: int = 1000
        ) -> AsyncIterator[Record]:
            async for row in read_metadata(collection, fields, filters=filters, batch_size=batch_size):
                yield row
            if collection == "out.reaction_checkpoints" and asyncio.current_task() is recovery_task:
                selected.set()
                await release.wait()

        @asynccontextmanager
        async def observed_lock(key: str) -> AsyncIterator[StorageBackend]:
            watching = key == target_key and asyncio.current_task() is writer_task
            if watching:
                writer_attempted.set()
            async with acquire_lock(key) as held:
                if watching:
                    writer_acquired.set()
                yield held

        monkeypatch.setattr(repo, "iter_metadata", observed_metadata)
        monkeypatch.setattr(repo, "lock", observed_lock)
        recovery = ReactionRecovery(
            repo,
            execution=ReactionRecoveryExecution(
                versions=runtime.settings.require_versions(), replay=runtime.settings.replay
            ),
            clock=collection_clock,
        )
        recovery_task = asyncio.create_task(recovery.recover(history.request))
        try:
            async with asyncio.timeout(10):
                await selected.wait()
                command = (
                    ManualReactionRevisionCommand(
                        event_id=target.event_id,
                        token_id=target.token_id,
                        checkpoint=target.checkpoint,
                        revision=history.checkpoint.revision + 1,
                        reason="Concurrent actual newer checkpoint",
                    )
                    if writer_kind == "latest"
                    else target
                )
                writer_task = asyncio.create_task(runtime.scheduler.reaction_capture(command))
                await writer_attempted.wait()
                assert not writer_acquired.is_set()
                independent = await recovery.recover(independent_history.request)
                assert independent.kind == "QUEUED"
                assert not writer_acquired.is_set()
                release.set()
                queued = await recovery_task
                assert queued.kind == "QUEUED"
                written = await writer_task
                assert writer_acquired.is_set()
                assert written.kind == "CAPTURED" and written.revision == 2
        finally:
            release.set()
            for task in (recovery_task, writer_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (recovery_task, writer_task) if task is not None), return_exceptions=True
            )
        assert await repo.get("ops.jobs", history.request.original_job_id) == original_before
        link_wire = await repo.get("ops.scheduler_checkpoints", queued.recovery_link_key)
        assert link_wire is not None
        link = ReactionRecoveryLink.model_validate(link_wire)
        assert link.state.checkpoint == history.checkpoint
        jobs_before = await repo.list("ops.jobs")
        repeated = await recovery.recover(history.request)
        if writer_kind == "marker":
            assert repeated.kind == "ALREADY_APPLIED"
        else:
            assert repeated.kind == "UNRESOLVED" and repeated.reason == "RECOVERY_LINK_UNADMITTED"
        assert await repo.list("ops.jobs") == jobs_before
