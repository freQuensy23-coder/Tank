"""Evidence-gated recovery admission and actual transactional queue/link effects."""

import asyncio
from collections.abc import Mapping

import pytest
from pydantic import JsonValue, ValidationError
from source_support import fixture_versions
from test_storage import repo as repo

from tank.contracts.reaction_recovery import (
    ReactionRecoveryCommand,
    ReactionRecoveryEvidence,
    ReactionRecoveryExecution,
    ReactionRecoveryLink,
    ReactionRecoveryUnresolved,
    ReactionRecoveryWitnessRequest,
    RetainedCoalescedReaction,
)
from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.models import Record
from tank.reaction_recovery import (
    ReactionRecovery,
    RecoveryWitnessIndex,
    admit_recovery_witnesses,
    recovery_backfill_from_record,
    recovery_eligibility,
    recovery_idempotency_key,
    recovery_link_key,
)
from tank.storage import ImmutableRecordError, MemoryRepository, StorageBackend
from tank.time_precision import Timestamp, collection_clock, propagate_timestamp
from tank.util import JsonInput, parse_time, sha256


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
