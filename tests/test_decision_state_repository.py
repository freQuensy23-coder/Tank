"""Decision current-state ordering follows committed revisions, never clock ties."""

from collections.abc import Mapping

import pytest
from pydantic import JsonValue
from test_decision_contracts import frozen_decision_fixture, status_writer_fixture

from tank.analysis.decision_codec import encode_decision
from tank.analysis.decision_contracts import DecisionRecord
from tank.analysis.decision_state import DecisionScope, DecisionStateConflict, DecisionStateRepository
from tank.analysis.decisions import end_decision_tracking
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock
from tank.util import JsonInput


def renamed(record: DecisionRecord, identity: str) -> DecisionRecord:
    financial = record.metadata.identity.model_copy(update={"decision_id": identity})
    return DecisionRecord.model_validate(
        record.model_copy(update={"metadata": record.metadata.model_copy(update={"identity": financial})})
    )


def scope(record: DecisionRecord) -> DecisionScope:
    return DecisionScope(
        event_id=record.metadata.identity.event_id, token_id=record.metadata.identity.token_id
    )


async def no_effects() -> None:
    """An empty write set explicitly has no reconciliation changes."""


async def test_equal_write_clocks_order_by_revision_and_a_new_active_state_can_follow_ended() -> None:
    store = MemoryRepository()
    states = DecisionStateRepository(store)
    active = frozen_decision_fixture()
    clock = collection_clock("2026-10-28T00:00:00Z")
    first = await states.advance(
        scope(active), expected=None, candidate=active, recorded_at=clock, effects=no_effects
    )
    ended = end_decision_tracking(
        active,
        decision_id="ended",
        expired_call_ids=("expired",),
        status_recorded_at=clock,
        writer=status_writer_fixture(active),
    )
    second = await states.advance(
        scope(active), expected=first, candidate=ended, recorded_at=clock, effects=no_effects
    )
    assert second.revision == 2 and second.previous_head_dec_id == first.dec_id
    assert second.recorded_at == first.recorded_at
    selected = await states.current(scope(active))
    assert selected is not None and selected[1] == ended
    successor = renamed(active, "new-active")
    third = await states.advance(
        scope(active), expected=second, candidate=successor, recorded_at=clock, effects=no_effects
    )
    assert third.revision == 3 and third.previous_head_dec_id == "ended"
    selected = await states.current(scope(active))
    assert selected is not None and selected[1] == successor
    assert successor.metadata.identity.cutoff == active.metadata.identity.cutoff


async def test_reverse_commit_refuses_stale_expected_head_before_any_effect() -> None:
    store = MemoryRepository()
    states = DecisionStateRepository(store)
    active = frozen_decision_fixture()
    clock = collection_clock("2026-10-28T00:00:00Z")
    first = await states.advance(
        scope(active), expected=None, candidate=active, recorded_at=clock, effects=no_effects
    )
    successor = renamed(active, "newer-authoritative")
    second = await states.advance(
        scope(active), expected=first, candidate=successor, recorded_at=clock, effects=no_effects
    )
    effect_calls: list[str] = []

    async def record_effect() -> None:
        effect_calls.append("stale mutation")

    with pytest.raises(DecisionStateConflict, match="changed after"):
        await states.advance(
            scope(active),
            expected=first,
            candidate=renamed(active, "stale"),
            recorded_at=clock,
            effects=record_effect,
        )
    assert effect_calls == []
    assert await store.get("ana.decisions", "stale") is None
    selected = await states.current(scope(active))
    assert selected is not None and selected[0] == second


async def test_ended_copy_cannot_close_an_unrelated_active_successor() -> None:
    store = MemoryRepository()
    states = DecisionStateRepository(store)
    original = frozen_decision_fixture()
    clock = collection_clock("2026-10-28T00:00:00Z")
    successor = renamed(original, "successor")
    first = await states.advance(
        scope(original), expected=None, candidate=successor, recorded_at=clock, effects=no_effects
    )
    ended = end_decision_tracking(
        original,
        decision_id="stale-ended",
        expired_call_ids=("old-call",),
        status_recorded_at=clock,
        writer=status_writer_fixture(original),
    )
    with pytest.raises(DecisionStateConflict, match="actual expected financial source"):
        await states.advance(
            scope(original), expected=first, candidate=ended, recorded_at=clock, effects=no_effects
        )
    assert await store.get("ana.decisions", "stale-ended") is None


async def test_reused_immutable_identity_cannot_change_even_unmodeled_wire_evidence() -> None:
    store = MemoryRepository()
    record = frozen_decision_fixture()
    wire = encode_decision(record)
    wire["historical_extra_evidence"] = "original"
    await store.put("ana.decisions", record.metadata.identity.decision_id, wire, immutable=True)
    with pytest.raises(DecisionStateConflict, match="conflicting content"):
        await DecisionStateRepository(store).advance(
            scope(record),
            expected=None,
            candidate=record,
            recorded_at=collection_clock("2026-10-28T00:00:00Z"),
            effects=no_effects,
        )


class FailingHeadStore(MemoryRepository):
    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]:
        if collection == "ana.decision_state_heads":
            raise RuntimeError("head write failed")
        return await super().put(collection, key, record, immutable=immutable)


async def test_failed_final_head_advancement_rolls_back_effects_and_embedded_financial_row() -> None:
    store = FailingHeadStore()
    record = frozen_decision_fixture()

    async def effects() -> None:
        await store.put("ana.watch_conditions", "effect", {"cond_id": "effect", "status": "CANCELLED"})

    with pytest.raises(RuntimeError, match="head write failed"):
        await DecisionStateRepository(store).advance(
            scope(record),
            expected=None,
            candidate=record,
            recorded_at=collection_clock("2026-10-28T00:00:00Z"),
            effects=effects,
        )
    assert await store.get("ana.watch_conditions", "effect") is None
    assert await store.get("ana.decisions", record.metadata.identity.decision_id) is None
    assert await DecisionStateRepository(store).current(scope(record)) is None


async def test_older_input_cutoff_cannot_replace_current_even_with_fresh_expected_revision() -> None:
    store = MemoryRepository()
    states = DecisionStateRepository(store)
    active = frozen_decision_fixture()
    clock = collection_clock("2026-10-28T00:00:00Z")
    first = await states.advance(
        scope(active), expected=None, candidate=active, recorded_at=clock, effects=no_effects
    )
    stale = renamed(active, "older-inputs")
    identity = stale.metadata.identity.model_copy(update={"cutoff": collection_clock("2026-10-24T00:00:00Z")})
    stale = DecisionRecord.model_validate(
        stale.model_copy(update={"metadata": stale.metadata.model_copy(update={"identity": identity})})
    )
    with pytest.raises(DecisionStateConflict, match="older financial input cutoff"):
        await states.advance(
            scope(active), expected=first, candidate=stale, recorded_at=clock, effects=no_effects
        )
    assert await store.get("ana.decisions", "older-inputs") is None


async def test_current_reader_refuses_a_document_that_no_longer_matches_its_head_digest() -> None:
    store = MemoryRepository()
    states = DecisionStateRepository(store)
    record = frozen_decision_fixture()
    await states.advance(
        scope(record),
        expected=None,
        candidate=record,
        recorded_at=collection_clock("2026-10-28T00:00:00Z"),
        effects=no_effects,
    )
    head = await store.get("ana.decision_state_heads", scope(record).key())
    assert head is not None
    head["dec_sha256"] = "0" * 64
    head["revision"] = 2
    head["previous_head_dec_id"] = record.metadata.identity.decision_id
    await store.put("ana.decision_state_heads", scope(record).key(), head)
    with pytest.raises(ValueError, match="actual immutable financial document"):
        await states.current(scope(record))


async def test_concurrent_equal_clock_first_writers_have_one_commit_and_one_stale_refusal() -> None:
    import asyncio

    store = MemoryRepository()
    states = DecisionStateRepository(store)
    record = frozen_decision_fixture()
    entered = asyncio.Event()
    release = asyncio.Event()
    effects_seen: list[str] = []

    async def first_effects() -> None:
        entered.set()
        await release.wait()
        effects_seen.append("first")

    async def stale_effects() -> None:
        effects_seen.append("stale")

    clock = collection_clock("2026-10-28T00:00:00Z")
    first = asyncio.create_task(
        states.advance(
            scope(record), expected=None, candidate=record, recorded_at=clock, effects=first_effects
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    second = asyncio.create_task(
        states.advance(
            scope(record),
            expected=None,
            candidate=renamed(record, "second"),
            recorded_at=clock,
            effects=stale_effects,
        )
    )
    release.set()
    committed = await first
    with pytest.raises(DecisionStateConflict, match="changed after"):
        await second
    assert committed.revision == 1 and effects_seen == ["first"]
    assert await store.get("ana.decisions", "second") is None


async def test_sole_legacy_display_projection_needs_no_invented_calculation_metadata() -> None:
    store = MemoryRepository()
    await store.put(
        "ana.decisions",
        "historical-display",
        {"dec_id": "historical-display", "event_id": "event", "token_id": "token", "action": "WAIT"},
    )
    selection = await DecisionStateRepository(store).legacy_current(
        DecisionScope(event_id="event", token_id="token")
    )
    assert selection is not None
    assert selection.basis == "SOLE_LEGACY_RECORD" and selection.decision_id == "historical-display"
    assert await store.get("ana.decision_state_heads", '["event","token"]') is None


async def test_legacy_equal_clock_status_lineage_is_explicit_without_a_historical_revision() -> None:
    store = MemoryRepository()
    record = frozen_decision_fixture()
    clock = record.metadata.calculated_at
    ended = end_decision_tracking(
        record,
        decision_id="legacy-ended",
        expired_call_ids=("expired",),
        status_recorded_at=clock,
        writer=status_writer_fixture(record),
    )
    await store.put("ana.decisions", record.metadata.identity.decision_id, encode_decision(record))
    await store.put("ana.decisions", "legacy-ended", encode_decision(ended))
    states = DecisionStateRepository(store)
    selection = await states.legacy_current(scope(record))
    assert selection is not None
    assert selection.basis == "EXPLICIT_STATUS_LINEAGE" and selection.decision_id == "legacy-ended"
    assert await states.current(scope(record)) is None


async def test_unrelated_legacy_equal_clock_siblings_remain_ambiguous() -> None:
    store = MemoryRepository()
    record = frozen_decision_fixture()
    await store.put("ana.decisions", record.metadata.identity.decision_id, encode_decision(record))
    sibling = renamed(record, "sibling")
    await store.put("ana.decisions", "sibling", encode_decision(sibling))
    with pytest.raises(DecisionStateConflict, match="equal-clock order is ambiguous"):
        await DecisionStateRepository(store).legacy_current(scope(record))


async def persisted_financial_state(store: MemoryRepository) -> dict[str, list[dict[str, JsonValue]]]:
    return {
        table: await store.list(table)
        for table in ("ana.watch_conditions", "ana.decisions", "ana.decision_state_heads")
    }


@pytest.mark.parametrize("supply_witness", [False, True])
async def test_first_active_head_cannot_bypass_newer_legacy_financial_state(
    supply_witness: bool,
) -> None:
    store = MemoryRepository()
    legacy = frozen_decision_fixture()
    await store.put("ana.decisions", legacy.metadata.identity.decision_id, encode_decision(legacy))
    states = DecisionStateRepository(store)
    witness = await states.legacy_current(scope(legacy))
    assert witness is not None
    candidate = renamed(legacy, "older-first-head")
    identity = candidate.metadata.identity.model_copy(
        update={"cutoff": collection_clock("2026-10-24T00:00:00Z")}
    )
    candidate = DecisionRecord.model_validate(
        candidate.model_copy(
            update={
                "metadata": candidate.metadata.model_copy(update={"identity": identity}),
                "outlook_id": "older-first-head:outlook",
            }
        )
    )
    before = await persisted_financial_state(store)
    effects_seen: list[str] = []

    async def effects() -> None:
        effects_seen.append("must not run")
        watch = legacy.conditions[0]
        await store.put("ana.watch_conditions", watch.cond_id, watch.model_dump(mode="json"))

    with pytest.raises(DecisionStateConflict, match="older financial input cutoff"):
        if supply_witness:
            await states.advance(
                scope(legacy),
                expected=None,
                candidate=candidate,
                recorded_at=collection_clock("2026-10-27T00:00:00Z"),
                effects=effects,
                legacy_source=witness,
            )
        else:
            await states.advance(
                scope(legacy),
                expected=None,
                candidate=candidate,
                recorded_at=collection_clock("2026-10-27T00:00:00Z"),
                effects=effects,
            )
    assert effects_seen == []
    assert await persisted_financial_state(store) == before
    assert await states.current(scope(legacy)) is None


async def test_first_active_head_cannot_omit_an_ambiguous_legacy_predecessor() -> None:
    store = MemoryRepository()
    legacy = frozen_decision_fixture()
    sibling = renamed(legacy, "legacy-sibling")
    await store.put("ana.decisions", legacy.metadata.identity.decision_id, encode_decision(legacy))
    await store.put("ana.decisions", sibling.metadata.identity.decision_id, encode_decision(sibling))
    before = await persisted_financial_state(store)
    effects_seen: list[str] = []

    async def effects() -> None:
        effects_seen.append("must not run")

    with pytest.raises(DecisionStateConflict, match="equal-clock order is ambiguous"):
        await DecisionStateRepository(store).advance(
            scope(legacy),
            expected=None,
            candidate=renamed(legacy, "new-first-head"),
            recorded_at=collection_clock("2026-10-27T00:00:00Z"),
            effects=effects,
        )
    assert effects_seen == []
    assert await persisted_financial_state(store) == before


async def test_legacy_witness_binds_the_actual_full_document_even_when_identity_is_unchanged() -> None:
    store = MemoryRepository()
    legacy = frozen_decision_fixture()
    identity = legacy.metadata.identity.decision_id
    wire = encode_decision(legacy)
    await store.put("ana.decisions", identity, wire)
    states = DecisionStateRepository(store)
    witness = await states.legacy_current(scope(legacy))
    assert witness is not None
    wire["historical_extra_evidence"] = "changed after input observation"
    await store.put("ana.decisions", identity, wire)
    before = await persisted_financial_state(store)

    with pytest.raises(DecisionStateConflict, match="Legacy financial source changed"):
        await states.advance(
            scope(legacy),
            expected=None,
            candidate=renamed(legacy, "new-first-head"),
            recorded_at=collection_clock("2026-10-27T00:00:00Z"),
            effects=no_effects,
            legacy_source=witness,
        )
    assert await persisted_financial_state(store) == before
