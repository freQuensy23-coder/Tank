"""Actual narrow call facts and reconciled outlook persistence boundaries."""

from decimal import Decimal

import pytest
from foundation_helpers import object_field
from pydantic import JsonValue
from test_decision_contracts import frozen_decision_fixture

from tank.analysis.decision_codec import (
    decision_call_from_record,
    decode_decision,
    decode_outlook,
    encode_decision,
    encode_outlook,
)
from tank.analysis.decision_contracts import (
    DecisionCore,
    DecisionRecord,
    DecisionScenarioReasons,
    DecisionScenarios,
    DecisionStatusWriter,
    OutlookConditionRefs,
)
from tank.analysis.decisions import end_decision_tracking
from tank.time_precision import collection_clock


def call_record() -> dict[str, JsonValue]:
    return {
        "call_id": "call",
        "event_id": "event",
        "token_id": "token",
        "status": "OPEN",
        "opened_at": "2026-10-20T12:03:00Z",
        "invalidation_price": "9.125",
    }


def test_call_projection_needs_no_uncaptured_delivery_or_entry_facts() -> None:
    record = call_record()
    context = decision_call_from_record(record)
    assert context.call_id == "call"
    assert context.invalidation_price == Decimal("9.125")
    assert context.opened_at.value == record["opened_at"]
    assert context.opened_at.precision is None
    assert context.opened_at.precision_reason == "INCOMPLETE"
    assert context.opened_instant().isoformat() == "2026-10-20T12:03:00+00:00"
    assert "opened_by_alert" not in record and "entry_ref_price" not in record


@pytest.mark.parametrize(
    "field,value", [("opened_at", None), ("status", "CLOSED"), ("invalidation_price", None)]
)
def test_call_projection_refuses_missing_consumed_facts(field: str, value: JsonValue) -> None:
    record = call_record()
    record[field] = value
    with pytest.raises(ValueError):
        decision_call_from_record(record)


def test_outlook_row_and_embedded_refs_derive_from_actual_reconciled_watches() -> None:
    record = frozen_decision_fixture()
    expected = OutlookConditionRefs.from_conditions(record.conditions)
    decision_wire = encode_decision(record)
    refs = object_field(object_field(decision_wire, "outlook"), "conditions")
    assert refs == {"upgrade": list(expected.upgrade), "downgrade": list(expected.downgrade)}
    assert decode_decision(decision_wire) == record
    row = encode_outlook(record)
    actual = decode_outlook(row)
    assert actual.conditions == expected
    assert actual.calculation == record.calculation.outlook
    assert actual.cutoff == record.metadata.identity.cutoff
    assert actual.generation == record.metadata.identity.generation
    assert actual.versions == record.metadata.identity.versions
    assert actual.outlook_id == record.outlook_id
    assert row["event_id"] == record.metadata.identity.event_id
    assert row["token_id"] == record.metadata.identity.token_id


def test_embedded_outlook_cannot_claim_an_unreconciled_watch() -> None:
    wire = encode_decision(frozen_decision_fixture())
    object_field(wire, "outlook")["conditions"] = {"upgrade": ["unrelated"], "downgrade": []}
    with pytest.raises(ValueError, match="disagree with reconciled"):
        decode_decision(wire)


def test_ended_tracking_cannot_replace_the_historical_outlook_row() -> None:
    frozen = frozen_decision_fixture()
    ended = end_decision_tracking(
        frozen,
        decision_id="ended",
        expired_call_ids=("expired-call",),
        status_recorded_at=collection_clock("2026-10-27T00:00:00Z"),
        writer=DecisionStatusWriter(
            versions=frozen.metadata.identity.versions,
            generation=frozen.metadata.identity.generation,
        ),
    )
    assert ended.outlook_id == frozen.outlook_id
    with pytest.raises(ValueError, match="historical outlook"):
        encode_outlook(ended)
    assert decode_decision(encode_decision(ended)) == ended
    assert encode_outlook(frozen)["out_id"] == frozen.outlook_id


def test_available_scenarios_keep_the_sparse_reason_wire_contract() -> None:
    record = frozen_decision_fixture()
    reasons = DecisionScenarioReasons(base=None, upside=None, failure=None)
    outlook = record.calculation.outlook.model_copy(
        update={
            "scenarios": DecisionScenarios(
                base="The authored product remains available.",
                upside="The authored deployment is verified.",
                failure="The authored approval is reversed.",
            ),
            "scenario_reasons": reasons,
        }
    )
    calculation = DecisionCore.model_validate(record.calculation.model_copy(update={"outlook": outlook}))
    declared = DecisionRecord.model_validate(record.model_copy(update={"calculation": calculation}))
    row = encode_outlook(declared)
    assert row["scenario_reasons"] == {}
    assert decode_outlook(row).calculation.scenario_reasons == reasons
