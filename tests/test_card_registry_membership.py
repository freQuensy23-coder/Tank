"""Registry membership and WHO evidence are independent affirmative facts."""

from hashlib import sha256

import pytest
from card_registry_support import fixture_registry_membership
from card_wire_support import card_inputs, wire_card_inputs
from foundation_helpers import object_field
from pydantic import JsonValue

from tank.analysis.card_codec import card_inputs_from_records, card_registry_from_record, encode_card_registry
from tank.analysis.card_contracts import CardInputs, CardRegistryMembership


def projected(
    registry: CardRegistryMembership, *, evidence: bool = True, display_label: bool = True
) -> CardInputs:
    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    if not evidence:
        beneficiary["evidence"] = []
    if not display_label:
        del object_field(event, "token_labels")["world:WLD"]
    authored = card_inputs()
    return card_inputs_from_records(
        event,
        assessment,
        [beneficiary],
        [decision],
        [truth],
        registry=registry,
        narrative=authored.narrative,
        policy=authored.policy,
    )


@pytest.mark.parametrize("evidence", [True, False])
def test_unregistered_selected_row_refuses_even_when_its_label_exists(evidence: bool) -> None:
    empty = card_registry_from_record({"tokens": []})
    with pytest.raises(ValueError, match="outside-scope participant"):
        projected(empty, evidence=evidence)


def test_actual_registered_membership_survives_a_missing_display_label() -> None:
    registry = fixture_registry_membership()
    actual = projected(registry, display_label=False)
    assert actual.beneficiaries[0].subject.registry_membership == "REGISTERED"
    assert actual.registry == registry and "world:WLD" in actual.registry.token_ids


def test_falsely_declared_registered_status_does_not_bypass_the_actual_roster() -> None:
    actual = projected(fixture_registry_membership())
    empty = card_registry_from_record({"tokens": []})
    with pytest.raises(ValueError, match="classification disagrees with actual registry"):
        CardInputs.model_validate(actual.model_copy(update={"registry": empty}))


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"tokens": [{}]},
        {"tokens": [{"token_id": "world:WLD"}, {"token_id": "world:WLD"}]},
        {"tokens": [{"token_id": ""}]},
    ],
)
def test_missing_or_ambiguous_registry_evidence_is_not_membership(record: dict[str, JsonValue]) -> None:
    with pytest.raises(ValueError):
        card_registry_from_record(record)


def test_membership_evidence_keeps_the_actual_snapshot_hash_and_stable_set_order() -> None:
    registry = card_registry_from_record({"tokens": [{"token_id": "b"}, {"token_id": "a"}]})
    expected = sha256(b'{"tokens":[{"token_id":"b"},{"token_id":"a"}]}').hexdigest()
    encoded = encode_card_registry(registry)
    assert encoded == {"token_ids": ["a", "b"], "registry_snapshot_sha256": expected}
