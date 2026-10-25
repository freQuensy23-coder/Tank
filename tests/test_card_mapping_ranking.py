"""Uncomputed ranking is distinct from a computed least-repriced designation."""

import pytest
from card_registry_support import fixture_registry_membership
from card_wire_support import card_inputs, wire_card_inputs
from foundation_helpers import object_field
from pydantic import JsonValue

from tank.analysis.card_codec import card_inputs_from_records
from tank.analysis.cards import compose_card


@pytest.mark.parametrize(
    ("present", "ranking", "expected"),
    [(False, None, None), (True, None, False), (True, "edge", True), (True, "other-edge", False)],
)
def test_card_ranking_phase_preserves_unknown_and_computed_designation(
    present: bool, ranking: str | None, expected: bool | None
) -> None:
    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    beneficiary["edge_id"] = "edge"
    mapping = object_field(event, "beneficiary_map")
    if present:
        mapping["least_repriced_edge"] = ranking
    else:
        del mapping["least_repriced_edge"]
    authored = card_inputs()
    card = card_inputs_from_records(
        event,
        assessment,
        [beneficiary],
        [decision],
        [truth],
        registry=fixture_registry_membership(),
        narrative=authored.narrative,
        policy=authored.policy,
    )
    assert card.beneficiaries[0].least_repriced is expected
    assert (" · least-repriced" in compose_card(card)) is (expected is True)


@pytest.mark.parametrize("ranking", [False, 0, "", ["edge"]])
def test_computed_ranking_requires_a_real_edge_identity(ranking: JsonValue) -> None:
    event, assessment, beneficiary, decision, truth = wire_card_inputs()
    beneficiary["edge_id"] = "edge"
    object_field(event, "beneficiary_map")["least_repriced_edge"] = ranking
    authored = card_inputs()
    with pytest.raises(ValueError, match="least_repriced_edge"):
        card_inputs_from_records(
            event,
            assessment,
            [beneficiary],
            [decision],
            [truth],
            registry=fixture_registry_membership(),
            narrative=authored.narrative,
            policy=authored.policy,
        )
