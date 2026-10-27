"""Owned funding reads retain immutable facts and fresh selected-source correlation."""

from funding_history_fixtures import policy, request, rows
from funding_input_fixtures import funding_scope_fixture

from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_funding_codec import FundingConditionLookup
from tank.time_precision import Timestamp


def test_unavailable_current_cannot_upgrade_selected_settled_age_or_identity() -> None:
    records = rows()
    records[-1].update(rate=None, interval_h="-4", rate_8h_eq=None)
    lookup = FundingConditionLookup(
        token_id="T",
        instruments=[funding_scope_fixture()],
        observations=records,
        request=request(),
        policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W1H",
    )
    observations = lookup.selected_observations()
    assert len(observations) == 1
    assert observations[0].kind == "SETTLED"
    assert observations[0].clock.timestamp.value == "2026-10-09T08:00:00Z"
    assert observations[0].evidence == lookup.facts.selected_reference(
        observations[0].instrument.instrument_id
    )
    assert observations[0].clock.timestamp.value != records[-1]["as_of"]
    assert lookup.owns_facts(lookup.facts)
    assert not lookup.owns_facts(lookup.facts.model_copy())
    before = lookup.facts
    records[1]["rate_8h_eq"] = "999"
    records[-1]["as_of"] = "2026-10-16T12:04:00Z"
    assert lookup.facts is before
    assert lookup.selected_observations() == observations


def test_visibility_signature_changes_only_when_actual_source_eligibility_changes() -> None:
    records = rows()
    records[-1]["received_ts"] = "2026-10-16T13:00:00Z"
    lookup = FundingConditionLookup(
        token_id="T",
        instruments=[funding_scope_fixture()],
        observations=records,
        request=request(),
        policy=policy(),
        history_cache=BaselineCaches.with_capacity(128).funding_history,
        window="W1H",
    )
    later_same = Timestamp(value="2026-10-16T12:10:00Z", precision="COLLECTION_ONLY", precision_reason=None)
    later_eligible = Timestamp(
        value="2026-10-16T14:00:00Z", precision="COLLECTION_ONLY", precision_reason=None
    )
    assert lookup.matches_visibility(later_same)
    assert not lookup.matches_visibility(later_eligible)
    assert "assembled_at" not in type(lookup.facts).model_fields
    assert lookup.selected_observations()[0].clock.timestamp.value == "2026-10-09T08:00:00Z"
