"""Observed zero and unavailable provider explanations remain distinct at rendering."""

from copy import deepcopy

import pytest
from card_registry_support import fixture_registry_membership
from card_wire_support import wire_card_inputs
from pydantic import JsonValue

from tank.analysis.card_codec import card_inputs_from_records, holders_revenue_from_record
from tank.analysis.card_contracts import (
    CardRevenue,
    PublicationNarrative,
    PublicationPolicy,
    UnavailableCardRevenue,
)
from tank.analysis.cards import holders_revenue_text


@pytest.mark.parametrize(
    "reason,label",
    [
        ("NO_CAPTURED_FUNDAMENTALS", "not captured"),
        ("ONLY_FUTURE_FUNDAMENTALS", "not available by cutoff"),
        ("NO_SUPPORTED_PUBLIC_FEES_ADAPTER", "provider metric unsupported"),
        ("NO_PUBLIC_FEES_ADAPTER_MATCHING_TOKEN_IDENTITY", "no verified provider match"),
        ("http error https://provider.invalid/?key=synthetic-secret", "incomplete provider data"),
        (None, "incomplete provider data"),
    ],
)
def test_unavailable_root_has_no_invented_observation_or_provider_value(
    reason: str | None, label: str
) -> None:
    recorded: dict[str, JsonValue] = {"protocols": [], "reason": reason}
    original = deepcopy(recorded)
    result = holders_revenue_from_record(recorded)
    assert isinstance(result, UnavailableCardRevenue)
    assert holders_revenue_text(result) == "holders revenue: n/a (" + label + ")"
    assert recorded == original
    assert "synthetic-secret" not in holders_revenue_text(result)


def test_known_zero_remains_an_observed_provider_period() -> None:
    recorded: dict[str, JsonValue] = {
        "status": "OBSERVED",
        "as_of": "2026-10-19T12:00:00Z",
        "as_of_prec": "COLLECTION_ONLY",
        "as_of_prec_reason": None,
        "coverage": {"24h": {"complete": True}},
        "protocols": [
            {
                "protocol": "adapter",
                "unit": "USD",
                "periods": [{"value": "0", "provider_period": "24h"}],
                "latest_chart_point": None,
                "reason": None,
            }
        ],
    }
    result = holders_revenue_from_record(recorded)
    assert isinstance(result, CardRevenue)
    assert "adapter 0 USD/24h" in holders_revenue_text(result)
    assert "observed 2026-10-19 12:00Z (collection)" in holders_revenue_text(result)
    assert "n/a" not in holders_revenue_text(result)


def test_malformed_root_absence_is_not_a_safe_reason_alias() -> None:
    with pytest.raises(ValueError, match="text or null"):
        holders_revenue_from_record({"protocols": [], "reason": {"error": "malformed"}})


def test_explicit_unprojected_feature_is_distinct_from_captured_unavailability() -> None:
    event, assessment, beneficiary, decision, market = wire_card_inputs()
    market["supply"] = {"holders_revenue": None}
    inputs = card_inputs_from_records(
        event,
        assessment,
        [beneficiary],
        [decision],
        [market],
        registry=fixture_registry_membership(),
        narrative=PublicationNarrative(
            kind="FULL",
            what="A source confirms approval.",
            verdict="Approval is confirmed.",
            why=("The source confirms approval", "Effects remain uncertain"),
        ),
        policy=PublicationPolicy(card_max_chars=5000, update_rate_limit_s=900),
    )
    assert inputs.market_by_token["world:WLD"].holders_revenue is None
    market["supply"] = {"holders_revenue": {"protocols": [], "reason": "NO_CAPTURED_FUNDAMENTALS"}}
    projected = card_inputs_from_records(
        event,
        assessment,
        [beneficiary],
        [decision],
        [market],
        registry=fixture_registry_membership(),
        narrative=inputs.narrative,
        policy=inputs.policy,
    )
    assert isinstance(projected.market_by_token["world:WLD"].holders_revenue, UnavailableCardRevenue)
