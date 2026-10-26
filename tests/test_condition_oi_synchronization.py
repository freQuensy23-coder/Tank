"""Synchronized OI keeps instrument samples distinct from its logical grid."""

from datetime import timedelta
from decimal import Decimal
from typing import Literal

import pytest
from pydantic import ValidationError
from test_condition_derivative_selection import derivative_view
from test_condition_graph_contracts import observed_oi_without_usd

from tank.analysis.condition_graph_codec import encode_condition_derivatives
from tank.analysis.condition_graph_contracts import (
    AssessedConditionProofFreshness,
    ConditionFieldObservation,
    ConditionOISynchronization,
    ConditionOISynchronizationContribution,
    ConditionOISynchronizationEndpoint,
    ConditionRequestedDerivativeView,
    ConditionSynchronizedOpenInterest,
)
from tank.time_precision import Timestamp
from tank.util import iso, parse_iso_time


def two_contract_synchronization() -> ConditionSynchronizedOpenInterest:
    point = observed_oi_without_usd()
    point = type(point).model_validate(point.model_copy(update={"oi_usd_start": None, "oi_usd_end": None}))
    assert point.start_observed_at.value is not None
    assert point.end_observed_at.value is not None

    def endpoint(side: Literal["start", "end"]) -> ConditionOISynchronizationEndpoint:
        grid = point.start_observed_at if side == "start" else point.end_observed_at
        assert grid.value is not None
        return ConditionOISynchronizationEndpoint(
            at=grid,
            contributions=tuple(
                ConditionOISynchronizationContribution(
                    instrument_id=identity,
                    venue="bybit",
                    observed_at=Timestamp(
                        value=iso(parse_iso_time(grid.value) - timedelta(seconds=offset)),
                        precision=None,
                        precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
                    ),
                    oi_native=quantity if side == "start" else Decimal(0),
                    oi_usd=None,
                )
                for identity, offset, quantity in (
                    ("contract-a", 1, Decimal(6)),
                    ("contract-b", 2, Decimal(4)),
                )
            ),
        )

    return ConditionSynchronizedOpenInterest(
        status="OBSERVED",
        venues=("bybit",),
        values=point,
        synchronization=ConditionOISynchronization(
            aggregation_class="DECLARED_SYNTHETIC_BASE_QUANTITY",
            tolerance_s=2.0,
            start=endpoint("start"),
            end=endpoint("end"),
        ),
    )


def test_synchronized_oi_retains_each_contract_and_actual_source_clock() -> None:
    value = two_contract_synchronization()
    assert value.values.oi_native_start == Decimal(10)
    assert value.values.oi_native_end == Decimal(0)
    assert value.venues == ("bybit",)
    assert [entry.instrument_id for entry in value.synchronization.end.contributions] == [
        "contract-a",
        "contract-b",
    ]
    assert all(
        entry.observed_at != value.synchronization.end.at for entry in value.synchronization.end.contributions
    )
    assert value.synchronization.end.usd_total() is None


@pytest.mark.parametrize("mutation", ["quantity", "clock", "membership", "venue", "usd"])
def test_synchronized_oi_refuses_contradictory_contribution_evidence(mutation: str) -> None:
    value = two_contract_synchronization()
    endpoint = value.synchronization.end
    first, second = endpoint.contributions
    if mutation == "quantity":
        first = first.model_copy(update={"oi_native": Decimal(1)})
    elif mutation == "clock":
        assert endpoint.at.value is not None
        first = first.model_copy(
            update={
                "observed_at": Timestamp(
                    value=iso(parse_iso_time(endpoint.at.value) - timedelta(seconds=3)), precision="EXACT"
                )
            }
        )
    elif mutation == "membership":
        first = first.model_copy(update={"instrument_id": "other-contract"})
    elif mutation == "venue":
        first = first.model_copy(update={"venue": "other-venue"})
    else:
        first = first.model_copy(update={"oi_usd": Decimal(0)})
        second = second.model_copy(update={"oi_usd": Decimal(0)})
    changed = value.model_copy(
        update={
            "synchronization": value.synchronization.model_copy(
                update={"end": endpoint.model_copy(update={"contributions": (first, second)})}
            )
        }
    )
    with pytest.raises(ValidationError):
        ConditionSynchronizedOpenInterest.model_validate(changed)
    assert value.values.oi_native_end == Decimal(0)
    assert value.synchronization.end.usd_total() is None


def test_requested_oi_freshness_uses_consumed_sample_instead_of_latest_venue_point() -> None:
    original = derivative_view("oi", 0)
    synchronized = original.requested.synchronized
    assert isinstance(synchronized, ConditionSynchronizedOpenInterest)
    endpoint = synchronized.synchronization.end
    assert endpoint.at.value is not None
    source_clock = Timestamp(
        value=iso(parse_iso_time(endpoint.at.value) - timedelta(seconds=1)),
        precision=None,
        precision_reason="SOURCE_PRECISION_NOT_CAPTURED",
    )
    contribution = endpoint.contributions[0].model_copy(update={"observed_at": source_clock})
    witness = synchronized.synchronization.model_copy(
        update={"tolerance_s": 1.0, "end": endpoint.model_copy(update={"contributions": (contribution,)})}
    )
    source = type(original.requested).model_validate(
        original.requested.model_copy(
            update={"synchronized": synchronized.model_copy(update={"synchronization": witness})}
        )
    )
    with pytest.raises(ValidationError, match="another financial observation clock"):
        ConditionRequestedDerivativeView.model_validate(original.model_copy(update={"requested": source}))
    proof = original.proofs[0]
    assert isinstance(proof.freshness, AssessedConditionProofFreshness)
    observed = proof.freshness.observations[0]
    assert isinstance(observed, ConditionFieldObservation)
    corrected = observed.model_copy(update={"observed_at": source_clock, "age_s": 1.0})
    updated_proof = proof.model_copy(
        update={"freshness": proof.freshness.model_copy(update={"observations": (corrected,)})}
    )
    value = ConditionRequestedDerivativeView.model_validate(
        original.model_copy(update={"requested": source, "proofs": (updated_proof,)})
    )
    assert value.proofs[0].available
    wire = encode_condition_derivatives(value.requested)
    aggregate = wire["synchronized"]
    assert isinstance(aggregate, dict)
    evidence = aggregate["synchronization"]
    assert isinstance(evidence, dict)
    closing = evidence["end"]
    assert isinstance(closing, dict)
    samples = closing["contributions"]
    assert isinstance(samples, list) and len(samples) == 1
    sample = samples[0]
    assert isinstance(sample, dict)
    assert sample["observed_at"] != closing["at"]


def test_synchronized_source_inside_tolerance_still_cannot_cross_request_cutoff() -> None:
    view = derivative_view("oi", 0)
    synchronized = view.requested.synchronized
    assert isinstance(synchronized, ConditionSynchronizedOpenInterest)
    witness = synchronized.synchronization
    endpoint = witness.end
    assert endpoint.at.value is not None
    future = Timestamp(value=iso(parse_iso_time(endpoint.at.value) + timedelta(seconds=1)), precision="EXACT")
    contribution = endpoint.contributions[0].model_copy(update={"observed_at": future})
    endpoint = endpoint.model_copy(update={"contributions": (contribution,)})
    witness = witness.model_copy(update={"tolerance_s": 1.0, "end": endpoint})
    aggregate = ConditionSynchronizedOpenInterest.model_validate(
        synchronized.model_copy(update={"synchronization": witness})
    )
    with pytest.raises(ValidationError, match="after the actual cutoff"):
        type(view.requested).model_validate(view.requested.model_copy(update={"synchronized": aggregate}))


def test_partial_usd_evidence_never_becomes_a_known_aggregate_zero() -> None:
    aggregate = two_contract_synchronization()
    endpoint = aggregate.synchronization.end
    first, second = endpoint.contributions
    first = first.model_copy(update={"oi_usd": Decimal(0)})
    updated = aggregate.model_copy(
        update={
            "synchronization": aggregate.synchronization.model_copy(
                update={"end": endpoint.model_copy(update={"contributions": (first, second)})}
            )
        }
    )
    accepted = ConditionSynchronizedOpenInterest.model_validate(updated)
    assert accepted.synchronization.end.contributions[0].oi_usd == Decimal(0)
    assert accepted.synchronization.end.contributions[1].oi_usd is None
    assert accepted.values.oi_usd_end is None
