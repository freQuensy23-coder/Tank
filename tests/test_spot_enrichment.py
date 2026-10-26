"""Spot result phases do not manufacture an uncomputed primary size profile."""

from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_condition_graph_codec import EvidenceLookup, condition_wire_clocks, projection
from test_condition_graph_contracts import AT, windowed_spot_graph

from tank.analysis.condition_graph_codec import encode_scoped_condition_spot
from tank.analysis.condition_graph_contracts import (
    ConditionCohortComparison,
    ConditionInterval,
    ConditionWindow,
    ObservedConditionCohort,
    ObservedConditionCohortComparison,
    ObservedConditionComposite,
    ObservedConditionSizeProfile,
    UnavailableConditionCohortComparison,
    UnavailableConditionSizeProfile,
    UnrequestedConditionSizeProfile,
    WindowedConditionSpot,
)
from tank.analysis.spot_enrichment import (
    CohortEnrichedConditionSpot,
    ConditionEnrichedSpotGraph,
    ConditionSpotCohortGraph,
    finish_spot_enrichment,
)


def cohort_phase() -> ConditionSpotCohortGraph:
    base = windowed_spot_graph()
    composite = base.composite_max
    assert isinstance(composite, ObservedConditionComposite)
    common = ObservedConditionCohort(
        status="OBSERVED",
        cvd=composite.cvd,
        volume=composite.volume,
        buy_share=composite.buy_share,
        bs_ratio=composite.bs_ratio,
        venues=composite.venues,
        sign=composite.sign,
        null_reasons=dict(composite.null_reasons),
        cohort=composite.venues,
        share_of_max=1.0,
        share_majority=True,
        velocity=None,
        velocity_reason="INCOMPLETE",
    )
    seconds: dict[ConditionWindow, float] = {
        "W15M": 900.0,
        "W1H": 3600.0,
        "W4H": 14400.0,
        "W24H": 86400.0,
        "EVENT": 14400.0,
    }
    spots: dict[ConditionWindow, CohortEnrichedConditionSpot] = {}
    for name, duration in seconds.items():
        window = WindowedConditionSpot.model_validate(
            base.model_copy(
                update={
                    "interval": ConditionInterval(
                        window=name, start=AT - timedelta(seconds=duration), cutoff=AT
                    ),
                    "composite_cohort": common,
                    "cohort_consistent": {other: True for other in seconds if other != name},
                }
            )
        )
        comparisons: dict[ConditionWindow, ConditionCohortComparison] = {
            other: ObservedConditionCohortComparison(
                status="OBSERVED",
                cohort=composite.venues,
                current=composite,
                other=composite,
                share_of_max=1.0,
                share_majority=True,
            )
            for other in seconds
            if other != name
        }
        spots[name] = CohortEnrichedConditionSpot.from_windowed(
            window, comparisons=comparisons, role="PRIMARY" if name == "W4H" else "OTHER_WINDOW"
        )
    return ConditionSpotCohortGraph(spots=spots, durations_s=seconds, primary="W4H")


def test_cohort_phase_retains_unrequested_primary_size_until_explicit_completion() -> None:
    phase = cohort_phase()
    primary = phase.spots[phase.primary]
    assert primary.size_profile.status == "NOT_REQUESTED"
    wire = encode_scoped_condition_spot(primary, EvidenceLookup(projection()), clocks=condition_wire_clocks())
    assert wire["enrichment_phase"] == "COHORT_COMPARISONS"
    assert wire["size_profile_status"] == "NOT_REQUESTED" and wire["size_profile"] is None
    assert "size_profile_reason" not in wire
    complete = finish_spot_enrichment(
        phase,
        primary_size_profile=UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="NOT_SUPPORTED"),
    )
    assert complete.spots[complete.primary].size_profile.status == "UNAVAILABLE"
    assert all(
        spot.size_profile.status == "NOT_REQUESTED"
        for name, spot in complete.spots.items()
        if name != complete.primary
    )
    full_wire = encode_scoped_condition_spot(
        complete.spots[complete.primary], EvidenceLookup(projection()), clocks=condition_wire_clocks()
    )
    assert full_wire["enrichment_phase"] == "FULL_ENRICHMENT"
    assert full_wire["size_profile_reason"] == "NOT_SUPPORTED"
    assert phase.spots[phase.primary].size_profile.status == "NOT_REQUESTED"


def test_completed_phase_cannot_admit_unrequested_primary_size_or_borrowed_profile_venue() -> None:
    phase = cohort_phase()
    complete = finish_spot_enrichment(
        phase, primary_size_profile=UnavailableConditionSizeProfile(status="UNAVAILABLE", reason="INCOMPLETE")
    )
    primary = complete.spots[complete.primary]
    with pytest.raises(ValidationError, match="unavailable, not unrequested"):
        ConditionEnrichedSpotGraph.model_validate(
            complete.model_copy(
                update={
                    "spots": {
                        **complete.spots,
                        complete.primary: primary.model_copy(
                            update={"size_profile": UnrequestedConditionSizeProfile(status="NOT_REQUESTED")}
                        ),
                    }
                }
            )
        )
    profile = ObservedConditionSizeProfile(
        status="OBSERVED",
        large_ticket_buy_share=1.0,
        large_ticket_sell_share=0.0,
        large_ticket_buy_majority=True,
        large_tickets_minority=True,
        large_ticket_buy_notional=Decimal(10),
        large_ticket_sell_notional=Decimal(0),
        sell_share_rising=None,
        compatible_venues=("OUTSIDE",),
        null_reasons={"sell_share_rising": "INCOMPLETE"},
    )
    with pytest.raises(ValidationError, match="unobserved venue"):
        finish_spot_enrichment(phase, primary_size_profile=profile)


def test_pairwise_phase_refuses_a_cherry_picked_intersection_or_false_native_total() -> None:
    phase = cohort_phase()
    current = phase.spots["W4H"]
    minority = UnavailableConditionCohortComparison(
        status="UNAVAILABLE",
        cohort=("A",),
        share_of_max=None,
        share_majority=None,
        reason="NOT_COHORT_CONSISTENT",
    )
    changed = current.model_copy(
        update={
            "cohort_comparisons": {**current.cohort_comparisons, "W1H": minority},
            "cohort_consistent": {**current.cohort_consistent, "W1H": False},
        }
    )
    with pytest.raises(ValidationError, match="complete-venue intersection"):
        ConditionSpotCohortGraph.model_validate(
            phase.model_copy(update={"spots": {**phase.spots, "W4H": changed}})
        )
    comparison = current.cohort_comparisons["W1H"]
    assert isinstance(comparison, ObservedConditionCohortComparison)
    wrong = comparison.model_copy(
        update={"other": comparison.other.model_copy(update={"volume": Decimal(31)})}
    )
    changed = current.model_copy(update={"cohort_comparisons": {**current.cohort_comparisons, "W1H": wrong}})
    with pytest.raises(ValidationError, match="exact native totals"):
        ConditionSpotCohortGraph.model_validate(
            phase.model_copy(update={"spots": {**phase.spots, "W4H": changed}})
        )


def test_pairwise_phase_retains_exact_named_windows_and_primary_role() -> None:
    phase = cohort_phase()
    with pytest.raises(ValidationError, match="five named"):
        ConditionSpotCohortGraph.model_validate(
            phase.model_copy(update={"spots": {"W4H": phase.spots["W4H"]}})
        )
    with pytest.raises(ValidationError, match="actual primary role"):
        ConditionSpotCohortGraph.model_validate(phase.model_copy(update={"primary": "W1H"}))
    with pytest.raises(ValidationError, match="actual interval durations"):
        ConditionSpotCohortGraph.model_validate(
            phase.model_copy(update={"durations_s": {**phase.durations_s, "W1H": 7200.0}})
        )


def test_requested_spot_retains_actual_coverage_clock_and_computed_enrichment() -> None:
    from tank.analysis.condition_graph_codec import encode_requested_condition_spot
    from tank.analysis.spot_enrichment import ConditionRequestedSpotView

    source = windowed_spot_graph()
    enriched = cohort_phase().spots["W1H"]
    lookup = EvidenceLookup(projection())
    view = ConditionRequestedSpotView(requested=source, state=enriched)
    wire = encode_requested_condition_spot(view, lookup, clocks=condition_wire_clocks())
    consistency = wire["cohort_consistent"]
    assert isinstance(consistency, dict)
    assert consistency["W4H"] is True
    assert wire["observed_at"] == "2026-10-17T12:00:00Z"
    assert wire["observed_at_prec"] == "BUCKET(60s)"
    assert wire["observed_at_bucket_anchor"] == "END"
    assert wire["enrichment_phase"] == "COHORT_COMPARISONS"
    assert lookup.calls == 0


def test_requested_spot_does_not_fill_unknown_coverage_precision_from_interval_math() -> None:
    from tank.analysis.condition_graph_codec import encode_requested_condition_spot
    from tank.analysis.spot_enrichment import ConditionRequestedSpotView
    from tank.time_precision import Timestamp

    source = windowed_spot_graph()
    observed = Timestamp(value=source.coverage_through.value, precision=None, precision_reason="NOT_CAPTURED")
    source = WindowedConditionSpot.model_validate(source.model_copy(update={"coverage_through": observed}))
    wire = encode_requested_condition_spot(
        ConditionRequestedSpotView(requested=source, state=None),
        EvidenceLookup(projection()),
        clocks=condition_wire_clocks(),
    )
    assert wire["observed_at_prec"] is None
    assert wire["observed_at_prec_reason"] == "NOT_CAPTURED"
    with pytest.raises(ValidationError, match="same actual subject/window/source clock"):
        ConditionRequestedSpotView(requested=source, state=cohort_phase().spots["W1H"])


def test_requested_spot_refuses_another_window_or_changed_coverage_before_lookup() -> None:
    from tank.analysis.spot_enrichment import ConditionRequestedSpotView

    source = windowed_spot_graph()
    with pytest.raises(ValidationError, match="same actual subject/window/source clock"):
        ConditionRequestedSpotView(requested=source, state=cohort_phase().spots["W4H"])
    state = cohort_phase().spots["W1H"]
    coverage = state.coverage.model_copy(update={"covered_share_est": 0.75})
    state = type(state).model_validate(state.model_copy(update={"coverage": coverage}))
    with pytest.raises(ValidationError, match="selected spot financial or coverage facts"):
        ConditionRequestedSpotView(requested=source, state=state)
