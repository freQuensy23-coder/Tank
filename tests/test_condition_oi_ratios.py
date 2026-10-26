"""Exact OI endpoints remain usable when a display ratio cannot be represented."""

from decimal import MAX_EMAX, Decimal
from fractions import Fraction

import pytest
from pydantic import ValidationError
from test_condition_graph_contracts import observed_oi_without_usd

from tank.analysis.condition_graph_codec import encode_condition_open_interest
from tank.analysis.condition_graph_contracts import ObservedConditionOpenInterest
from tank.analysis.oi_ratio import (
    NativeOiChange,
    OiEndpoints,
    OiFractionNotEstimable,
    exact_oi_endpoint_fraction,
    native_oi_change,
    usd_oi_change,
)
from tank.analysis.positioning import (
    MissingOIDecomposition,
    decomposition,
    oi_class,
    oi_class_from_comparisons,
)
from tank.analysis.statistics_contracts import FloatBounds, FloatRange


def zero_range() -> FloatRange:
    return FloatRange(bounds=FloatBounds(lower=0.0, upper=0.0), reason=None)


def tiny_increase() -> tuple[Decimal, Decimal]:
    return Decimal("1e200"), Decimal((0, (1, *([0] * 399), 1), -200))


def test_underflow_display_preserves_exact_positive_change_and_baseline_predicate() -> None:
    start, end = tiny_increase()
    value = native_oi_change(start, end, normal_range=zero_range())
    assert value.value is None and value.reason == "NOT_ESTIMABLE"
    assert value.position == "ABOVE" and value.beyond is True
    assert value.endpoints.start == start and value.endpoints.end == end
    with pytest.raises(ValidationError, match="exact endpoint change"):
        NativeOiChange.model_validate(value.model_copy(update={"value": 0.0, "reason": None}))


def test_representable_display_does_not_replace_an_exact_endpoint_baseline_comparison() -> None:
    rounded = 1 / 3
    value = native_oi_change(
        Decimal(3),
        Decimal(4),
        normal_range=FloatRange(bounds=FloatBounds(lower=rounded, upper=rounded), reason=None),
    )
    assert value.value == rounded and value.reason is None
    assert value.position == "ABOVE" and value.beyond is True


def test_extreme_exponents_do_not_expand_unbounded_powers_or_invent_neutral_comparisons() -> None:
    large = Decimal((0, (1,), MAX_EMAX))
    tiny = Decimal((0, (1,), -MAX_EMAX))
    positive = native_oi_change(tiny, large, normal_range=zero_range())
    assert positive.value is None and positive.reason == "NOT_ESTIMABLE"
    assert positive.position == "ABOVE" and positive.beyond is True
    negative = native_oi_change(
        large, tiny, normal_range=FloatRange(bounds=FloatBounds(lower=-1.0, upper=-1.0), reason=None)
    )
    assert negative.value == -1.0 and negative.reason is None
    assert negative.position == "ABOVE" and negative.beyond is True


def test_zero_baseline_and_missing_usd_are_distinct_from_unrepresentable_known_usd() -> None:
    zero = native_oi_change(Decimal(0), Decimal(3), normal_range=zero_range())
    assert zero.value is None and zero.reason == "ZERO_BASELINE"
    assert zero.position is None and zero.beyond is None
    missing = usd_oi_change(None, Decimal(0), normal_range=zero_range())
    assert missing.reason == "INCOMPLETE" and missing.endpoints.end == Decimal(0)
    known_zero = usd_oi_change(Decimal(0), Decimal(0), normal_range=zero_range())
    assert known_zero.reason == "ZERO_BASELINE"
    start, end = tiny_increase()
    known = usd_oi_change(start, end, normal_range=zero_range())
    assert known.reason == "NOT_ESTIMABLE" and known.beyond is True


def test_observed_oi_binds_both_ratios_to_endpoints_and_keeps_known_classification() -> None:
    base = observed_oi_without_usd()
    start, end = tiny_increase()
    usd_start = Decimal("1e400")
    usd_end = Decimal((0, (1, *([0] * 399), 1), 0))
    dec = decomposition(start, end, Decimal("1e200"), Decimal("1e200"))
    value = ObservedConditionOpenInterest.model_validate(
        base.model_copy(
            update={
                "oi_native_start": start,
                "oi_native_end": end,
                "oi_native_chg_pct": None,
                "oi_native_chg_pct_reason": "NOT_ESTIMABLE",
                "oi_usd_start": usd_start,
                "oi_usd_end": usd_end,
                "oi_usd_chg_pct": None,
                "oi_usd_chg_pct_reason": "NOT_ESTIMABLE",
                "oi_usd_reason": None,
                "oi_usd_detail": None,
                "decomposition": dec,
                "native_normal_range": zero_range(),
                "usd_normal_range": zero_range(),
                "native_oi_vs_normal": "ABOVE",
                "native_beyond": True,
                "usd_beyond": True,
                "oi_class": "REAL_NATIVE",
                "classification_ready": True,
            }
        )
    )
    wire = encode_condition_open_interest(value)
    assert wire["oi_native_chg_pct"] is None and wire["oi_native_chg_pct_reason"] == "NOT_ESTIMABLE"
    assert wire["oi_usd_chg_pct"] is None and wire["oi_usd_chg_pct_reason"] == "NOT_ESTIMABLE"
    assert wire["oi_usd_reason"] is None and wire["oi_usd_detail"] is None
    assert wire["native_beyond"] is True and wire["oi_class"] == "REAL_NATIVE"
    assert wire["oi_native_end"] == str(end) and wire["oi_usd_end"] == str(usd_end)
    with pytest.raises(ValidationError, match="Native OI display"):
        ObservedConditionOpenInterest.model_validate(
            value.model_copy(update={"oi_native_chg_pct": 0.0, "oi_native_chg_pct_reason": None})
        )
    with pytest.raises(ValidationError, match="missing-conversion gap"):
        ObservedConditionOpenInterest.model_validate(
            value.model_copy(
                update={
                    "oi_usd_reason": "INCOMPLETE",
                    "oi_usd_detail": "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE",
                }
            )
        )


def test_observed_native_zero_start_retains_endpoints_and_an_explicit_undefined_ratio() -> None:
    value = ObservedConditionOpenInterest.model_validate(
        observed_oi_without_usd().model_copy(
            update={
                "oi_native_start": Decimal(0),
                "oi_native_end": Decimal(3),
                "oi_native_chg_pct": None,
                "oi_native_chg_pct_reason": "ZERO_BASELINE",
                "native_oi_vs_normal": None,
                "native_beyond": None,
            }
        )
    )
    assert value.oi_native_start == Decimal(0) and value.oi_native_end == Decimal(3)
    assert value.oi_native_chg_pct_reason == "ZERO_BASELINE"
    assert value.oi_class is None and value.classification_ready is False


def test_invalid_reported_usd_preserves_native_endpoints_and_its_distinct_source_cause() -> None:
    base = observed_oi_without_usd()
    value = ObservedConditionOpenInterest.model_validate(
        base.model_copy(
            update={
                "oi_usd_start": None,
                "oi_usd_end": Decimal(0),
                "oi_usd_chg_pct_reason": "INCOMPLETE",
                "oi_usd_reason": "NOT_ESTIMABLE",
                "oi_usd_detail": "REPORTED_USD_INVALID",
                "decomposition": MissingOIDecomposition(detail="REPORTED_USD_INVALID"),
            }
        )
    )
    wire = encode_condition_open_interest(value)
    assert wire["oi_native_start"] == "10" and wire["oi_native_end"] == "0"
    assert wire["oi_native_chg_pct"] == -1.0 and wire["native_beyond"] is True
    assert wire["oi_usd_start"] is None and wire["oi_usd_end"] == "0"
    assert wire["oi_usd_reason"] == "NOT_ESTIMABLE"
    assert wire["oi_usd_detail"] == "REPORTED_USD_INVALID"
    assert wire["oi_usd_chg_pct_reason"] == "INCOMPLETE"
    assert isinstance(value.decomposition, MissingOIDecomposition)
    assert value.decomposition.detail == "REPORTED_USD_INVALID"
    assert value.classification_ready is False
    with pytest.raises(ValidationError, match="explicit qualification cause"):
        ObservedConditionOpenInterest.model_validate(value.model_copy(update={"oi_usd_reason": "INCOMPLETE"}))
    with pytest.raises(ValidationError, match="explicit qualification cause"):
        ObservedConditionOpenInterest.model_validate(
            value.model_copy(update={"oi_usd_detail": "SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE"})
        )
    with pytest.raises(ValidationError, match="missing-conversion gap"):
        ObservedConditionOpenInterest.model_validate(
            base.model_copy(
                update={"oi_usd_reason": "NOT_ESTIMABLE", "oi_usd_detail": "REPORTED_USD_INVALID"}
            )
        )


def test_absent_recorded_usd_does_not_claim_an_unconsumed_mark_or_fx_source() -> None:
    value = ObservedConditionOpenInterest.model_validate(
        observed_oi_without_usd().model_copy(
            update={
                "oi_usd_start": None,
                "oi_usd_chg_pct_reason": "INCOMPLETE",
                "oi_usd_reason": "INCOMPLETE",
                "oi_usd_detail": "RECORDED_USD_NOT_CAPTURED",
                "decomposition": MissingOIDecomposition(detail="RECORDED_USD_NOT_CAPTURED"),
            }
        )
    )
    wire = encode_condition_open_interest(value)
    assert wire["oi_native_start"] == "10" and wire["oi_native_end"] == "0"
    assert wire["oi_native_chg_pct"] == -1.0 and wire["native_beyond"] is True
    assert wire["oi_usd_start"] is None and wire["oi_usd_end"] == "0"
    assert wire["oi_usd_reason"] == "INCOMPLETE" and wire["oi_usd_detail"] == "RECORDED_USD_NOT_CAPTURED"
    assert isinstance(value.decomposition, MissingOIDecomposition)
    assert value.decomposition.detail == "RECORDED_USD_NOT_CAPTURED"
    with pytest.raises(ValidationError, match="explicit qualification cause"):
        ObservedConditionOpenInterest.model_validate(
            value.model_copy(update={"oi_usd_reason": "NOT_ESTIMABLE"})
        )


def test_classification_helper_retains_existing_api_and_three_valued_predicates() -> None:
    dec = decomposition(Decimal(10), Decimal(20), Decimal(2), Decimal(2))
    assert oi_class(1.0, 1.0, dec, [-0.1, 0.1], [-0.1, 0.1]) == "REAL_NATIVE"
    assert oi_class_from_comparisons(True, True, dec) == "REAL_NATIVE"
    assert oi_class_from_comparisons(None, True, dec) is None
    assert oi_class_from_comparisons(False, None, dec) is None
    assert oi_class_from_comparisons(False, False, MissingOIDecomposition()) == "MIXED"
    assert oi_class_from_comparisons(False, True, MissingOIDecomposition()) == "PRICE_MARKUP"


def test_selected_ratio_failure_is_not_relabelled_missing_market_data() -> None:
    from test_condition_derivative_selection import derivative_view
    from test_condition_graph_contracts import synchronized_oi_fixture
    from test_condition_state_contracts import selective_close

    from tank.analysis.condition_graph_codec import encode_requested_condition_derivatives
    from tank.analysis.condition_graph_contracts import (
        ConditionRequestedDerivativeView,
        ConditionUnavailableRequirement,
        SelectiveConditionValues,
        UnrequestedConditionFacet,
        condition_derivative_failure,
    )

    view = derivative_view("oi", 0)
    venue = view.requested.per_venue[0]
    assert isinstance(venue.open_interest, ObservedConditionOpenInterest)
    start, end = tiny_increase()
    point = ObservedConditionOpenInterest.model_validate(
        observed_oi_without_usd().model_copy(
            update={
                "oi_native_start": start,
                "oi_native_end": end,
                "oi_native_chg_pct": None,
                "oi_native_chg_pct_reason": "NOT_ESTIMABLE",
                "start_observed_at": venue.open_interest.start_observed_at,
                "end_observed_at": venue.open_interest.end_observed_at,
                "native_oi_vs_normal": "WITHIN",
                "native_beyond": False,
            }
        )
    )
    source = type(view.requested).model_validate(
        view.requested.model_copy(
            update={
                "per_venue": (type(venue).model_validate(venue.model_copy(update={"open_interest": point})),),
                "synchronized": synchronized_oi_fixture(
                    point, venue=venue.venue, instrument_id=venue.instrument_id
                ),
            }
        )
    )
    assert condition_derivative_failure(source, "deriv.native_oi_chg_pct") == "NOT_ESTIMABLE"
    assert condition_derivative_failure(source, "deriv.native_oi_vs_normal") is None
    proof = type(view.proofs[0]).model_validate(
        view.proofs[0].model_copy(update={"available": False, "reason": "NOT_ESTIMABLE"})
    )
    value = ConditionRequestedDerivativeView(requested=source, state=None, proofs=(proof,))
    wire = encode_requested_condition_derivatives(value)
    synchronized = wire["synchronized"]
    assert isinstance(synchronized, dict)
    assert synchronized["oi_native_chg_pct"] is None
    assert synchronized["oi_native_chg_pct_reason"] == "NOT_ESTIMABLE"
    evaluation = wire["requested_evaluation"]
    assert isinstance(evaluation, list) and isinstance(evaluation[0], dict)
    assert evaluation[0]["available"] is False and evaluation[0]["reason"] == "NOT_ESTIMABLE"
    base = selective_close()
    root = SelectiveConditionValues.model_validate(
        base.model_copy(
            update={
                "assembled_at": proof.assembled_at,
                "requested": (proof.requirement,),
                "derivatives": {"W1H": source},
                "close": UnrequestedConditionFacet(status="NOT_REQUESTED"),
                "proofs": (proof,),
                "unavailable_metrics": (
                    ConditionUnavailableRequirement(requirement=proof.requirement, reason="NOT_ESTIMABLE"),
                ),
            }
        )
    )
    assert root.unavailable_metrics[0].reason == "NOT_ESTIMABLE"
    bad = type(proof).model_validate(proof.model_copy(update={"reason": "INCOMPLETE"}))
    with pytest.raises(ValidationError, match="actual source or ratio cause"):
        ConditionRequestedDerivativeView(requested=source, state=None, proofs=(bad,))
    with pytest.raises(ValidationError, match="actual evaluated proof cause"):
        SelectiveConditionValues.model_validate(
            root.model_copy(
                update={
                    "unavailable_metrics": (
                        ConditionUnavailableRequirement(requirement=proof.requirement, reason="INCOMPLETE"),
                    ),
                }
            )
        )


def test_integer_marker_cannot_be_a_known_baseline_predicate() -> None:
    invalid = observed_oi_without_usd().model_copy(update={"native_beyond": 1})
    with pytest.raises(ValueError, match="require booleans"):
        oi_class_from_comparisons(invalid.native_beyond, None, MissingOIDecomposition())


def test_unestimable_population_range_preserves_current_native_endpoints_and_known_change() -> None:
    unavailable = FloatRange(bounds=None, reason="NOT_ESTIMABLE")
    change = native_oi_change(Decimal(10), Decimal(12), normal_range=unavailable)
    assert change.value == 0.2 and change.reason is None
    assert change.position is None and change.beyond is None
    assert change.endpoints.start == Decimal(10) and change.endpoints.end == Decimal(12)
    base = observed_oi_without_usd()
    value = ObservedConditionOpenInterest.model_validate(
        base.model_copy(
            update={
                "native_normal_range": unavailable,
                "native_oi_vs_normal": None,
                "native_beyond": None,
                "oi_class": None,
                "classification_ready": False,
            }
        )
    )
    wire = encode_condition_open_interest(value)
    assert value.oi_native_start == base.oi_native_start and value.oi_native_end == base.oi_native_end
    assert value.oi_native_chg_pct == base.oi_native_chg_pct and value.oi_native_chg_pct_reason is None
    assert wire["native_normal_range"] is None and wire["native_normal_range_reason"] == "NOT_ESTIMABLE"
    assert wire["native_beyond"] is None and wire["oi_class"] is None


def test_public_endpoint_fraction_normalizes_huge_shared_scale_and_actual_zero_end() -> None:
    one = Decimal((0, (1,), 1_000_000_000))
    two = Decimal((0, (2,), 1_000_000_000))
    assert exact_oi_endpoint_fraction(OiEndpoints(start=one, end=two)) == Fraction(1)
    assert exact_oi_endpoint_fraction(OiEndpoints(start=two, end=one)) == Fraction(-1, 2)
    assert exact_oi_endpoint_fraction(OiEndpoints(start=one, end=one)) == Fraction(0)
    assert exact_oi_endpoint_fraction(OiEndpoints(start=one, end=Decimal(0))) == Fraction(-1)


def test_public_endpoint_fraction_refuses_expansion_before_unrepresentable_gap_allocation() -> None:
    huge = Decimal((0, (1,), 1_000_000_000))
    for start, end in ((Decimal(1), huge), (huge, Decimal(1))):
        points = OiEndpoints(start=start, end=end)
        with pytest.raises(OiFractionNotEstimable, match="magnitude gap"):
            exact_oi_endpoint_fraction(points)
        assert points.start == start and points.end == end
    with pytest.raises(ValueError, match="positive baseline"):
        exact_oi_endpoint_fraction(OiEndpoints(start=Decimal(0), end=Decimal(0)))
    with pytest.raises(ValidationError):
        exact_oi_endpoint_fraction(OiEndpoints.model_construct(start=Decimal("NaN"), end=Decimal(0)))
