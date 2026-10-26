"""Exact OI endpoints remain usable when a display ratio cannot be represented."""

from decimal import MAX_EMAX, Decimal
from fractions import Fraction

import pytest
from pydantic import ValidationError
from test_condition_graph_contracts import observed_oi_without_usd

from tank.analysis.condition_graph_codec import (
    encode_condition_open_interest,
    encode_recorded_oi_decomposition,
)
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
from tank.analysis.recorded_oi_decomposition import (
    DefinedRecordedOiDecomposition,
    RecordedOiEndpoints,
    recorded_oi_decomposition,
)
from tank.analysis.statistics_contracts import FloatBounds, FloatRange


def zero_range() -> FloatRange:
    return FloatRange(bounds=FloatBounds(lower=0.0, upper=0.0), reason=None)


def recorded_point(points: RecordedOiEndpoints) -> ObservedConditionOpenInterest:
    native = native_oi_change(points.native_start, points.native_end, normal_range=zero_range())
    usd = usd_oi_change(points.usd_start, points.usd_end, normal_range=zero_range())
    exact = recorded_oi_decomposition(points)
    classification = oi_class_from_comparisons(native.beyond, usd.beyond, exact)
    return ObservedConditionOpenInterest.model_validate(
        observed_oi_without_usd().model_copy(
            update={
                "oi_native_start": points.native_start,
                "oi_native_end": points.native_end,
                "oi_native_chg_pct": native.value,
                "oi_native_chg_pct_reason": native.reason,
                "oi_usd_start": points.usd_start,
                "oi_usd_end": points.usd_end,
                "oi_usd_chg_pct": usd.value,
                "oi_usd_chg_pct_reason": usd.reason,
                "oi_usd_reason": None,
                "oi_usd_detail": None,
                "decomposition": exact,
                "native_normal_range": zero_range(),
                "usd_normal_range": zero_range(),
                "native_oi_vs_normal": native.position,
                "native_beyond": native.beyond,
                "usd_beyond": usd.beyond,
                "oi_class": classification,
                "classification_ready": classification is not None,
            }
        )
    )


def test_recorded_implied_price_boundary_is_mixed_without_rounding_the_mark() -> None:
    point = recorded_point(
        RecordedOiEndpoints(
            native_start=Decimal(3), native_end=Decimal(4), usd_start=Decimal(1), usd_end=Decimal(1)
        )
    )
    assert point.native_beyond is True and point.usd_beyond is False
    assert point.oi_class == "MIXED" and point.classification_ready
    exact = point.decomposition
    assert isinstance(exact, DefinedRecordedOiDecomposition)
    assert exact.pos_share == 0.5 and exact.native_majority is False
    wire = encode_condition_open_interest(point)
    assert wire["oi_class"] == "MIXED"
    encoded = wire["decomposition"]
    assert isinstance(encoded, dict) and encoded["basis"] == "RECORDED_USD_IMPLIED_PRICE"
    assert encoded["native_majority"] is False


def test_unmaterialized_share_does_not_block_an_exact_recorded_native_conclusion() -> None:
    point = recorded_point(
        RecordedOiEndpoints(
            native_start=Decimal(1),
            native_end=Decimal("1E1000000000"),
            usd_start=Decimal(1),
            usd_end=Decimal(3),
        )
    )
    assert point.oi_native_chg_pct is None and point.oi_native_chg_pct_reason == "NOT_ESTIMABLE"
    assert point.native_beyond is True
    assert point.oi_class == "REAL_NATIVE" and point.classification_ready
    exact = point.decomposition
    assert isinstance(exact, DefinedRecordedOiDecomposition)
    assert exact.pos_share is None and exact.pos_share_reason == "DISPLAY_NOT_MATERIALIZED"
    assert exact.native_majority is True
    encoded = encode_recorded_oi_decomposition(exact)
    assert encoded["pos_share"] is None and encoded["native_majority"] is True
    assert encoded["pos_share_reason"] == "DISPLAY_NOT_MATERIALIZED"


def test_sparse_wire_preserves_exponents_beyond_javascript_integer_precision_as_text() -> None:
    power = 10_000_000_000_000_000
    point = recorded_point(
        RecordedOiEndpoints(
            native_start=Decimal(f"1E{power}"),
            native_end=Decimal(f"2E{power}"),
            usd_start=Decimal(f"1E{power}"),
            usd_end=Decimal(f"2E{power}"),
        )
    )
    encoded = encode_condition_open_interest(point)["decomposition"]
    assert isinstance(encoded, dict)
    terms = encoded["terms"]
    assert isinstance(terms, dict) and terms["representation"] == "SPARSE_DECIMAL"
    denominator = terms["denominator"]
    assert isinstance(denominator, dict)
    assert denominator["exponent"] == str(2 * power)
    position = terms["position"]
    assert isinstance(position, dict)
    monomials = position["terms"]
    assert isinstance(monomials, list) and len(monomials) == 1
    monomial = monomials[0]
    assert isinstance(monomial, dict) and monomial["exponent"] == str(3 * power)
    assert point.oi_class == "REAL_NATIVE"


@pytest.mark.parametrize("native_start,native_end,usd_start,usd_end", [(0, 4, 1, 2), (3, 4, 0, 0)])
def test_recorded_zero_endpoint_and_zero_terms_keep_known_values_without_inventing_majority(
    native_start: int, native_end: int, usd_start: int, usd_end: int
) -> None:
    point = recorded_point(
        RecordedOiEndpoints(
            native_start=Decimal(native_start),
            native_end=Decimal(native_end),
            usd_start=Decimal(usd_start),
            usd_end=Decimal(usd_end),
        )
    )
    assert point.oi_native_start == Decimal(native_start) and point.oi_usd_start == Decimal(usd_start)
    assert point.oi_class is None and not point.classification_ready
    encoded = encode_condition_open_interest(point)["decomposition"]
    assert isinstance(encoded, dict)
    assert encoded["native_majority"] is None and encoded["pos_share"] is None
    assert encoded["native_majority_reason"] == (
        "NATIVE_ENDPOINT_ZERO" if native_start == 0 else "ZERO_ABSOLUTE_TERMS"
    )


def test_recorded_decomposition_cannot_borrow_another_endpoint_scope_or_mutated_predicate() -> None:
    point = recorded_point(
        RecordedOiEndpoints(
            native_start=Decimal(3), native_end=Decimal(4), usd_start=Decimal(1), usd_end=Decimal(1)
        )
    )
    foreign = recorded_oi_decomposition(
        RecordedOiEndpoints(
            native_start=Decimal(3), native_end=Decimal(5), usd_start=Decimal(1), usd_end=Decimal(1)
        )
    )
    with pytest.raises(ValidationError, match="bind the actual native and USD endpoints"):
        ObservedConditionOpenInterest.model_validate(point.model_copy(update={"decomposition": foreign}))
    exact = point.decomposition
    assert isinstance(exact, DefinedRecordedOiDecomposition)
    mutated = exact.model_copy(update={"native_majority": True})
    with pytest.raises(ValidationError, match="exact endpoint-bound mathematics"):
        oi_class_from_comparisons(True, False, mutated)
    with pytest.raises(ValidationError, match="exact endpoint-bound mathematics"):
        encode_recorded_oi_decomposition(mutated)


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
