from decimal import Decimal
from fractions import Fraction

import pytest
from pydantic import ValidationError

from tank.analysis.recorded_oi_decomposition import (
    DecimalPowerTerm,
    DefinedRecordedOiDecomposition,
    RecordedOiEndpoints,
    SparseDecimalPolynomial,
    UndefinedRecordedOiDecomposition,
    recorded_oi_decomposition,
)


def endpoints(initial: str, final: str, usd0: str, usd1: str) -> RecordedOiEndpoints:
    return RecordedOiEndpoints(
        native_start=Decimal(initial),
        native_end=Decimal(final),
        usd_start=Decimal(usd0),
        usd_end=Decimal(usd1),
    )


def polynomial_value(value: SparseDecimalPolynomial) -> Fraction:
    return sum(
        (Fraction(term.coefficient) * Fraction(10) ** term.exponent for term in value.terms), Fraction(0)
    )


@pytest.mark.parametrize(
    "values",
    [
        ("3", "4", "1", "1"),
        ("3", "4", "1", "2"),
        ("100", "200", "1000", "4000"),
        ("4", "3", "2", "1"),
        ("12.5", "9.1", "0.125", "11"),
        ("1", "2", "0", "1"),
        ("1", "2", "1", "0"),
    ],
)
def test_exact_sparse_terms_and_majority_match_independent_rational_formula(
    values: tuple[str, str, str, str],
) -> None:
    source = endpoints(*values)
    result = recorded_oi_decomposition(source)
    assert isinstance(result, DefinedRecordedOiDecomposition)
    a, z, u, v = (Fraction(Decimal(value)) for value in values)
    expected = ((z - a) * u / a, a * (v / z - u / a), (z - a) * (v / z - u / a))
    denominator = (
        Fraction(result.terms.denominator.coefficient) * Fraction(10) ** result.terms.denominator.exponent
    )
    actual = tuple(
        polynomial_value(value) / denominator
        for value in (result.terms.position, result.terms.markup, result.terms.interaction)
    )
    assert actual == expected
    total = sum(abs(value) for value in expected)
    assert result.native_majority == (2 * abs(expected[0]) > total)
    assert result.pos_share == float(abs(expected[0]) / total)
    assert result.pos_share_reason is None
    assert result.basis == "RECORDED_USD_IMPLIED_PRICE"


def test_nonterminating_implied_price_cannot_turn_exact_half_into_native_majority() -> None:
    result = recorded_oi_decomposition(endpoints("3", "4", "1", "1"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert result.pos_share == 0.5
    assert result.native_majority is False
    assert result.native_majority_reason is None


@pytest.mark.parametrize(
    "values,zero",
    [(("0", "4", "1", "2"), "START"), (("3", "0", "1", "0"), "END"), (("0", "0", "0", "0"), "BOTH")],
)
def test_zero_native_endpoint_is_explicitly_undefined_without_a_mark(
    values: tuple[str, str, str, str], zero: str
) -> None:
    source = endpoints(*values)
    result = recorded_oi_decomposition(source)
    assert isinstance(result, UndefinedRecordedOiDecomposition)
    assert result.endpoints == source and result.zero_endpoint == zero
    assert result.native_majority is None and result.native_majority_reason == "NATIVE_ENDPOINT_ZERO"
    assert result.pos_share is None and result.pos_share_reason == "NATIVE_ENDPOINT_ZERO"


def test_zero_absolute_terms_have_no_share_or_majority() -> None:
    result = recorded_oi_decomposition(endpoints("3", "4", "0", "0"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert all(
        not value.terms for value in (result.terms.position, result.terms.markup, result.terms.interaction)
    )
    assert result.native_majority is None and result.native_majority_reason == "ZERO_ABSOLUTE_TERMS"
    assert result.pos_share is None and result.pos_share_reason == "ZERO_ABSOLUTE_TERMS"


@pytest.mark.parametrize("power", [400, 1_000_000_000])
def test_extreme_relative_native_magnitude_keeps_exact_half_without_expansion(power: int) -> None:
    result = recorded_oi_decomposition(endpoints("1", f"1E{power}", "1", "1"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert result.native_majority is False and result.pos_share == 0.5
    assert len(result.terms.position.terms) <= 2
    assert max(value.exponent for value in result.terms.position.terms) == 2 * power


@pytest.mark.parametrize("power", [1_000_000_000, 10_000_000_000_000_000])
def test_common_billion_exponents_keep_known_native_majority(power: int) -> None:
    result = recorded_oi_decomposition(endpoints(f"1E{power}", f"2E{power}", f"1E{power}", f"2E{power}"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert result.native_majority is True and result.pos_share == 1
    assert len(result.terms.position.terms) == 1
    assert result.terms.position.terms[0].exponent == 3 * power


def test_unmaterialized_display_does_not_erase_exact_majority() -> None:
    result = recorded_oi_decomposition(endpoints("1", "1E1000000000", "1", "3"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert result.native_majority is True
    assert result.pos_share is None and result.pos_share_reason == "DISPLAY_NOT_MATERIALIZED"


def test_positive_share_underflow_is_not_known_zero() -> None:
    result = recorded_oi_decomposition(endpoints("1", "2", "1E-400", "2"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    assert result.native_majority is False
    assert result.pos_share is None and result.pos_share_reason == "FLOAT64_UNDERFLOW"


def test_copied_endpoint_or_predicate_tampering_refuses() -> None:
    result = recorded_oi_decomposition(endpoints("3", "4", "1", "1"))
    assert isinstance(result, DefinedRecordedOiDecomposition)
    with pytest.raises(ValidationError, match="endpoint-bound"):
        DefinedRecordedOiDecomposition.model_validate(result.model_copy(update={"native_majority": True}))
    with pytest.raises(ValidationError, match="endpoint-bound"):
        DefinedRecordedOiDecomposition.model_validate(
            result.model_copy(
                update={"endpoints": result.endpoints.model_copy(update={"native_end": Decimal(5)})}
            )
        )
    with pytest.raises(ValidationError):
        recorded_oi_decomposition(result.endpoints.model_copy(update={"native_end": True}))
    with pytest.raises(ValidationError):
        DecimalPowerTerm(coefficient=Decimal(1), exponent=True)
