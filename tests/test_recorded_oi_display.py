"""Dollar displays never round repeating implied marks or constrain exact classification."""

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from tank.analysis.recorded_oi_decomposition import RecordedOiEndpoints, recorded_oi_decomposition
from tank.analysis.recorded_oi_display import (
    RecordedOiDollarTerm,
    RecordedOiDollarTerms,
    recorded_oi_dollar_terms,
)


def test_terminating_dollar_terms_preserve_exact_values_under_low_decimal_precision() -> None:
    source = recorded_oi_decomposition(
        RecordedOiEndpoints(
            native_start=Decimal(100),
            native_end=Decimal("108.18"),
            usd_start=Decimal(100),
            usd_end=Decimal("115.80669"),
        )
    )
    with localcontext() as context:
        context.prec = 2
        result = recorded_oi_dollar_terms(source)
    assert result.position.value == Decimal("8.18")
    assert result.markup.value == Decimal("7.05")
    assert result.interaction.value == Decimal("0.57669")
    assert all(term.reason is None for term in (result.position, result.markup, result.interaction))


def test_repeating_dollar_terms_keep_exact_majority_and_sparse_source() -> None:
    source = recorded_oi_decomposition(
        RecordedOiEndpoints(
            native_start=Decimal(3), native_end=Decimal(4), usd_start=Decimal(1), usd_end=Decimal(1)
        )
    )
    result = recorded_oi_dollar_terms(source)
    assert result.source == source
    assert result.source.native_majority is False
    assert result.source.pos_share == 0.5
    assert result.markup.value == Decimal("-0.25")
    assert result.markup.reason is None
    for term in (result.position, result.interaction):
        assert term.value is None
        assert term.reason == "NON_TERMINATING_DECIMAL"


def test_display_materialization_limit_does_not_remove_exact_majority() -> None:
    source = recorded_oi_decomposition(
        RecordedOiEndpoints(
            native_start=Decimal(1),
            native_end=Decimal("1E10000"),
            usd_start=Decimal(1),
            usd_end=Decimal(1),
        )
    )
    result = recorded_oi_dollar_terms(source)
    assert result.source.native_majority is False
    assert result.position.value is None
    assert result.position.reason == "DISPLAY_NOT_MATERIALIZED"


def test_zero_native_endpoint_cannot_invent_implied_dollar_terms() -> None:
    result = recorded_oi_dollar_terms(
        recorded_oi_decomposition(
            RecordedOiEndpoints(
                native_start=Decimal(1), native_end=Decimal(0), usd_start=Decimal(1), usd_end=Decimal(0)
            )
        )
    )
    assert result.source.native_majority is None
    for term in (result.position, result.markup, result.interaction):
        assert term.value is None and term.reason == "NATIVE_ENDPOINT_ZERO"


def test_public_dollar_display_admission_rejects_copied_incorrect_value() -> None:
    result = recorded_oi_dollar_terms(
        recorded_oi_decomposition(
            RecordedOiEndpoints(
                native_start=Decimal(1), native_end=Decimal(2), usd_start=Decimal(1), usd_end=Decimal(2)
            )
        )
    )
    copied = result.model_copy(update={"position": RecordedOiDollarTerm(value=Decimal(99), reason=None)})
    with pytest.raises(ValidationError, match="Dollar displays must equal"):
        RecordedOiDollarTerms.model_validate(copied)
