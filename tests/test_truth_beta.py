"""Unavailable regressions cannot manufacture adjusted returns or review direction."""

from datetime import UTC, datetime, timedelta

import pytest

from tank.analysis.statistics_contracts import EstimatedBeta, UnavailableBeta
from tank.analysis.truth import beta


@pytest.mark.parametrize("sample_count", [1, 5, 167])
def test_insufficient_paired_history_has_no_numerical_beta(sample_count: int) -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    observed = {start + timedelta(hours=i): float(i) for i in range(sample_count)}
    result = beta(observed, observed)
    assert isinstance(result, UnavailableBeta)
    assert result.value is None
    assert result.reason == "INSUFFICIENT_HISTORY"
    assert result.paired_sample_count == sample_count


def test_zero_variance_benchmark_has_no_numerical_beta() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    asset = {start + timedelta(hours=i): i / 1000 for i in range(168)}
    benchmark = {at: 0.0 for at in asset}
    result = beta(asset, benchmark)
    assert isinstance(result, UnavailableBeta)
    assert result.value is None
    assert result.reason == "NOT_ESTIMABLE"
    assert result.paired_sample_count == 168


def test_estimable_beta_preserves_the_actual_blume_shrinkage() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    benchmark = {start + timedelta(hours=i): i / 1000 for i in range(168)}
    asset = {at: value * 2 for at, value in benchmark.items()}
    result = beta(asset, benchmark)
    assert isinstance(result, EstimatedBeta)
    assert result.value == pytest.approx(5 / 3)
    assert result.paired_sample_count == 168


def test_unpaired_observations_cannot_satisfy_the_seven_day_beta_minimum() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    asset = {start + timedelta(hours=i): i / 1000 for i in range(336)}
    benchmark = {start + timedelta(hours=i): i / 2000 for i in range(167)}
    result = beta(asset, benchmark)
    assert isinstance(result, UnavailableBeta)
    assert result.value is None and result.reason == "INSUFFICIENT_HISTORY"
    assert result.paired_sample_count == 167
