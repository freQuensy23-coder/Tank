"""Unavailable regressions cannot manufacture adjusted returns or review direction."""

from datetime import UTC, datetime, timedelta

import pytest

from tank.analysis.truth import beta


def test_insufficient_paired_history_has_no_numerical_beta() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    observed = {start + timedelta(hours=i): float(i) for i in range(5)}
    value, reason = beta(observed, observed)
    assert value is None
    assert reason == "INSUFFICIENT_HISTORY"


def test_zero_variance_benchmark_has_no_numerical_beta() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    asset = {start + timedelta(hours=i): i / 1000 for i in range(168)}
    benchmark = {at: 0.0 for at in asset}
    value, reason = beta(asset, benchmark)
    assert value is None
    assert reason == "NOT_ESTIMABLE"


def test_estimable_beta_preserves_the_actual_blume_shrinkage() -> None:
    start = datetime(2026, 10, 1, tzinfo=UTC)
    benchmark = {start + timedelta(hours=i): i / 1000 for i in range(168)}
    asset = {at: value * 2 for at, value in benchmark.items()}
    value, reason = beta(asset, benchmark)
    assert value == pytest.approx(5 / 3)
    assert reason is None
