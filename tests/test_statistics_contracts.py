from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.statistics_contracts import (
    BaselineCache,
    BitcoinContextHistory,
    DecimalBounds,
    DecimalRange,
    EstimatedBeta,
    FloatBounds,
    FloatRange,
    HistoryDiagnostics,
    NativeVolumeBoundary,
    NativeVolumeIndex,
    RelativeHistory,
    StatisticalPopulation,
    UnavailableBeta,
)


def test_population_is_compact_immutable_ordered_and_sign_reversible() -> None:
    source = [3.0, -2.0, 1.0]
    population = StatisticalPopulation(source)
    source[0] = 99.0
    assert tuple(population) == (3.0, -2.0, 1.0)
    assert population[:] == (3.0, -2.0, 1.0)
    assert [population.ordered_value(i) for i in range(3)] == [-2.0, 1.0, 3.0]
    negative = population.scaled(-1)
    assert tuple(negative) == (-3.0, 2.0, -1.0)
    assert [negative.ordered_value(i) for i in range(3)] == [-3.0, -1.0, 2.0]
    assert negative.scaled(-1) is population
    assert population.scaled(1) is population


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True])
def test_population_refuses_nonfinite_and_boolean_samples(value: float) -> None:
    with pytest.raises(ValueError, match="finite real"):
        StatisticalPopulation([value])


def test_population_model_boundary_revalidates_without_exposing_mutable_arrays() -> None:
    source = StatisticalPopulation([1.0, 2.0])
    history = BitcoinContextHistory(returns=source, volatility=StatisticalPopulation([]))
    assert history.returns is not source
    assert tuple(history.returns) == (1.0, 2.0)
    assert not history.volatility
    with pytest.raises(ValidationError):
        BitcoinContextHistory.model_validate({"returns": [1.0], "volatility": []})


def test_ranges_distinguish_unavailable_float_statistics_and_exact_decimal_constants() -> None:
    exact = Decimal("0.0000000000000000000000000000000000001")
    constant = DecimalRange(bounds=DecimalBounds(lower=exact, upper=exact), reason=None)
    assert constant.bounds is not None and constant.bounds.lower == exact
    assert FloatRange(bounds=None, reason="DEGENERATE_HISTORY").reason == "DEGENERATE_HISTORY"
    with pytest.raises(ValidationError):
        FloatRange(bounds=None, reason=None)
    with pytest.raises(ValidationError):
        FloatRange(bounds=FloatBounds(lower=0.0, upper=1.0), reason="INCOMPLETE")
    with pytest.raises(ValidationError):
        FloatBounds(lower=2.0, upper=1.0)
    with pytest.raises(ValidationError):
        DecimalBounds(lower=Decimal("NaN"), upper=Decimal(1))


def test_unavailable_beta_has_no_numeric_assumption() -> None:
    unavailable = UnavailableBeta(
        kind="UNAVAILABLE", value=None, reason="NOT_ESTIMABLE", paired_sample_count=200
    )
    assert unavailable.value is None and unavailable.reason == "NOT_ESTIMABLE"
    with pytest.raises(ValidationError):
        UnavailableBeta.model_validate(
            {"kind": "UNAVAILABLE", "value": 1.0, "reason": "NOT_ESTIMABLE", "paired_sample_count": 200}
        )


def test_empty_history_has_explicit_unmeasured_endpoints_and_matching_sample_count() -> None:
    diagnostic = HistoryDiagnostics(
        target_window_duration_s=3600.0,
        observed_duration_bounds_s=None,
        endpoint_staleness_bounds_s=None,
        exact=False,
        resolution_s=60.0,
        alignment="AS_OF_AT_EXACT_TARGET_BOUNDS",
        sample_count=0,
        latest_observed_at=None,
    )
    assert not RelativeHistory(samples=StatisticalPopulation([]), diagnostics=diagnostic).samples
    with pytest.raises(ValidationError, match="sample count"):
        RelativeHistory(samples=StatisticalPopulation([1.0]), diagnostics=diagnostic)
    with pytest.raises(ValidationError, match="Empty history"):
        HistoryDiagnostics.model_validate(diagnostic.model_copy(update={"exact": True}))


def test_volume_index_preserves_exact_money_and_epoch_zero_without_sentinels() -> None:
    at = datetime(1970, 1, 1, tzinfo=UTC)
    amount = Decimal("1000000000000000000000000000.000000000000000000000001")
    index = NativeVolumeIndex(
        bounds={at: NativeVolumeBoundary(cumulative=amount, segment=2)},
        running=amount,
        segment=2,
        last=None,
        last_open_us=0,
    )
    assert index.bounds[at].cumulative == amount
    assert index.last is None and index.last_open_us == 0
    with pytest.raises(ValidationError):
        NativeVolumeIndex.model_validate(
            index.model_copy(update={"bounds": {datetime(1970, 1, 1): index.bounds[at]}})
        )


def test_family_cache_evicts_oldest_only_and_does_not_recompute_hits() -> None:
    cache = BaselineCache[int](capacity=2)
    calls = 0

    def build() -> int:
        nonlocal calls
        calls += 1
        return 0

    assert cache.remember("first-fingerprint", build) == 0
    assert cache.remember("first-fingerprint", build) == 0
    assert calls == 1
    cache.put("second-fingerprint", 2)
    cache.put("first-fingerprint", 3)
    cache.put("third-fingerprint", 4)
    assert cache.get("first-fingerprint") is None
    assert cache.get("second-fingerprint") == 2
    assert cache.get("third-fingerprint") == 4


def test_measured_zero_beta_remains_estimated_and_finite() -> None:
    estimate = EstimatedBeta(kind="ESTIMATED", value=0.0, paired_sample_count=200)
    assert estimate.value == 0.0 and estimate.kind == "ESTIMATED"
    with pytest.raises(ValidationError):
        EstimatedBeta(kind="ESTIMATED", value=float("nan"), paired_sample_count=200)
