from datetime import UTC, datetime, timedelta

import pytest

from tank.analysis.price_reference import (
    ObservedReferencePrice,
    PriceCadence,
    ReferencePriceSeries,
    UnavailableReferencePrice,
)


@pytest.mark.parametrize("seconds", [60.0, 3600.0])
def test_declared_minute_and_hour_cadence_bounds_reference_age(seconds: float) -> None:
    at = datetime(2026, 10, 1, tzinfo=UTC)
    series = ReferencePriceSeries({at: 100.0}, {at: PriceCadence(seconds=seconds, source="BAR_INTERVAL")})
    observed = series.at(at + timedelta(seconds=seconds))
    assert isinstance(observed, ObservedReferencePrice)
    assert observed.price == 100.0 and observed.age_s == seconds
    stale = series.at(at + timedelta(seconds=seconds, microseconds=1))
    assert isinstance(stale, UnavailableReferencePrice)
    assert stale.reason == "STALE"


def test_lone_observation_without_declared_cadence_is_unavailable() -> None:
    at = datetime(2026, 10, 1, tzinfo=UTC)
    observed = ReferencePriceSeries({at: 100.0}, {}).at(at)
    assert isinstance(observed, UnavailableReferencePrice)
    assert observed.reason == "CADENCE_NOT_CAPTURED"
    assert observed.observed_at == at and observed.age_s == 0.0
    assert observed.cadence is None


def test_large_observation_gap_cannot_expand_declared_freshness() -> None:
    at = datetime(2026, 10, 1, tzinfo=UTC)
    later = at + timedelta(hours=4)
    cadence = PriceCadence(seconds=60.0, source="BAR_INTERVAL")
    series = ReferencePriceSeries({at: 100.0, later: 110.0}, {at: cadence, later: cadence})
    returned = series.return_between(at, later + timedelta(minutes=30))
    assert returned.value is None and returned.reason == "NO_FRESH_ENDPOINTS"
    assert isinstance(returned.end, UnavailableReferencePrice)
    assert returned.end.reason == "STALE" and returned.end.age_s == 1800.0


def test_subminute_cadence_has_no_minute_floor() -> None:
    at = datetime(2026, 10, 1, tzinfo=UTC)
    observed = ReferencePriceSeries(
        {at: 100.0}, {at: PriceCadence(seconds=5.0, source="CAPTURE_METADATA")}
    ).at(at + timedelta(seconds=6))
    assert isinstance(observed, UnavailableReferencePrice)
    assert observed.reason == "STALE"


def test_measured_zero_reference_return_is_available() -> None:
    at = datetime(2026, 10, 1, tzinfo=UTC)
    end = at + timedelta(minutes=1)
    cadence = PriceCadence(seconds=60.0, source="BAR_INTERVAL")
    returned = ReferencePriceSeries({at: 100.0, end: 100.0}, {at: cadence, end: cadence}).return_between(
        at, end
    )
    assert returned.value == 0.0 and returned.reason is None
