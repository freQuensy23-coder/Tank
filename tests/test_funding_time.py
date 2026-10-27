"""Clock bounds distinguish paid events, sample observations and operational readings."""

import pytest
from pydantic import ValidationError

from tank.analysis.funding_time import FundingClockBounds, funding_clock_bounds, funding_receipt_visible
from tank.time_precision import Timestamp


def test_unknown_precision_keeps_reported_scalar_basis_without_exact_backfill() -> None:
    clock = Timestamp(value="2026-10-16T12:00:00Z", precision=None, precision_reason="UNREPORTED")
    result = funding_clock_bounds(clock, role="OPERATIONAL")
    assert result.clock.precision is None
    assert result.basis == "REPORTED_SCALAR"
    assert result.lower == result.upper
    with pytest.raises(ValidationError):
        FundingClockBounds.model_validate(result.model_copy(update={"basis": "DECLARED_EXACT"}))


def test_collection_clock_is_a_sample_but_not_a_paid_occurrence() -> None:
    clock = Timestamp(value="2026-10-16T12:00:00Z", precision="COLLECTION_ONLY")
    sample = funding_clock_bounds(clock, role="ESTIMATE_SAMPLE")
    paid = funding_clock_bounds(clock, role="SETTLEMENT_EVENT")
    assert sample.basis == "COLLECTION_OBSERVATION"
    assert sample.clock == paid.clock == clock
    assert paid.lower is paid.upper is None
    assert paid.reason == "PAID_TIME_NOT_CAPTURED"


@pytest.mark.parametrize("anchor", [None, "UNSPECIFIED"])
def test_unreported_bucket_orientation_has_no_invented_start_edge(anchor: str | None) -> None:
    clock = Timestamp(value="2026-10-16T12:00:00Z", precision="BUCKET(5m)", bucket_anchor=anchor)
    bounds = funding_clock_bounds(clock, role="OPERATIONAL")
    assert bounds.lower is bounds.upper is None
    assert bounds.reason == "BUCKET_ORIENTATION_NOT_CAPTURED"
    assert bounds.clock.bucket_anchor == anchor


def test_receipt_and_assembly_ranges_require_order_not_nominal_labels() -> None:
    receipt = Timestamp(value="2026-10-16T12:04:00Z", precision="BUCKET(5m)", bucket_anchor="START")
    before = Timestamp(value="2026-10-16T12:05:00Z", precision="COLLECTION_ONLY")
    after = Timestamp(value="2026-10-16T12:10:00Z", precision="COLLECTION_ONLY")
    assert funding_receipt_visible(receipt, before) is None
    assert funding_receipt_visible(receipt, after) is True
    known_receipt = Timestamp(value="2026-10-16T12:03:00Z", precision="COLLECTION_ONLY")
    uncertain_assembly = Timestamp(value="2026-10-16T12:05:00Z", precision="BUCKET(5m)", bucket_anchor="END")
    assert funding_receipt_visible(known_receipt, uncertain_assembly) is None
