"""Funding timing roles preserve metadata and never authorize overlapping buckets."""

import pytest
from funding_condition_fixtures import evaluate, packet
from funding_input_fixtures import funding_record_fixture

from tank.analysis.baseline_cache import BaselineCaches
from tank.analysis.condition_funding_inputs import FundingKind


@pytest.mark.parametrize("kind", ["SETTLED", "CURRENT_ESTIMATE"])
def test_source_bucket_crossing_cutoff_cannot_authorize_funding(kind: FundingKind) -> None:
    snapshot = packet()
    snapshot.update(cutoff="2026-10-16T12:02:00Z", assembled_at="2026-10-16T12:06:00Z")
    observation = funding_record_fixture(kind)
    observation.update(
        as_of="2026-10-16T12:00:00Z",
        as_of_prec="BUCKET(5m)",
        as_of_bucket_anchor="START",
        received_ts="2026-10-16T12:03:00Z",
    )
    if kind == "SETTLED":
        observation.update(
            settlement_time="2026-10-16T12:00:00Z",
            settlement_time_prec="BUCKET(5m)",
            settlement_time_bucket_anchor="START",
            funding_time="2026-10-16T12:00:00Z",
            funding_time_prec="BUCKET(5m)",
            funding_time_bucket_anchor="START",
        )
    captured = snapshot["funding"]
    assert isinstance(captured, list)
    captured[-1] = observation
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is False
    assert row["funding_vs_normal"] is None
    assert row["funding_8h_kind"] == "SETTLED"
    assert row["funding_settled_last_observed_at"] == "2026-10-09T08:00:00Z"
