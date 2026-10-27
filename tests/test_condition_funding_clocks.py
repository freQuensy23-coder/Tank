"""Funding timing roles preserve metadata and never authorize overlapping buckets."""

import pytest
from funding_condition_fixtures import evaluate, packet
from funding_input_fixtures import funding_record_fixture
from pydantic import JsonValue

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


def test_receipt_bucket_overlap_changes_visibility_without_changing_content_or_head() -> None:
    from tank.analysis.condition_funding_codec import FundingConditionLookup
    from tank.analysis.condition_graph_contracts import ConditionTopCacheKey

    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(
        received_ts="2026-10-16T12:04:00Z", received_ts_prec="BUCKET(5m)", received_ts_bucket_anchor="START"
    )
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    _, before = evaluate(snapshot, cache, baseline)
    assert before["available"] is False
    snapshot["assembled_at"] = "2026-10-16T12:10:00Z"
    _, after = evaluate(snapshot, cache, baseline)
    assert after["available"] is True
    assert after["reused"] is False
    assert after["dependency_fingerprint"] == before["dependency_fingerprint"]
    assert after["content_sha256"] == before["content_sha256"]
    assert after["visibility_sha256"] != before["visibility_sha256"]


def test_assembly_bucket_cannot_claim_a_receipt_before_every_possible_assembly_time() -> None:
    snapshot = packet()
    snapshot.update(assembled_at_prec="BUCKET(5m)", assembled_at_bucket_anchor="END")
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1]["received_ts"] = "2026-10-16T12:03:00Z"
    _, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is False


@pytest.mark.parametrize("anchor", [None, "UNSPECIFIED"])
def test_unknown_bucket_orientation_does_not_authorize_current_funding(anchor: str | None) -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1]["as_of_prec"] = "BUCKET(5m)"
    if anchor is not None:
        captured[-1]["as_of_bucket_anchor"] = anchor
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is False
    assert row["funding_vs_normal"] is None


@pytest.mark.parametrize(
    "anchor,reported", [("START", "2026-10-16T11:55:00Z"), ("END", "2026-10-16T12:00:00Z")]
)
def test_wholly_past_bucket_with_guaranteed_freshness_retains_its_original_label(
    anchor: str, reported: str
) -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(as_of=reported, as_of_prec="BUCKET(5m)", as_of_bucket_anchor=anchor)
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is True
    assert row["funding_estimate_observed_at"] == reported
    assert row["funding_estimate_observed_at_prec"] == "BUCKET(5m)"
    assert row["funding_estimate_observed_at_bucket_anchor"] == anchor


def test_source_bucket_cannot_use_its_end_label_to_hide_the_oldest_possible_sample_age() -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(as_of_prec="BUCKET(10m)", as_of_bucket_anchor="END")
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert row["funding_8h_kind"] == "CURRENT_ESTIMATE"
    assert row["funding_estimate_observed_at"] == "2026-10-16T12:00:00Z"
    assert row["funding_vs_normal"] is None
    assert proof["available"] is False


def test_settled_collection_clock_cannot_supply_paid_event_timing() -> None:
    snapshot = packet()
    observation = funding_record_fixture("SETTLED")
    observation.update(
        settlement_time="2026-10-16T12:00:00Z",
        settlement_time_prec="COLLECTION_ONLY",
        funding_time="2026-10-16T12:00:00Z",
        funding_time_prec="COLLECTION_ONLY",
    )
    captured = snapshot["funding"]
    assert isinstance(captured, list)
    captured[-1] = observation
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is False
    assert row["funding_settled_last_observed_at"] == "2026-10-09T08:00:00Z"


def test_current_collection_observation_and_unreported_scalar_precision_remain_distinct() -> None:
    snapshot = packet()
    first, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert proof["available"] is True
    assert first["funding_estimate_observed_at_prec"] == "COLLECTION_ONLY"
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].pop("as_of_prec")
    captured[-1].pop("received_ts_prec")
    snapshot.pop("assembled_at_prec")
    unknown, uncertain_proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert uncertain_proof["available"] is True
    assert unknown["funding_estimate_observed_at_prec"] is None
    assert unknown["funding_estimate_observed_at_prec_reason"]
    assert uncertain_proof["assembled_at_prec"] is None
    assert uncertain_proof["assembled_at_prec_reason"]


def test_overlapping_possible_latest_sources_do_not_choose_a_nominal_anchor() -> None:
    snapshot = packet()
    captured = snapshot["funding"]
    assert isinstance(captured, list) and isinstance(captured[-1], dict)
    captured[-1].update(as_of_prec="BUCKET(5m)", as_of_bucket_anchor="END")
    point = funding_record_fixture("CURRENT_ESTIMATE")
    point.update(as_of="2026-10-16T11:58:00Z", as_of_prec="EXACT", rate="0.0615", rate_8h_eq="0.123")
    captured.append(point)
    row, proof = evaluate(snapshot, {}, BaselineCaches.with_capacity(128))
    assert row["funding_estimate_8h_eq"] is None
    assert proof["available"] is False


def test_event_start_uses_its_own_declared_clock_and_invalidates_changed_anchor_metadata() -> None:
    from funding_condition_fixtures import evaluate_window

    from tank.analysis.condition_funding_codec import FundingConditionLookup
    from tank.analysis.condition_graph_contracts import ConditionTopCacheKey

    snapshot = packet()
    anchor: dict[str, JsonValue] = {
        "type": "T1",
        "ts": "2026-10-16T11:00:00Z",
        "ts_prec": "BUCKET(5m)",
        "ts_bucket_anchor": "END",
    }
    snapshot["anchor"] = anchor
    captured = snapshot["funding"]
    assert isinstance(captured, list)
    prior = funding_record_fixture("SETTLED")
    prior.update(
        settlement_time="2026-10-16T10:58:00Z",
        funding_time="2026-10-16T10:58:00Z",
        as_of="2026-10-16T10:58:00Z",
        rate="0.0025",
        rate_8h_eq="0.005",
    )
    captured.append(prior)
    cache: dict[ConditionTopCacheKey, FundingConditionLookup] = {}
    baseline = BaselineCaches.with_capacity(128)
    before, first = evaluate_window(snapshot, cache, baseline, window="EVENT")
    assert before["funding_start_observed_at"] == "2026-10-09T08:00:00Z"
    anchor["ts_prec"] = "EXACT"
    anchor.pop("ts_bucket_anchor")
    after, second = evaluate_window(snapshot, cache, baseline, window="EVENT")
    assert after["funding_start_observed_at"] == "2026-10-16T10:58:00Z"
    assert first["content_sha256"] != second["content_sha256"]
    assert second["reused"] is False
