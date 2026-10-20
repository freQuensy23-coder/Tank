from datetime import timedelta

import pytest

from tank.time_precision import chosen_boundary, collection_clock, offset_schedule, propagate_timestamp

AT = "2026-10-18T00:00:00Z"


def test_collection_clock_policy_and_canonical_fields():
    assert collection_clock(AT).fields("created_at") == {
        "created_at": AT,
        "created_at_prec": "COLLECTION_ONLY",
        "created_at_prec_reason": None,
    }
    with pytest.raises(ValueError):
        collection_clock(AT, policy="EXACT")


def test_propagation_preserves_bucket_duration_and_existing_sql_alias():
    stamp = propagate_timestamp(
        {"published_prec": "BUCKET", "published_at_precision": "BUCKET(1d)", "published_at": AT},
        "published_at",
        bucket_size=(86400, "s"),
    )
    assert stamp.precision == "BUCKET(86400s)"
    fields = stamp.fields("published_at")
    assert fields["published_prec"] == "BUCKET"
    assert fields["published_at_bucket_size_s"] == 86400
    assert propagate_timestamp(fields, "published_at").precision == "BUCKET(86400s)"
    exact_fields = chosen_boundary(AT, precision="EXACT").fields("published_at")
    assert exact_fields["published_at_bucket_size_s"] is None
    assert propagate_timestamp({**fields, **exact_fields}, "published_at").precision == "EXACT"
    assert offset_schedule(stamp, timedelta(hours=24)).precision == stamp.precision
    assert offset_schedule(stamp, timedelta(hours=24)).value == "2026-10-19T00:00:00Z"


def test_legacy_unknown_never_infers_exact_from_microseconds():
    stamp = propagate_timestamp({"cutoff": "2026-10-18T00:00:00.123456Z"}, "cutoff")
    assert stamp.precision is None and stamp.precision_reason == "INCOMPLETE"
    assert offset_schedule(stamp, timedelta(seconds=60)).precision_reason == "INCOMPLETE"
    assert propagate_timestamp({"last_item_at": None}, "last_item_at").value is None


def test_explicit_boundaries_and_bucket_units():
    assert chosen_boundary(AT, precision="EXACT").precision == "EXACT"
    assert chosen_boundary(AT, precision="BUCKET", bucket_size=(300, "s")).precision == "BUCKET(300s)"
    with pytest.raises(TypeError):
        chosen_boundary(AT, precision="BUCKET", bucket_size=300)
    with pytest.raises(ValueError):
        chosen_boundary(AT, precision="BUCKET(1h)", bucket_size="2h")
    with pytest.raises(ValueError):
        chosen_boundary(AT, precision="UNKNOWN")


def test_conflicting_aliases_refused_and_generic_alias_explicit():
    with pytest.raises(ValueError):
        propagate_timestamp(
            {"public_at": AT, "public_at_prec": "EXACT", "public_at_precision": "BUCKET(1d)"}, "public_at"
        )
    raw = {"ts": AT, "timestamp_precision": "EXACT"}
    assert propagate_timestamp(raw, "ts").precision is None
    assert propagate_timestamp(raw, "ts", precision_aliases=("timestamp_precision",)).precision == "EXACT"


def test_nested_event_timestamp_and_selection_keep_precision():
    source = {"event_time": {"value": AT, "precision": "BUCKET_1D", "quote": "yesterday"}}
    stamp = propagate_timestamp(source, "event_time")
    assert stamp.fields("event_time")["event_time_prec"] == "BUCKET(1d)"
    assert offset_schedule(stamp, timedelta(hours=-24)).precision == "BUCKET(1d)"


def test_missing_publication_preserves_declared_collection_only():
    stamp = propagate_timestamp(
        {"published_at": None, "published_prec": "COLLECTION_ONLY", "published_at_reason": "NOT_ESTIMABLE"},
        "published_at",
    )
    assert stamp.precision == "COLLECTION_ONLY" and stamp.value is None
    assert stamp.fields("published_at")["published_at_reason"] == "NOT_ESTIMABLE"


def test_bucket_duration_and_bounds_preserve_explicit_anchor():
    from tank.time_precision import bucket_duration, interval_bounds
    from tank.util import parse_time

    assert bucket_duration("BUCKET(300s)") == timedelta(minutes=5)
    assert bucket_duration("BUCKET_1D") == timedelta(days=1)
    anchor = "2026-10-18T00:03:00Z"
    bounds = interval_bounds(chosen_boundary(anchor, precision="BUCKET(5m)"))
    assert bounds == (parse_time(anchor), parse_time(anchor) + timedelta(minutes=5))
    assert interval_bounds(chosen_boundary(AT, precision="EXACT")) == (parse_time(AT), parse_time(AT))
    assert interval_bounds(collection_clock(AT)) == (None, None)
    assert interval_bounds(propagate_timestamp({"public_at": AT}, "public_at")) == (None, None)
    with pytest.raises(ValueError):
        bucket_duration("BUCKET")
    with pytest.raises(ValueError):
        bucket_duration("EXACT")
    with pytest.raises(ValueError):
        bucket_duration("BUCKET(0.0000001s)")


def test_end_labelled_source_bucket_preserves_raw_time_and_roundtrips():
    from tank.time_precision import interval_bounds
    from tank.util import parse_time

    source = {"ts": AT, "ts_prec": "BUCKET(1h)", "ts_bucket_anchor": "END"}
    stamp = propagate_timestamp(source, "ts")
    assert stamp.value == AT and stamp.precision == "BUCKET(1h)"
    assert interval_bounds(stamp) == (parse_time(AT) - timedelta(hours=1), parse_time(AT))
    assert propagate_timestamp(stamp.fields("ts"), "ts") == stamp
    later = offset_schedule(stamp, timedelta(hours=1))
    assert later.bucket_anchor == "END"
    assert interval_bounds(later) == (parse_time(AT), parse_time(AT) + timedelta(hours=1))


def test_unknown_source_bucket_edge_does_not_invent_an_interval():
    from tank.time_precision import interval_bounds

    stamp = propagate_timestamp({"ts": AT, "ts_prec": "BUCKET(5m)", "ts_bucket_anchor": "UNSPECIFIED"}, "ts")
    assert stamp.value == AT and stamp.precision == "BUCKET(5m)"
    assert interval_bounds(stamp) == (None, None)
    assert propagate_timestamp(stamp.fields("ts"), "ts") == stamp
    nullable = propagate_timestamp({"ts": AT, "ts_prec": "BUCKET(5m)", "ts_bucket_anchor": None}, "ts")
    assert nullable == stamp
    exact = propagate_timestamp({"ts": AT, "ts_prec": "EXACT", "ts_bucket_anchor": None}, "ts")
    assert exact.bucket_anchor is None and interval_bounds(exact)[0] == interval_bounds(exact)[1]


@pytest.mark.parametrize("precision", ["EXACT", "COLLECTION_ONLY", None])
def test_bucket_edge_cannot_upgrade_or_conflict_with_precision(precision):
    with pytest.raises(ValueError, match="Bucket anchor requires"):
        propagate_timestamp({"ts": AT, "ts_prec": precision, "ts_bucket_anchor": "END"}, "ts")
    with pytest.raises(ValueError, match="Conflicting timestamp bucket anchors"):
        propagate_timestamp(
            {
                "ts": {"value": AT, "precision": "BUCKET(1h)", "bucket_anchor": "END"},
                "ts_bucket_anchor": "START",
            },
            "ts",
        )
