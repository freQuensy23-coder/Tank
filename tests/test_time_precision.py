from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import JsonValue

from tank.models import Record
from tank.time_precision import (
    chosen_boundary,
    collection_clock,
    offset_schedule,
    propagate_timestamp,
    reconcile_precision,
)
from tank.util import (
    EpochInput,
    EpochUnit,
    clock_iso,
    iso,
    parse_epoch,
    parse_iso_time,
    parse_time,
    parse_utc_time,
)

AT = "2026-10-18T00:00:00Z"


def test_collection_clock_policy_and_canonical_fields() -> None:
    assert collection_clock(AT).fields("created_at") == {
        "created_at": AT,
        "created_at_prec": "COLLECTION_ONLY",
        "created_at_prec_reason": None,
    }
    with pytest.raises(ValueError):
        collection_clock(AT, policy="EXACT")


def test_propagation_preserves_bucket_duration_and_existing_sql_alias() -> None:
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


def test_legacy_unknown_never_infers_exact_from_microseconds() -> None:
    stamp = propagate_timestamp({"cutoff": "2026-10-18T00:00:00.123456Z"}, "cutoff")
    assert stamp.precision is None and stamp.precision_reason == "INCOMPLETE"
    assert offset_schedule(stamp, timedelta(seconds=60)).precision_reason == "INCOMPLETE"
    assert propagate_timestamp({"last_item_at": None}, "last_item_at").value is None


def test_explicit_boundaries_and_bucket_units() -> None:
    assert chosen_boundary(AT, precision="EXACT").precision == "EXACT"
    assert chosen_boundary(AT, precision="BUCKET", bucket_size=(300, "s")).precision == "BUCKET(300s)"
    with pytest.raises(ValueError, match="explicit unit"):
        chosen_boundary(AT, precision="BUCKET", bucket_size="300")
    with pytest.raises(ValueError):
        chosen_boundary(AT, precision="BUCKET(1h)", bucket_size="2h")
    with pytest.raises(ValueError):
        chosen_boundary(AT, precision="UNKNOWN")


def test_conflicting_aliases_refused_and_generic_alias_explicit() -> None:
    with pytest.raises(ValueError):
        propagate_timestamp(
            {"public_at": AT, "public_at_prec": "EXACT", "public_at_precision": "BUCKET(1d)"}, "public_at"
        )
    raw = {"ts": AT, "timestamp_precision": "EXACT"}
    assert propagate_timestamp(raw, "ts").precision is None
    assert propagate_timestamp(raw, "ts", precision_aliases=("timestamp_precision",)).precision == "EXACT"


def test_nested_event_timestamp_and_selection_keep_precision() -> None:
    source: Record = {"event_time": {"value": AT, "precision": "BUCKET_1D", "quote": "yesterday"}}
    stamp = propagate_timestamp(source, "event_time")
    assert stamp.fields("event_time")["event_time_prec"] == "BUCKET(1d)"
    assert offset_schedule(stamp, timedelta(hours=-24)).precision == "BUCKET(1d)"


def test_missing_publication_preserves_declared_collection_only() -> None:
    stamp = propagate_timestamp(
        {"published_at": None, "published_prec": "COLLECTION_ONLY", "published_at_reason": "NOT_ESTIMABLE"},
        "published_at",
    )
    assert stamp.precision == "COLLECTION_ONLY" and stamp.value is None
    assert stamp.fields("published_at")["published_at_reason"] == "NOT_ESTIMABLE"


def test_bucket_duration_and_bounds_preserve_explicit_anchor() -> None:
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


def test_end_labelled_source_bucket_preserves_raw_time_and_roundtrips() -> None:
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


def test_unknown_source_bucket_edge_does_not_invent_an_interval() -> None:
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
def test_bucket_edge_cannot_upgrade_or_conflict_with_precision(precision: str | None) -> None:
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


@pytest.mark.parametrize(
    "value,unit",
    [
        (1700000000.123456, "s"),
        ("1700000000123.456", "ms"),
        (1700000000123456, "us"),
    ],
)
def test_declared_epoch_units_preserve_existing_microsecond_rounding(
    value: EpochInput, unit: EpochUnit
) -> None:
    assert iso(parse_epoch(value, unit=unit)) == "2023-11-14T22:13:20.123456Z"


@pytest.mark.parametrize("value", [True, False, float("nan"), float("inf"), "NaN"])
def test_declared_epoch_rejects_boolean_and_nonfinite_values(value: EpochInput) -> None:
    with pytest.raises(ValueError):
        parse_epoch(value, unit="s")


def test_explicit_utc_decoder_preserves_zone_less_transport_contract() -> None:
    expected = datetime(2026, 10, 24, 12, tzinfo=UTC)
    assert parse_utc_time("2026-10-24 12:00:00") == expected
    assert parse_utc_time(datetime(2026, 10, 24, 12)) == expected


def test_explicit_processing_clock_is_separate_from_formatting(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed = datetime(2026, 10, 24, 12, tzinfo=UTC)
    monkeypatch.setattr("tank.util.utcnow", lambda: fixed)
    assert clock_iso() == "2026-10-24T12:00:00Z"


def test_iso_decoder_requires_a_declared_timezone() -> None:
    expected = datetime(2026, 10, 24, 12, 3, 4, 123456, tzinfo=UTC)
    assert parse_iso_time("2026-10-24T14:03:04.123456+02:00") == expected
    assert parse_iso_time(expected) == expected
    with pytest.raises(ValueError, match="explicit timezone"):
        parse_iso_time("2026-10-24 12:03:04.123456")
    with pytest.raises(ValueError, match="explicit timezone"):
        parse_iso_time(datetime(2026, 10, 24, 12))
    with pytest.raises(ValueError, match="explicit epoch unit"):
        parse_iso_time("1700000000000")


def test_generic_iso_clock_is_required_and_aware() -> None:
    assert iso("2026-10-24T14:03:04.123456+02:00") == "2026-10-24T12:03:04.123456Z"
    for value in ("2026-10-24 12:03:04", "1700000000000"):
        with pytest.raises(ValueError):
            parse_time(value)
        with pytest.raises(ValueError):
            iso(value)
    with pytest.raises(ValueError, match="explicit timezone"):
        iso(datetime(2026, 10, 24, 12))


@pytest.mark.parametrize("value", [0, 1700000000000, True, Decimal("1700000000"), float("inf")])
def test_generic_timestamp_wire_evidence_does_not_guess_epoch_units(value: JsonValue | Decimal) -> None:
    with pytest.raises(ValueError, match="requires ISO date-time"):
        propagate_timestamp({"ts": value, "ts_prec": "EXACT"}, "ts")


def test_calendar_precision_metadata_does_not_require_or_invent_an_instant() -> None:
    assert reconcile_precision([], field="identity_reviewed_at") is None
    assert reconcile_precision(["BUCKET(1d)", "BUCKET(86400s)"], field="identity_reviewed_at") == "BUCKET(1d)"
    with pytest.raises(ValueError, match="Conflicting"):
        reconcile_precision(["EXACT", "BUCKET(1d)"], field="identity_reviewed_at")


def test_explicit_empty_precision_field_is_not_replaced_by_a_canonical_default() -> None:
    stamp = collection_clock("2026-10-18T00:00:00Z")
    with pytest.raises(ValueError, match="nonempty name"):
        stamp.fields("ts", precision_field="")
    assert stamp.fields("ts", precision_field="observed_precision")["observed_precision"] == (
        "COLLECTION_ONLY"
    )
