"""Explicit timestamp provenance; formatting and precision are separate decisions.

No constructor infers source precision from ISO formatting, decimal places or local storage.
Producers choose the correct constructor; repositories must not assign source precision.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from pydantic import JsonValue

from tank.util import TimeInput, iso, parse_time

type BucketSize = str | timedelta | tuple[int | float | Decimal | str, str]
type TimestampRecord = Mapping[str, JsonValue | datetime | Decimal]


def _text(value: JsonValue | datetime | Decimal, field: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise ValueError(f"{field} requires text")


def _time(value: JsonValue | datetime | Decimal) -> TimeInput:
    if isinstance(value, bool) or not isinstance(value, (datetime, str, int, float, Decimal)):
        raise ValueError("Timestamp value must be a declared date or number")
    return value


CANONICAL_PRECISION_FIELDS = {"published_at": "published_prec"}
_BUCKET = re.compile(r"^BUCKET\(([^()]+)\)$")
_SIZE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)(us|ms|s|m|h|d|w)$")
_LEGACY_BUCKETS = {"HOUR": "1h", "DAY": "1d", "BUCKET_1H": "1h", "BUCKET_1D": "1d", "BUCKET_1M": "1m"}
_FACTORS = {
    "us": Decimal("0.000001"),
    "ms": Decimal("0.001"),
    "s": Decimal(1),
    "m": Decimal(60),
    "h": Decimal(3600),
    "d": Decimal(86400),
    "w": Decimal(604800),
}


def _size(value: BucketSize) -> str:
    if isinstance(value, timedelta):
        value = (
            str(Decimal(value.days * 86400 + value.seconds) + Decimal(value.microseconds) / Decimal(1000000))
            + "s"
        )
    elif isinstance(value, tuple) and len(value) == 2:
        value = str(value[0]) + str(value[1])
    if not isinstance(value, str):
        raise TypeError("Bucket size requires an explicit unit, e.g. '300s' or (300,'s')")
    match = _SIZE.fullmatch(value.strip())
    if match is None or Decimal(match[1]) <= 0:
        raise ValueError("Bucket size must be a positive duration with an explicit unit")
    return value.strip()


def _duration(value: str) -> Decimal:
    match = _SIZE.fullmatch(value)
    assert match is not None
    return Decimal(match[1]) * _FACTORS[match[2]]


def normalize_precision(precision: str | None, *, bucket_size: BucketSize | None = None) -> str | None:
    """Normalize explicit supported legacy names; a bare BUCKET needs an explicit duration."""
    if precision is None or precision == "UNKNOWN":
        return None
    if precision in ("EXACT", "COLLECTION_ONLY"):
        if bucket_size is not None:
            raise ValueError("Non-bucket precision cannot carry a bucket size")
        return precision
    embedded = _LEGACY_BUCKETS.get(precision)
    match = _BUCKET.fullmatch(precision)
    if match:
        embedded = _size(match[1])
    if precision == "BUCKET" and bucket_size is None:
        return None
    supplied = _size(bucket_size) if bucket_size is not None else None
    if embedded is None and precision != "BUCKET":
        raise ValueError(f"Unsupported timestamp precision: {precision}")
    if embedded is not None and supplied is not None and _duration(embedded) != _duration(supplied):
        raise ValueError("Conflicting bucket duration declarations")
    duration = embedded or supplied
    assert duration is not None
    return "BUCKET(" + duration + ")"


def _equivalent(left: str | None, right: str | None) -> bool:
    if left == right:
        return True
    left_match = _BUCKET.fullmatch(left) if left else None
    right_match = _BUCKET.fullmatch(right) if right else None
    if left_match and right_match:
        return _duration(left_match[1]) == _duration(right_match[1])
    return False


@dataclass(frozen=True)
class Timestamp:
    value: str | None
    precision: str | None
    precision_reason: str | None = None
    bucket_anchor: str | None = None

    def __post_init__(self) -> None:
        if self.value is not None:
            parse_time(self.value)
        if self.precision is None:
            if not self.precision_reason:
                raise ValueError("Unknown precision requires an explicit reason")
        elif (self.value is None and self.precision != "COLLECTION_ONLY") or normalize_precision(
            self.precision
        ) != self.precision:
            raise ValueError("Timestamp requires a value or explicitly COLLECTION_ONLY missing occurrence")
        if self.bucket_anchor is not None:
            if self.bucket_anchor not in {"START", "END", "UNSPECIFIED"}:
                raise ValueError("Unsupported timestamp bucket anchor")
            if not self.precision or not _BUCKET.fullmatch(self.precision):
                raise ValueError("Bucket anchor requires explicit bucket precision")

    def fields(self, field: str, *, precision_field: str | None = None) -> dict[str, str | int | None]:
        """Emit the original field plus its canonical precision and explicit unknown reason."""
        sibling = precision_field or CANONICAL_PRECISION_FIELDS.get(field, field + "_prec")
        result: dict[str, str | int | None] = {
            field: self.value,
            sibling: self.precision,
            sibling + "_reason": self.precision_reason,
        }
        if self.bucket_anchor is not None:
            result[field + "_bucket_anchor"] = self.bucket_anchor
        if field == "published_at" and sibling == "published_prec":
            result["published_at_precision"] = self.precision
            result["published_at_bucket_size_s"] = None
            match = _BUCKET.fullmatch(self.precision) if self.precision else None
            if match:
                seconds = _duration(match[1])
                result["published_prec"] = "BUCKET"
                result["published_at_bucket_size_s"] = (
                    int(seconds) if seconds == seconds.to_integral_value() else str(seconds)
                )
            if self.value is None:
                result["published_at_reason"] = self.precision_reason or "INCOMPLETE"
        return result


def collection_clock(value: TimeInput | None = None, *, policy: str = "COLLECTION_ONLY") -> Timestamp:
    """Call only where the producer actually observes a local collection/processing clock."""
    if policy != "COLLECTION_ONLY":
        raise ValueError("Collection-clock policy must be explicitly COLLECTION_ONLY")
    return Timestamp(iso(value), policy)


def chosen_boundary(value: TimeInput, *, precision: str, bucket_size: BucketSize | None = None) -> Timestamp:
    """A caller-defined boundary, with caller-declared exactness or bucket duration."""
    normalized = normalize_precision(precision, bucket_size=bucket_size)
    if value is None or normalized is None or normalized == "COLLECTION_ONLY":
        raise ValueError("Chosen boundaries require a value and explicit EXACT or BUCKET(size) precision")
    return Timestamp(iso(value), normalized)


def propagate_timestamp(
    record: TimestampRecord,
    field: str,
    *,
    precision_aliases: Sequence[str] = (),
    bucket_size: BucketSize | None = None,
    missing_reason: str = "INCOMPLETE",
) -> Timestamp:
    """Copy precision with a selected timestamp; reject contradictory declared aliases.

    Reads canonical <field>_prec (published_prec for published_at), documented
    <field>_precision aliases, and nested {value|ts,precision|prec}. Generic provider
    timestamp_precision is read only when explicitly named in precision_aliases.
    Unknown legacy precision remains None with a reason; it never becomes EXACT.
    """
    if bucket_size is None and record.get(field + "_bucket_size_s") is not None:
        size = record[field + "_bucket_size_s"]
        if isinstance(size, bool) or not isinstance(size, (str, int, float, Decimal)):
            raise ValueError("Bucket size must be a number of seconds")
        bucket_size = (size, "s")
    raw = record.get(field)
    declarations: list[str] = []
    anchors = []
    reason = None
    value: JsonValue | datetime | Decimal
    if isinstance(raw, Mapping):
        value = raw.get("value", raw.get("ts"))
        for key in ("precision", "prec"):
            declaration = _text(raw.get(key), key)
            if declaration is not None:
                declarations.append(declaration)
        reason = _text(raw.get("precision_reason"), "precision_reason")
        if raw.get("bucket_anchor") is not None:
            anchors.append(_text(raw["bucket_anchor"], "bucket_anchor"))
    else:
        value = raw
    if record.get(field + "_bucket_anchor") is not None:
        anchors.append(_text(record[field + "_bucket_anchor"], "bucket_anchor"))
    if anchors and any(anchor != anchors[0] for anchor in anchors[1:]):
        raise ValueError(f"Conflicting timestamp bucket anchors for {field}")
    bucket_anchor = anchors[0] if anchors else None
    canonical = CANONICAL_PRECISION_FIELDS.get(field, field + "_prec")
    names = dict.fromkeys((canonical, field + "_prec", field + "_precision", *precision_aliases))
    for name in names:
        if record.get(name) is not None:
            declaration = _text(record[name], name)
            if declaration is not None:
                declarations.append(declaration)
        reason = reason or _text(record.get(name + "_reason"), name + "_reason")
    normalized = [normalize_precision(item, bucket_size=bucket_size) for item in declarations]
    if normalized and any(not _equivalent(normalized[0], item) for item in normalized[1:]):
        raise ValueError(f"Conflicting timestamp precision declarations for {field}")
    precision = normalized[0] if normalized else None
    if (
        bucket_anchor is None
        and precision
        and _BUCKET.fullmatch(precision)
        and (field + "_bucket_anchor" in record or isinstance(raw, Mapping) and "bucket_anchor" in raw)
    ):
        bucket_anchor = "UNSPECIFIED"
    if value is None:
        reason = reason or _text(record.get(field + "_reason"), field + "_reason") or missing_reason
        return Timestamp(None, "COLLECTION_ONLY" if precision == "COLLECTION_ONLY" else None, reason)
    return Timestamp(
        iso(_time(value)),
        precision,
        None if precision is not None else reason or missing_reason,
        bucket_anchor,
    )


def offset_schedule(anchor: Timestamp, delta: timedelta) -> Timestamp:
    """A fixed offset changes the value, never the selected anchor's uncertainty."""
    if not isinstance(anchor, Timestamp) or not isinstance(delta, timedelta):
        raise TypeError("Offset schedules require a Timestamp anchor and timedelta")
    value = iso(parse_time(anchor.value) + delta) if anchor.value is not None else None
    return Timestamp(value, anchor.precision, anchor.precision_reason, anchor.bucket_anchor)


def bucket_duration(precision: str) -> timedelta:
    """Return an explicitly declared bucket duration without guessing a unit or rounding."""
    canonical = normalize_precision(precision)
    match = _BUCKET.fullmatch(canonical) if canonical else None
    if match is None:
        raise ValueError("A known BUCKET(size) precision is required")
    microseconds = _duration(match[1]) * Decimal(1000000)
    if microseconds != microseconds.to_integral_value():
        raise ValueError("Bucket duration is finer than supported microsecond resolution")
    return timedelta(microseconds=int(microseconds))


def interval_bounds(timestamp: Timestamp) -> tuple[datetime | None, datetime | None]:
    """Preserve the provider's declared bucket edge; an unknown edge has no interval.

    Existing records without an edge retain their original start-bucket contract.
    End-labelled observations retain the provider timestamp rather than shifting it.
    """
    if timestamp.value is None or timestamp.precision is None or timestamp.precision == "COLLECTION_ONLY":
        return None, None
    point = parse_time(timestamp.value)
    if timestamp.precision == "EXACT":
        return point, point
    if timestamp.bucket_anchor == "UNSPECIFIED":
        return None, None
    if timestamp.bucket_anchor == "END":
        return point - bucket_duration(timestamp.precision), point
    return point, point + bucket_duration(timestamp.precision)
