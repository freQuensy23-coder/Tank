"""Exact consumed-input identities for condition facts and visibility witnesses."""

import hashlib
import json
from array import array
from collections.abc import Mapping, Sequence

from tank.analysis.condition_visibility import VisibilityDomain, condition_visibility
from tank.time_precision import propagate_timestamp, timestamp_owned_fields
from tank.util import JsonInput, jsonable

type ConditionFingerprintInput = (
    JsonInput
    | array[int]
    | array[float]
    | bytearray
    | Mapping[str, ConditionFingerprintInput]
    | list[ConditionFingerprintInput]
    | tuple[ConditionFingerprintInput, ...]
)


def _hash_input(value: ConditionFingerprintInput) -> JsonInput:
    """Preserve the established compact-buffer hash spelling at the wire boundary."""
    if isinstance(value, array):
        numbers: list[JsonInput] = [number for number in value]
        return numbers
    if isinstance(value, bytearray):
        return str(value)
    if isinstance(value, Mapping):
        return {key: _hash_input(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_hash_input(child) for child in value]
    return value


def _fingerprint(value: JsonInput) -> str:
    return hashlib.sha256(
        json.dumps(jsonable(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def condition_input_fingerprints(
    snapshot: Mapping[str, ConditionFingerprintInput], fields: Sequence[str], domain: VisibilityDomain
) -> tuple[str, str, str]:
    if "assembled_at" in fields:
        raise ValueError("Processing assembly must remain a fresh evaluation context")
    consumed: dict[str, JsonInput] = {field: _hash_input(snapshot.get(field)) for field in fields}
    series_facts = (
        "times_us",
        "values",
        "complete",
        "instrument_id",
        "token_id",
        "venue",
        "market_type",
        "source_market_type",
        "interval_s",
        "intervals_s",
        "time_basis",
        "cadence_s",
        "metric_key",
        "precision",
        "precision_reason",
        "coverage_count",
    )
    for field in ("stat_price_series", "stat_volume_series"):
        series = consumed.get(field)
        if series is None:
            continue
        if not isinstance(series, Mapping):
            raise ValueError("Compact condition input requires scoped series projections")
        selected: dict[str, JsonInput] = {}
        for iid, value in series.items():
            if not isinstance(value, Mapping):
                raise ValueError("Compact condition input requires scoped series projections")
            selected[iid] = {name: value[name] for name in series_facts if name in value}
        consumed[field] = selected
    clocks = jsonable(
        {
            name: _hash_input(snapshot[name])
            for field in ("cutoff", "anchor", "assembled_at")
            for name in timestamp_owned_fields(field)
            if name in snapshot
        }
    )
    identity: dict[str, JsonInput] = {
        "inputs": consumed,
        "cutoff_clock": propagate_timestamp(clocks, "cutoff").fields("cutoff"),
    }
    if "anchor" in fields:
        identity["anchor_clock"] = propagate_timestamp(
            clocks, "anchor", missing_reason="NO_EVENT_ANCHOR"
        ).fields("anchor")
    content = _fingerprint(identity)
    heads = snapshot.get("domain_dependency_fingerprints")
    if heads is not None and not isinstance(heads, Mapping):
        raise ValueError("Condition dependency identities require a domain mapping")
    supplied = heads.get(domain) if isinstance(heads, Mapping) else None
    dependency = (
        content
        if supplied is None
        else supplied
        if isinstance(supplied, str)
        else _fingerprint(_hash_input(supplied))
    )
    token_id = snapshot.get("token_id")
    if not isinstance(token_id, str) or not token_id.strip():
        raise ValueError("Condition input identity requires the actual token")
    visibility = condition_visibility(
        domain=domain,
        token_id=token_id,
        assembled_at=propagate_timestamp(clocks, "assembled_at"),
        content_sha256=content,
        consumed=consumed,
    )
    return dependency, content, visibility.projection.fingerprint()
