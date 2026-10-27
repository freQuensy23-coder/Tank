"""Explicit assembly-comparison inputs for pure condition cache identity."""

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import Field

from tank.analysis.observation_clock import receipt_visible
from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument
from tank.time_precision import Timestamp, propagate_timestamp, timestamp_owned_fields
from tank.util import JsonInput, canonical_json, jsonable, parse_iso_time, sha256

type VisibilityDomain = Literal[
    "spot", "oi", "funding", "close", "dc", "depth", "state", "relative", "derivatives", "supply"
]
type SourceClockField = Literal["received_ts", "observed_at"]
type AssemblyRelation = Literal[
    "AT_OR_BEFORE_ASSEMBLY",
    "AFTER_ASSEMBLY",
    "SOURCE_CLOCK_UNCAPTURED",
    "ASSEMBLY_UNCAPTURED",
    "INVALID_SOURCE_CLOCK",
    "ORDER_UNCERTAIN",
]


class InvalidVisibilityClock(DomainModel):
    kind: Literal["INVALID_CAPTURED_TIMESTAMP"]


class ConditionClockComparison(DomainModel):
    source_paths: tuple[str, ...] = Field(min_length=1)
    field: SourceClockField
    source_clock: Timestamp | InvalidVisibilityClock
    relation: AssemblyRelation


class ConditionVisibilityProjection(DomainModel):
    format: Literal["CONDITION_VISIBILITY_V1"]
    domain: VisibilityDomain
    token_id: str = Field(min_length=1, pattern=r"\S")
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    assembly_knowledge: Literal["KNOWN", "UNCAPTURED"]
    comparisons: tuple[ConditionClockComparison, ...]

    def fingerprint(self) -> str:
        return sha256(self)


class ConditionVisibilityEvaluation(DomainModel):
    assembled_at: Timestamp
    projection: ConditionVisibilityProjection


def _record(value: JsonInput, path: str) -> Mapping[str, JsonInput]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Condition visibility source requires an object: {path}")
    return value


def _records(value: JsonInput, path: str) -> tuple[Mapping[str, JsonInput], ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"Condition visibility source requires an array: {path}")
    return tuple(_record(row, f"{path}[{index}]") for index, row in enumerate(value))


def _nested(row: Mapping[str, JsonInput], field: str, path: str) -> Mapping[str, JsonInput] | None:
    value = row.get(field)
    return _record(value, path) if value else None


def condition_visibility(
    *,
    domain: VisibilityDomain,
    token_id: str,
    assembled_at: Timestamp,
    content_sha256: str,
    consumed: Mapping[str, JsonInput],
) -> ConditionVisibilityEvaluation:
    """Project only the receipt/observation comparisons used by current builders.

    This identifies unchanged calculation inputs, not admission of a financial fact.
    Missing or malformed source clocks retain explicit states; they never become a
    receipt. Source content is a canonical read projection, not provider HTTP bytes.
    """
    assembly = Timestamp.model_validate(assembled_at)
    groups: dict[
        tuple[SourceClockField, str], tuple[Timestamp | InvalidVisibilityClock, AssemblyRelation, list[str]]
    ] = {}

    def compare(row: Mapping[str, JsonInput], path: str, field: SourceClockField) -> None:
        selected = {name: row[name] for name in timestamp_owned_fields(field) if name in row}
        try:
            raw_key = canonical_json(selected)
        except (ValueError, TypeError, OverflowError):
            raw_key = "INVALID_CAPTURED_TIMESTAMP"
        group_key = (field, raw_key)
        if group_key in groups:
            groups[group_key][2].append(path)
            return
        clock: Timestamp | InvalidVisibilityClock
        try:
            wire = JsonDocument.validate_value(jsonable(selected))
            if not isinstance(wire, dict):
                raise ValueError("A timestamp projection must be an object")
            clock = propagate_timestamp(wire, field, missing_reason="SOURCE_CLOCK_NOT_CAPTURED")
        except (ValueError, TypeError, OverflowError):
            clock = InvalidVisibilityClock(kind="INVALID_CAPTURED_TIMESTAMP")
        relation: AssemblyRelation
        if isinstance(clock, InvalidVisibilityClock):
            relation = "INVALID_SOURCE_CLOCK"
        elif clock.value is None:
            relation = "SOURCE_CLOCK_UNCAPTURED"
        elif assembly.value is None:
            relation = "ASSEMBLY_UNCAPTURED"
        elif domain == "funding":
            visible = receipt_visible(clock, assembly)
            relation = (
                "AT_OR_BEFORE_ASSEMBLY"
                if visible is True
                else "AFTER_ASSEMBLY"
                if visible is False
                else "ORDER_UNCERTAIN"
            )
        else:
            relation = (
                "AT_OR_BEFORE_ASSEMBLY"
                if parse_iso_time(clock.value) <= parse_iso_time(assembly.value)
                else "AFTER_ASSEMBLY"
            )
        groups[group_key] = (clock, relation, [path])

    typed_spot = domain in {"spot", "state"}
    spot_read = domain in {"spot", "close", "dc", "state", "relative"}
    references = domain in {"state", "relative"}
    fields: Sequence[str] = (
        "flow_bars",
        "flow_parent_bars",
        "flow_segments",
        "tickers",
        "klines",
        "books",
        "oi",
        "funding",
        "liquidations",
        "supply",
        "funding_confirmations",
    )
    for name in fields:
        if name not in consumed:
            continue
        if name == "flow_parent_bars" and not typed_spot:
            continue
        if name == "funding_confirmations" and domain != "state":
            continue
        for index, row in enumerate(_records(consumed[name], name)):
            path = f"{name}[{index}]"
            compare(row, path, "received_ts")
            if name in {"flow_bars", "flow_parent_bars", "tickers"} and typed_spot:
                compare(row, path, "observed_at")
            if name == "flow_segments" and spot_read:
                compare(row, path, "observed_at")
            if name in {"flow_bars", "flow_parent_bars"} and spot_read:
                for child_index, child in enumerate(_records(row.get("boundary_segments"), path)):
                    child_path = f"{path}.boundary_segments[{child_index}]"
                    compare(child, child_path, "received_ts")
                    compare(child, child_path, "observed_at")
            if name == "flow_bars" and references:
                for point_index, point in enumerate(_records(row.get("reference_points"), path)):
                    point_path = f"{path}.reference_points[{point_index}]"
                    compare(point, point_path, "received_ts")
                    compare(point, point_path, "observed_at")
                    trade = _nested(point, "last_trade", point_path + ".last_trade")
                    if trade is not None:
                        compare(trade, point_path + ".last_trade", "received_ts")
                trade = _nested(row, "last_trade", path + ".last_trade")
                if trade is not None:
                    compare(trade, path + ".last_trade", "received_ts")
            if name == "books" and domain == "state":
                rest = _nested(row, "independent_rest", path + ".independent_rest")
                if rest is not None:
                    compare(rest, path + ".independent_rest", "received_ts")
    if references:
        for index, point in enumerate(_records(consumed.get("reference_points"), "reference_points")):
            path = f"reference_points[{index}]"
            compare(point, path, "received_ts")
            compare(point, path, "observed_at")
            trade = _nested(point, "last_trade", path + ".last_trade")
            if trade is not None:
                compare(trade, path + ".last_trade", "received_ts")
    return ConditionVisibilityEvaluation(
        assembled_at=assembly,
        projection=ConditionVisibilityProjection(
            format="CONDITION_VISIBILITY_V1",
            domain=domain,
            token_id=token_id,
            content_sha256=content_sha256,
            assembly_knowledge="KNOWN" if assembly.value is not None else "UNCAPTURED",
            comparisons=tuple(
                ConditionClockComparison(
                    source_paths=tuple(paths), field=field, source_clock=clock, relation=relation
                )
                for (field, _raw_key), (clock, relation, paths) in groups.items()
            ),
        ),
    )
