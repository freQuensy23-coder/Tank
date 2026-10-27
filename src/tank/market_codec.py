"""Pure market persistence row codecs; no repository or network dependencies."""

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pydantic import JsonValue

from tank.market_schema import MARKET_ALIASES, MARKET_COLUMNS, MARKET_IDENTITIES
from tank.util import JsonInput, iso, jsonable, parse_iso_time

type StoredValue = JsonValue | datetime | Decimal
type StoredRow = dict[str, StoredValue]
type IdentityValue = str | int | float | bool | datetime | Decimal


def _timestamp(value: StoredValue) -> datetime:
    if not isinstance(value, (str, datetime)):
        raise ValueError("Stored timestamp requires ISO date-time text or an aware datetime")
    return parse_iso_time(value)


def _optional_object(value: JsonValue, field: str) -> dict[str, JsonValue]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be an object")
    return value


def _object_index(value: JsonValue, field: str) -> dict[str, dict[str, JsonValue]]:
    result: dict[str, dict[str, JsonValue]] = {}
    for key, child in _optional_object(value, field).items():
        if not isinstance(child, dict):
            raise ValueError(f"{field}.{key} must be an object")
        result[key] = child
    return result


def _optional_text(value: JsonValue, field: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise ValueError(f"{field} must be text")


def _identity(value: StoredValue, collection: str) -> IdentityValue:
    if value is None or value == "" or not isinstance(value, (str, int, float, bool, datetime, Decimal)):
        raise ValueError(f"Missing typed market identity {collection}: {MARKET_IDENTITIES[collection]}")
    return value


FINGERPRINT_EXCLUDED = {"mkt.book_summaries": frozenset({"bids", "asks"})}
UNPROVEN = "STREAM_COVERAGE_UNPROVEN"


def _typed_value(collection: str, name: str, value: StoredValue) -> StoredValue:
    if value is None:
        return None
    kind = MARKET_COLUMNS[collection][name]
    if kind == "timestamptz":
        return _timestamp(value)
    if kind == "numeric":
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError(f"{collection}.{name} requires a decimal")
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"{collection}.{name} requires a decimal") from exc
    if kind == "text" and not isinstance(value, str):
        raise ValueError(f"{collection}.{name} requires text")
    if kind == "boolean" and not isinstance(value, bool):
        raise ValueError(f"{collection}.{name} requires a boolean")
    if kind == "bigint" and (isinstance(value, bool) or not isinstance(value, int)):
        raise ValueError(f"{collection}.{name} requires an integer")
    if kind == "text[]" and (not isinstance(value, list) or any(not isinstance(item, str) for item in value)):
        raise ValueError(f"{collection}.{name} requires a list of text")
    return value


def _generated_key(identity: tuple[IdentityValue, ...]) -> str:
    return ":".join(str(value) for value in identity)


def _remember_generated_key(
    owners: dict[str, tuple[IdentityValue, ...]], key: str, identity: tuple[IdentityValue, ...]
) -> None:
    previous = owners.get(key)
    if previous is not None and previous != identity:
        raise ValueError("Conflicting generated market storage key")
    owners[key] = identity


def _validate_generated_market_keys(collection: str, records: Sequence[Mapping[str, JsonInput]]) -> None:
    """Check the complete dispatch's native identity coordinates before chunk I/O."""
    fields = MARKET_IDENTITIES[collection]
    if "storage_key" in fields:
        return
    columns = MARKET_COLUMNS[collection]
    aliases = MARKET_ALIASES.get(collection, {})
    owners: dict[str, tuple[IdentityValue, ...]] = {}
    for record in records:
        coordinates: StoredRow = {}
        for name, raw in record.items():
            field = aliases.get(name, name)
            if field not in fields:
                continue
            value = jsonable(raw)
            if columns[field] == "timestamptz" and isinstance(value, dict):
                if "value" not in value and "ts" not in value:
                    raise ValueError("Timestamp evidence requires a declared value or ts field")
                value = value.get("value", value.get("ts"))
            normalized = _typed_value(collection, field, value)
            if field in coordinates and coordinates[field] != normalized:
                raise ValueError(f"Conflicting market aliases {collection}.{name}")
            coordinates[field] = normalized
        identity = tuple(_identity(coordinates.get(field), collection) for field in fields)
        _remember_generated_key(owners, _generated_key(identity), identity)


def encode(collection: str, record: Mapping[str, JsonInput]) -> StoredRow:
    from tank.time_precision import propagate_timestamp

    columns = MARKET_COLUMNS[collection]
    aliases = MARKET_ALIASES.get(collection, {})
    timestamps = {name for name, kind in columns.items() if kind == "timestamptz"}
    time_aliases = {name: target for name, target in aliases.items() if target in timestamps}
    for key, raw in record.items():
        if aliases.get(key, key) not in timestamps or raw is None:
            continue
        if isinstance(raw, Mapping):
            if "value" in raw:
                raw = raw["value"]
            elif "ts" in raw:
                raw = raw["ts"]
            else:
                raise ValueError("Timestamp evidence requires a declared value or ts field")
        if raw is not None:
            if not isinstance(raw, (str, datetime)):
                raise ValueError("Stored timestamp requires ISO date-time text or an aware datetime")
            parse_iso_time(raw)
    document = jsonable(dict(record))
    precision_names = {field + "_precision": field for field in timestamps}
    precision_names.update({alias + "_precision": target for alias, target in time_aliases.items()})
    result: StoredRow = {}
    value: StoredValue
    for key, value in document.items():
        if key in precision_names or key.removesuffix("_reason") in precision_names:
            continue
        target = aliases.get(key, key)
        if target not in columns:
            raise ValueError(f"Unknown typed market field {collection}.{key}")
        if target in timestamps and value is not None:
            if isinstance(value, dict):
                value = value.get("value", value.get("ts"))
            value = _timestamp(value) if value is not None else None
        if target in result and result[target] != value:
            if target.endswith("_prec"):
                continue
            raise ValueError(f"Conflicting market aliases {collection}.{key}")
        result[target] = value
    for field in timestamps:
        associated = [alias for alias, target in time_aliases.items() if target == field]
        precision_aliases = [name for alias in associated for name in (alias + "_prec", alias + "_precision")]
        if collection == "mkt.oi_obs" and field == "ts":
            precision_aliases.append("timestamp_precision")
        declared = field in result or any(
            document.get(name) is not None
            for name in (field + "_prec", field + "_precision", *precision_aliases)
        )
        if declared:
            selected = dict(document)
            if field in document:
                selected[field] = document[field]
            else:
                for alias in associated:
                    if alias in document:
                        selected[field] = document[alias]
                        break
            stamp = propagate_timestamp(selected, field, precision_aliases=precision_aliases)
            result.update(stamp.fields(field))
    for name, value in list(result.items()):
        result[name] = _typed_value(collection, name, value)
    return result


def decode(collection: str, row: Mapping[str, StoredValue]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    value: StoredValue
    for key, kind in MARKET_COLUMNS[collection].items():
        value = row.get(key)
        if value is None:
            result[key] = None
            continue
        if kind == "timestamptz":
            value = iso(_timestamp(value))
        elif kind == "numeric":
            value = str(value)
        result[key] = jsonable(value)
    for alias, target in MARKET_ALIASES.get(collection, {}).items():
        if target in result:
            result[alias] = result[target]
    return result


def semantic_fingerprint(record: JsonInput, collection: str | None = None) -> str:
    excluded = FINGERPRINT_EXCLUDED.get(collection, frozenset()) if collection is not None else frozenset()
    document = jsonable(record)
    if excluded:
        if not isinstance(document, dict):
            raise ValueError("Collection fingerprint requires an object")
        document = {k: v for k, v in document.items() if k not in excluded}

    def strip(value: JsonValue) -> JsonValue:
        if isinstance(value, dict):
            return {
                k: strip(v)
                for k, v in value.items()
                if v is not None
                and not k.startswith(("received_ts", "recorded_at", "updated_at", "observed_at"))
                and k not in ("code_version", "config_version", "semantic_fingerprint")
            }
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value

    return hashlib.sha256(
        json.dumps(strip(document), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def merge_point(
    collection: str, old: Mapping[str, JsonInput], incoming: Mapping[str, JsonInput]
) -> dict[str, JsonValue]:
    """Memory-test parity for the named server-side point mergers."""
    import copy

    old_document = jsonable(dict(old))
    incoming_document = jsonable(dict(incoming))
    merged = copy.deepcopy(old_document)
    merged.update({k: v for k, v in incoming_document.items() if v is not None})
    if collection == "mkt.funding_obs":
        sources = copy.deepcopy(_object_index(old_document.get("source_observations"), "source_observations"))
        for key, value in _object_index(
            incoming_document.get("source_observations"), "source_observations"
        ).items():
            if semantic_fingerprint(sources.get(key)) != semantic_fingerprint(value):
                sources[key] = copy.deepcopy(value)
        merged["source_observations"] = {key: value for key, value in sources.items()}
        preferred: str | None = "PUBLIC_REST" if merged.get("kind") == "SETTLED" else "WEBSOCKET"
        preferred = (
            preferred
            if preferred in sources
            else _optional_text(incoming_document.get("capture_source"), "capture_source")
        )
        if preferred is not None and preferred in sources:
            chosen = sources[preferred]
            for field in (
                "rate",
                "interval_h",
                "rate_8h_eq",
                "as_of",
                "as_of_prec",
                "as_of_prec_reason",
                "funding_time",
                "funding_time_prec",
                "funding_time_prec_reason",
                "received_ts",
                "received_ts_prec",
                "received_ts_prec_reason",
                "recorded_at",
                "recorded_at_prec",
                "recorded_at_prec_reason",
                "source_path",
            ):
                if field in chosen:
                    merged[field] = chosen[field]
            merged["capture_source"] = preferred
            merged["primary_source"] = preferred
        merged["source_disagreement"] = {
            "present": len(
                {str(v["rate_8h_eq"]) for v in sources.values() if v.get("rate_8h_eq") is not None}
            )
            > 1,
            "values": {
                k: {f: v.get(f) for f in ("rate", "interval_h", "rate_8h_eq")} for k, v in sources.items()
            },
        }
    elif collection == "mkt.tickers_24h":
        sources = copy.deepcopy(_object_index(old_document.get("field_sources"), "field_sources"))
        for field, meta in _object_index(incoming_document.get("field_sources"), "field_sources").items():
            prior = sources.get(field)
            if (
                prior is None
                or meta.get("as_of")
                and (not prior.get("as_of") or _timestamp(meta["as_of"]) >= _timestamp(prior["as_of"]))
            ):
                sources[field] = copy.deepcopy(meta)
            elif field in old_document:
                merged[field] = old_document[field]
        for field in _optional_object(old_document.get("field_sources"), "field_sources"):
            if (
                field not in _optional_object(incoming_document.get("field_sources"), "field_sources")
                and field in old_document
            ):
                merged[field] = old_document[field]
        merged["field_sources"] = {key: value for key, value in sources.items()}
        capture_sources = {
            source
            for meta in sources.values()
            if (source := _optional_text(meta.get("capture_source"), "capture_source")) is not None
        }
        merged["capture_sources"] = list(sorted(capture_sources))
        for prefix in ("mark", "index"):
            component = sources.get(prefix + "_price")
            if component:
                for suffix in ("as_of", "as_of_prec", "as_of_prec_reason"):
                    merged[prefix + "_" + suffix] = component.get(suffix)
        merged["unclocked_components"] = _optional_object(
            old_document.get("unclocked_components"), "unclocked_components"
        ) | _optional_object(incoming_document.get("unclocked_components"), "unclocked_components")
    merged["semantic_fingerprint"] = semantic_fingerprint(merged)
    return merged


def _unproves(collection: str, old: Mapping[str, JsonInput], new: Mapping[str, JsonInput]) -> bool:
    return (
        collection == "mkt.flow_bars_1m"
        and old.get("complete") is True
        and new.get("complete") is not True
        and (new.get("gap_reason") or UNPROVEN) == UNPROVEN
    )


def _prepare_records(
    collection: str,
    records: Sequence[Mapping[str, JsonInput]],
    keys: Sequence[str] | None,
) -> dict[str, StoredRow]:
    """Encoded, de-duplicated and fingerprinted batch records by storage key (no I/O)."""
    encoded = [encode(collection, r) for r in records]
    generated = keys is None
    if keys is None:
        if "storage_key" in MARKET_IDENTITIES[collection]:
            raise ValueError(f"{collection} requires explicit storage keys")
        keys = [
            _generated_key(
                tuple(_identity(row.get(field), collection) for field in MARKET_IDENTITIES[collection])
            )
            for row in encoded
        ]
    keys = list(keys)
    if len(keys) != len(records):
        raise ValueError("keys and records must have equal lengths")
    unique: dict[str, StoredRow] = {}
    identity_keys: dict[tuple[IdentityValue, ...], str] = {}
    generated_owners: dict[str, tuple[IdentityValue, ...]] = {}
    for key, record in zip(keys, encoded, strict=True):
        identity = tuple(
            str(key) if name == "storage_key" else _identity(record.get(name), collection)
            for name in MARKET_IDENTITIES[collection]
        )
        if any(value is None or value == "" for value in identity):
            raise ValueError(f"Missing typed market identity {collection}: {MARKET_IDENTITIES[collection]}")
        if generated:
            _remember_generated_key(generated_owners, key, identity)
        previous_key = identity_keys.get(identity)
        if previous_key is not None:
            previous_record = unique[previous_key]
            if _unproves(collection, previous_record, record):
                continue
            unique.pop(previous_key)
            if collection in ("mkt.funding_obs", "mkt.tickers_24h"):
                record = encode(collection, merge_point(collection, previous_record, record))
        identity_keys[identity] = str(key)
        record["semantic_fingerprint"] = semantic_fingerprint(record, collection)
        unique[str(key)] = record
    return unique
