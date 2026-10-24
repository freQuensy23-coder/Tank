"""Pure market persistence row codecs; no repository or network dependencies."""

import hashlib
import json
from decimal import Decimal

from tank.market_schema import MARKET_ALIASES, MARKET_COLUMNS, MARKET_IDENTITIES
from tank.util import iso, jsonable, parse_time

FINGERPRINT_EXCLUDED = {"mkt.book_summaries": frozenset({"bids", "asks"})}
UNPROVEN = "STREAM_COVERAGE_UNPROVEN"


def encode(collection, record):
    from tank.time_precision import propagate_timestamp

    record = jsonable(record)
    columns = MARKET_COLUMNS[collection]
    aliases = MARKET_ALIASES.get(collection, {})
    timestamps = {name for name, kind in columns.items() if kind == "timestamptz"}
    time_aliases = {name: target for name, target in aliases.items() if target in timestamps}
    precision_names = {field + "_precision": field for field in timestamps}
    precision_names.update({alias + "_precision": target for alias, target in time_aliases.items()})
    result = {}
    for key, value in record.items():
        if key in precision_names or key.removesuffix("_reason") in precision_names:
            continue
        target = aliases.get(key, key)
        if target not in columns:
            raise ValueError(f"Unknown typed market field {collection}.{key}")
        if target in timestamps and value is not None:
            if isinstance(value, dict):
                value = value.get("value", value.get("ts"))
            value = parse_time(value) if value is not None else None
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
            record.get(name) is not None
            for name in (field + "_prec", field + "_precision", *precision_aliases)
        )
        if declared:
            selected = dict(record)
            selected[field] = record.get(
                field, next((record[alias] for alias in associated if alias in record), result.get(field))
            )
            stamp = propagate_timestamp(selected, field, precision_aliases=precision_aliases)
            result.update(stamp.fields(field))
    for name, value in list(result.items()):
        if value is None:
            continue
        kind = columns[name]
        if kind == "timestamptz":
            result[name] = parse_time(value)
        elif kind == "numeric":
            result[name] = Decimal(str(value))
        elif kind == "text" and not isinstance(value, str):
            raise ValueError(f"{collection}.{name} requires text")
    return result


def decode(collection, row):
    result = {}
    for key, kind in MARKET_COLUMNS[collection].items():
        value = row.get(key)
        if value is None:
            result[key] = None
            continue
        if kind == "timestamptz":
            value = iso(value)
        elif kind == "numeric":
            value = str(value)
        result[key] = value
    for alias, target in MARKET_ALIASES.get(collection, {}).items():
        if target in result:
            result[alias] = result[target]
    return result


def semantic_fingerprint(record, collection=None):
    excluded = FINGERPRINT_EXCLUDED.get(collection, frozenset())
    if excluded:
        record = {k: v for k, v in record.items() if k not in excluded}

    def strip(value):
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
        json.dumps(strip(jsonable(record)), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def merge_point(collection, old, incoming):
    """Memory-test parity for the named server-side point mergers."""
    import copy

    merged = copy.deepcopy(old)
    merged.update({k: v for k, v in incoming.items() if v is not None})
    if collection == "mkt.funding_obs":
        sources = copy.deepcopy(old.get("source_observations") or {})
        for key, value in (incoming.get("source_observations") or {}).items():
            if semantic_fingerprint(sources.get(key)) != semantic_fingerprint(value):
                sources[key] = copy.deepcopy(value)
        merged["source_observations"] = sources
        preferred = "PUBLIC_REST" if merged.get("kind") == "SETTLED" else "WEBSOCKET"
        preferred = preferred if preferred in sources else incoming.get("capture_source")
        if preferred in sources:
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
        sources = copy.deepcopy(old.get("field_sources") or {})
        for field, meta in (incoming.get("field_sources") or {}).items():
            prior = sources.get(field)
            if (
                prior is None
                or meta.get("as_of")
                and (not prior.get("as_of") or parse_time(meta["as_of"]) >= parse_time(prior["as_of"]))
            ):
                sources[field] = copy.deepcopy(meta)
            elif field in old:
                merged[field] = old[field]
        for field in old.get("field_sources") or {}:
            if field not in (incoming.get("field_sources") or {}) and field in old:
                merged[field] = old[field]
        merged["field_sources"] = sources
        merged["capture_sources"] = sorted(
            {m["capture_source"] for m in sources.values() if m.get("capture_source")}
        )
        for prefix in ("mark", "index"):
            meta = sources.get(prefix + "_price")
            if meta:
                for suffix in ("as_of", "as_of_prec", "as_of_prec_reason"):
                    merged[prefix + "_" + suffix] = meta.get(suffix)
        merged["unclocked_components"] = (old.get("unclocked_components") or {}) | (
            incoming.get("unclocked_components") or {}
        )
    merged["semantic_fingerprint"] = semantic_fingerprint(merged)
    return merged


def _unproves(collection, old, new):
    return (
        collection == "mkt.flow_bars_1m"
        and old.get("complete") is True
        and new.get("complete") is not True
        and (new.get("gap_reason") or UNPROVEN) == UNPROVEN
    )


def _prepare_records(collection, records, keys, columns, migration_target):
    """Encoded, de-duplicated and fingerprinted batch records by storage key (no I/O)."""
    encoded = [encode(collection, r) for r in records]
    if migration_target:
        for row in encoded:
            unsupported = [name for name, value in row.items() if name not in columns and value is not None]
            if unsupported:
                raise ValueError(f"Non-null fields unavailable in migration target: {unsupported}")
    if keys is None:
        keys = [":".join(str(r.get(k, "")) for k in MARKET_IDENTITIES[collection]) for r in encoded]
    keys = list(keys)
    if len(keys) != len(records):
        raise ValueError("keys and records must have equal lengths")
    unique = {}
    identity_keys = {}
    for key, record in zip(keys, encoded, strict=True):
        identity = tuple(
            str(key) if name == "storage_key" else record.get(name) for name in MARKET_IDENTITIES[collection]
        )
        if any(value is None or value == "" for value in identity):
            raise ValueError(f"Missing typed market identity {collection}: {MARKET_IDENTITIES[collection]}")
        previous_key = identity_keys.get(identity)
        if previous_key is not None:
            previous_record = unique.pop(previous_key)
            if collection in ("mkt.funding_obs", "mkt.tickers_24h"):
                record = encode(collection, merge_point(collection, previous_record, record))
        identity_keys[identity] = str(key)
        record["semantic_fingerprint"] = semantic_fingerprint(record, collection)
        unique[str(key)] = record
    return unique
