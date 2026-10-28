import gzip
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

MANIFEST_SHA256 = "a4f924bafc62ca2d422da7245e2a4cade6b011fd87e864ea998edd07660c9285"
PUBLIC_TABLES = {
    "registry-resolved": "reg.instruments",
    "registry-tokens": "reg.tokens",
    "capture-tier-public": "ops.capture_tier_transitions",
    "capture-gaps-public": "mkt.capture_gaps",
}
PRIVATE_CONTENT = re.compile(
    r"://|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|-----BEGIN|"
    r"(?:password|passwd|api_key|access_token|authorization|telegram_id|chat_id|user_id|email)\s*[\"=:]",
    re.IGNORECASE,
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON field")
        result[key] = value
    return result


def read_json(value):
    return json.loads(
        value,
        parse_float=Decimal,
        parse_constant=lambda value: require(False, "Nonfinite JSON constant"),
        object_pairs_hook=unique_object,
    )


def public_value(value, allowed=None):
    if isinstance(value, dict):
        require(allowed is None or set(value) <= allowed, "Unexpected recursive public field")
        for key, child in value.items():
            require(not PRIVATE_CONTENT.search(key), "Private public-fixture key")
            public_value(child, allowed)
    elif isinstance(value, list):
        for child in value:
            public_value(child, allowed)
    elif isinstance(value, str):
        require(not PRIVATE_CONTENT.search(value), "Private public-fixture value")
    elif isinstance(value, Decimal):
        require(value.is_finite(), "Nonfinite public JSON number")


def validate_fixture(fixtures):
    fixtures = Path(fixtures)
    require(digest(fixtures / "manifest.json") == MANIFEST_SHA256, "Full fixture manifest mismatch")
    manifest = read_json((fixtures / "manifest.json").read_text())
    for entry in manifest["auxiliary_files"]:
        path = fixtures / entry["name"]
        require(path.stat().st_size == entry["bytes"], "Auxiliary fixture size mismatch")
        require(digest(path) == entry["sha256"], "Auxiliary fixture hash mismatch")
    for entry in manifest["files"]:
        path = fixtures / entry["name"]
        require(path.stat().st_size == entry["compressed_bytes"], "Fixture archive size mismatch")
        require(digest(path) == entry["archive_sha256"], "Fixture archive hash mismatch")
        checksum, size, rows = hashlib.sha256(), 0, 0
        with gzip.open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""):
                checksum.update(block)
                size += len(block)
                rows += block.count(b"\n")
        require(checksum.hexdigest() == entry["sha256"], "Uncompressed fixture hash mismatch")
        require((size, rows) == (entry["bytes"], entry["rows"]), "Fixture size/count mismatch")
    privacy = read_json((fixtures / "privacy.json").read_text())
    require(privacy["omitted_occurrences"] == manifest["privacy_omitted_occurrences"], "Omission mismatch")
    require(privacy["unapproved_fields_or_values"] == 0, "Unapproved public fields")
    require(privacy["all_urls_omitted"] and privacy["source_archive_not_selected"], "Privacy mismatch")
    return manifest, privacy


def retained_rows(fixtures, entry, privacy, schema):
    checksum, size, count, seen = hashlib.sha256(), 0, 0, set()
    table = entry["table"]
    columns = set(entry["columns"])
    require(columns <= set(privacy["top_level_allowlist"][table]), "Public schema mismatch")
    require(columns <= set(schema) | {"storage_key"}, "Selected package schema mismatch")
    recursive = privacy["recursive_key_allowlist"].get(table)
    with gzip.open(Path(fixtures) / entry["name"], "rb") as stream:
        for line in stream:
            checksum.update(line)
            size += len(line)
            row = read_json(line)
            require(isinstance(row, dict) and set(row) <= columns, "Unexpected captured field")
            require("source_archive" not in row, "Source archive was not captured")
            seen.update(row)
            for key, value in row.items():
                kind = schema.get(key, "text")
                decoded = (
                    read_json(value)
                    if value is not None and kind == "jsonb" and isinstance(value, str)
                    else value
                )
                public_value(decoded, set(recursive) if recursive and kind == "jsonb" else None)
                if value is not None and kind == "timestamptz":
                    timestamp(value)
                elif value is not None and kind == "numeric":
                    require(Decimal(str(value)).is_finite(), "Nonfinite captured numeric")
            if table in privacy["recursive_key_allowlist"] and not table.startswith("mkt."):
                public_value(row, set(recursive))
            count += 1
            yield row, line.decode().strip()
    require(seen == columns, "Captured column union mismatch")
    require((size, count) == (entry["bytes"], entry["rows"]), "Stream fixture count mismatch")
    require(checksum.hexdigest() == entry["sha256"], "Stream fixture hash mismatch")


def native_counts(fixtures, manifest):
    path = Path(fixtures) / "kline_day_counts.json"
    entry = next(row for row in manifest["auxiliary_files"] if row["name"] == path.name)
    require(digest(path) == entry["sha256"], "Native cache hash mismatch")
    cache = read_json(path.read_text())
    require(
        cache["version"] == 1 and cache["saved_at"] == manifest["native_cache"]["saved_at"],
        "Cache header mismatch",
    )
    first = timestamp(manifest["history_start"]).replace(hour=0, minute=0, second=0, microsecond=0)
    first += timedelta(days=1)
    last = timestamp(manifest["semantic_cutoff"]).replace(hour=0, minute=0, second=0, microsecond=0)
    expected = {
        (iid, first + timedelta(days=offset))
        for iid in manifest["peer_spot_instrument_ids"]
        for offset in range((last - first).days)
    }
    counts = {}
    for iid, day, count in cache["counts"]:
        key = iid, timestamp(day)
        require(key not in counts and type(count) is int and count >= 0, "Invalid native cache count")
        counts[key] = count
    require(set(counts) == expected, "Native cache dimensions mismatch")
    require(len(counts) == manifest["native_cache"]["entries"], "Native cache cardinality mismatch")
    for observed, field in (
        (sum(counts.values()), "eligible_whole_day_rows"),
        (sum(n > 0 for n in counts.values()), "positive_entries"),
        (sum(n == 0 for n in counts.values()), "zero_entries"),
    ):
        require(observed == manifest["native_cache"][field], "Native cache total mismatch")
    return counts


async def load_native_day_counts(market, fixtures, manifest):
    expected = native_counts(fixtures, manifest)
    require(
        digest(market._day_counts_path()) == digest(Path(fixtures) / "kline_day_counts.json"),
        "Installed native cache mismatch",
    )
    require(
        set(manifest["peer_spot_instrument_ids"]) <= set(market.instruments),
        "Missing configured peers",
    )
    require(not market._kline_day_counts, "Day-count cache must start empty")
    loaded = await market._load_day_counts()
    require(
        loaded == len(expected) and market._kline_day_counts == expected,
        "Native cache restore/invalidation mismatch",
    )
    coverage = await market._series_counts(
        manifest["peer_spot_instrument_ids"],
        manifest["history_start"],
        manifest["semantic_cutoff"],
        manifest["availability_cutoff"],
    )
    require(coverage == manifest["peer_coverage_counts"], "Native coverage/edge counts mismatch")
    return {
        "loaded": loaded,
        "saved_at": manifest["native_cache"]["saved_at"],
        "native_invalidation_executed": True,
        "coverage": coverage,
    }


async def postgres_count(repo, table):
    from psycopg import sql

    async with repo.transaction():
        query = sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(*table.split(".")))
        return (await (await repo._conn().execute(query)).fetchone())["n"]


async def insert_postgres(repo, table, rows, columns):
    from psycopg import sql

    names = sql.SQL(",").join(map(sql.Identifier, columns))
    target = sql.Identifier(*table.split("."))
    query = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM jsonb_populate_record(NULL::{}, %s::jsonb)").format(
        target, names, names, target
    )
    async with repo.transaction(), repo._conn().cursor() as cursor:
        await cursor.executemany(query, [(raw,) for raw in rows])


async def seed_full(repo, settings, fixtures):
    from psycopg.conninfo import conninfo_to_dict

    from tank import clickhouse_market
    from tank.market_schema import MARKET_COLUMNS, MARKET_IDENTITIES
    from tank.models import TABLE_COLUMNS

    database = conninfo_to_dict(repo.database_url).get("dbname", "")
    require(re.fullmatch(r"tank_test_fixture_[a-f0-9]{32}", database), "Owned fixture database required")
    require(repo.clickhouse.database == "mkt_" + database, "Owned ClickHouse database required")
    schema_path = Path(clickhouse_market.__file__).with_name("market_schema.py")
    require(
        digest(schema_path) == "250a2cb262289908da7887754f8fa632d8a4f82ffec20d2dc485b9587ceb2823",
        "Original/current typed storage contract mismatch",
    )
    manifest, privacy = validate_fixture(fixtures)
    for filename, entry in manifest["source_config_git_objects"].items():
        require(
            hashlib.sha256(settings.read_config_bytes(Path(filename).name)).hexdigest() == entry["sha256"],
            "Original package config mismatch",
        )
    native_counts(fixtures, manifest)
    cache_path = Path(settings.data_dir) / "market" / "kline_day_counts.json"
    require(not cache_path.exists(), "Native day-count cache already exists")
    inventory, envelopes = [], []
    for entry in manifest["files"]:
        captured = entry["table"]
        table = PUBLIC_TABLES.get(captured, captured)
        is_ch = table in clickhouse_market.MARKET_TABLES
        schema = MARKET_COLUMNS.get(table, TABLE_COLUMNS[table])
        columns = (
            ["storage_key", *MARKET_COLUMNS[table]]
            if table in MARKET_COLUMNS
            else ["storage_key", "document", "immutable", *TABLE_COLUMNS[table]]
        )
        if captured in PUBLIC_TABLES:
            schema = {key: schema.get(key, "text") for key in entry["columns"]}
            if captured == "capture-tier-public":
                schema["capture_cadences"] = "jsonb"
        if is_ch:
            existing = await repo.clickhouse.rows(f"SELECT count() AS n FROM {table} FINAL")
            require(existing == [{"n": 0}], "ClickHouse fixture target is not empty")
        else:
            require(await postgres_count(repo, table) == 0, "PostgreSQL fixture target is not empty")
        batch, by_instrument, missing = [], Counter(), Counter()
        for index, (row, raw) in enumerate(retained_rows(fixtures, entry, privacy, schema)):
            by_instrument[row.get("instrument_id") or row.get("token_id")] += 1
            missing.update(set(entry["columns"]) - set(row))
            if is_ch:
                require(
                    isinstance(row.get("storage_key"), str) and row["storage_key"],
                    "Missing actual storage key",
                )
                require(
                    all(row.get(key) is not None for key in MARKET_IDENTITIES[table]),
                    "Missing actual market identity",
                )
                batch.append({**row, "_version": 1})
            else:
                additions = {}
                if captured in PUBLIC_TABLES:
                    key = row.get(
                        "instrument_id" if captured == "registry-resolved" else "token_id"
                    )
                    if captured.startswith("capture-"):
                        key = f"fixture-public:{captured}:{index:06d}"
                        field = "transition_id" if captured == "capture-tier-public" else "gap_id"
                        additions[field] = key
                    document = raw[:-1] + ("," + json.dumps(additions)[1:] if additions else "}")
                    additions["storage_key"] = key
                    if not table.startswith("mkt."):
                        additions["immutable"] = False
                        payload = json.dumps(additions)[:-1] + ',"document":' + document + "}"
                        raw = raw[:-1] + "," + payload[1:]
                    else:
                        raw = raw[:-1] + "," + json.dumps(additions)[1:]
                require(
                    row.get("storage_key") or additions.get("storage_key"),
                    "Missing PostgreSQL storage identity",
                )
                batch.append(raw)
            if len(batch) == 5000:
                if is_ch:
                    await repo.clickhouse.insert(table, batch)
                else:
                    await insert_postgres(repo, table, batch, columns)
                batch.clear()
        if batch:
            if is_ch:
                await repo.clickhouse.insert(table, batch)
            else:
                await insert_postgres(repo, table, batch, columns)
        if is_ch:
            identity = ",".join(f"`{field}`" for field in MARKET_IDENTITIES[table])
            counted = await repo.clickhouse.rows(
                f"SELECT count() AS n, uniqExact(storage_key) AS keys, "
                f"uniqExact(tuple({identity})) AS identities FROM {table} FINAL"
            )
            require(
                counted == [{"n": entry["rows"], "keys": entry["rows"], "identities": entry["rows"]}],
                "ClickHouse typed count/identity mismatch",
            )
        else:
            require(
                await postgres_count(repo, table) == entry["rows"], "PostgreSQL typed count mismatch"
            )
        inventory.append(
            {
                "name": entry["name"],
                "collection": table,
                "rows": entry["rows"],
                "by_instrument": dict(by_instrument),
                "absent_captured_columns": dict(missing),
                "sha256": entry["sha256"],
                "archive_sha256": entry["archive_sha256"],
            }
        )
        if captured in PUBLIC_TABLES:
            envelopes.append(
                {"collection": table, "identity": "Fixture-owned storage identity, not captured"}
            )
    for table in manifest["confirmed_empty_tables"]:
        require(
            await repo.clickhouse.rows(f"SELECT count() AS n FROM {table} FINAL") == [{"n": 0}],
            "Captured empty collection is not empty",
        )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("xb") as stream:
        stream.write((Path(fixtures) / "kline_day_counts.json").read_bytes())
    require(
        digest(cache_path) == digest(Path(fixtures) / "kline_day_counts.json"),
        "Installed native cache mismatch",
    )
    return {
        "manifest": manifest,
        "inputs": inventory,
        "qualification": manifest["qualification"],
        "expected_coverage": manifest["peer_coverage_counts"],
        "expected_dense_projections": manifest["dense_projections"],
        "public_envelopes": envelopes,
        "native_cache_path": str(cache_path),
        "native_cache_loaded": False,
        "manifest_sha256": MANIFEST_SHA256,
        "loader_sha256": digest(__file__),
        "selected_contract_sha256": digest(schema_path),
        "limitations": manifest["limitations"],
        "synthetic_controls": [],
    }
