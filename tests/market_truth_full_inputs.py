import ast
import gzip
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter, process_time
from urllib.parse import urlsplit

MANIFEST_SHA256 = "a4f924bafc62ca2d422da7245e2a4cade6b011fd87e864ea998edd07660c9285"
PUBLIC_TABLES = {
    "registry-resolved": "reg.instruments",
    "registry-tokens": "reg.tokens",
    "capture-tier-public": "ops.capture_tier_transitions",
    "capture-gaps-public": "mkt.capture_gaps",
}
_PRIVATE_CONTENT_PREFIX = r"://|"
_PRIVATE_CONTENT_SUFFIX = (
    r"-----BEGIN|"
    r"(?:password|passwd|api_key|access_token|authorization|telegram_id|chat_id|user_id|email)\s*[\"=:]"
)
PRIVATE_CONTENT = re.compile(
    _PRIVATE_CONTENT_PREFIX + r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|" + _PRIVATE_CONTENT_SUFFIX,
    re.IGNORECASE,
)
NON_EMAIL_PRIVATE_CONTENT = re.compile(_PRIVATE_CONTENT_PREFIX + _PRIVATE_CONTENT_SUFFIX, re.IGNORECASE)


def require(condition, message):
    if not condition:
        raise ValueError(message)


PROGRESS_FIELDS = {
    "source_preflight_complete": set(),
    "repository_acquired": set(),
    "seed_start": set(),
    "fixture_preflight_complete": {"rows", "collections", "dimensions", "peers"},
    "collection_native_import_complete": {"collection", "rows"},
    "collection_native_copy_complete": {"collection", "rows"},
    "native_cache_installed": {"dimensions"},
    "sessions_seeded": {"sessions"},
    "native_cache_complete": {
        "dimensions",
        "peers",
        "configured_instruments",
        "captured_resolved_instruments",
    },
    "ready": set(),
    "sample_start": {"state"},
    "sample_completed": {"state"},
    "completed": set(),
}
PROGRESS_COLLECTIONS = {
    "mkt.klines_1m",
    "mkt.flow_bars_1m",
    "mkt.oi_obs",
    "mkt.funding_obs",
    "mkt.tickers_24h",
    "mkt.supply_obs",
    "reg.instruments",
    "reg.tokens",
    "ops.capture_tier_transitions",
    "mkt.capture_gaps",
}
PROGRESS_TIMES = {
    "wall_seconds",
    "process_cpu_seconds",
    "elapsed_wall_seconds",
    "elapsed_process_cpu_seconds",
}
INCOMPLETE = "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"


def validate_progress(value):
    require(isinstance(value, dict), "Invalid progress receipt")
    stage = value.get("stage")
    require(isinstance(stage, str) and stage in PROGRESS_FIELDS, "Unexpected progress stage")
    fields = PROGRESS_FIELDS[stage]
    require(
        set(value) == {"kind", "stage", "completion"} | PROGRESS_TIMES | fields,
        "Unexpected progress fields",
    )
    require(value["kind"] == "progress" and value["completion"] == INCOMPLETE, "Invalid progress status")
    for key in PROGRESS_TIMES:
        require(type(value[key]) in (int, float) and 0 <= value[key] <= 86400, "Invalid progress interval")
    for key in fields - {"collection", "state"}:
        require(type(value[key]) is int and 0 <= value[key] <= 1_000_000_000, "Invalid progress count")
    if "collection" in fields:
        require(value["collection"] in PROGRESS_COLLECTIONS, "Unexpected progress collection")
    if "state" in fields:
        require(value["state"] in {"cold", "warm"}, "Unexpected progress sample state")


class StageProgress:
    def __init__(self, emit):
        self.emit = emit
        self.started = self.previous = perf_counter()
        self.cpu = self.previous_cpu = process_time()
        self.count = 0

    def record(self, stage, **details):
        require(isinstance(stage, str) and stage in PROGRESS_FIELDS, "Unexpected progress stage")
        require(set(details) == PROGRESS_FIELDS[stage], "Unexpected progress fields")
        now, cpu = perf_counter(), process_time()
        value = {
            "kind": "progress",
            "stage": stage,
            "completion": INCOMPLETE,
            "wall_seconds": now - self.previous,
            "process_cpu_seconds": cpu - self.previous_cpu,
            "elapsed_wall_seconds": now - self.started,
            "elapsed_process_cpu_seconds": cpu - self.cpu,
            **details,
        }
        validate_progress(value)
        require(self.count < 24, "Progress receipts exceed their bound")
        self.emit(value)
        self.count += 1
        self.previous, self.previous_cpu = perf_counter(), process_time()


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


def _private_content(text):
    matcher = PRIVATE_CONTENT if "@" in text else NON_EMAIL_PRIVATE_CONTENT
    return bool(matcher.search(text))


def public_value(value, allowed=None):
    if isinstance(value, dict):
        require(allowed is None or set(value) <= allowed, "Unexpected recursive public field")
        for key, child in value.items():
            require(not _private_content(key), "Private public-fixture key")
            public_value(child, allowed)
    elif isinstance(value, list):
        for child in value:
            public_value(child, allowed)
    elif isinstance(value, str):
        require(not _private_content(value), "Private public-fixture value")
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


async def insert_clickhouse(ch, table, rows):
    from tank.market_schema import MARKET_IDENTITIES

    if not rows:
        return
    for row, _ in rows:
        require(
            isinstance(row.get("storage_key"), str) and row["storage_key"],
            "Missing actual storage key",
        )
        require(
            all(row.get(key) is not None for key in MARKET_IDENTITIES[table]),
            "Missing actual market identity",
        )
    if all(
        raw.isascii()
        and "\\" not in raw
        and all(type(value) in (str, int, bool, type(None)) for value in row.values())
        for row, raw in rows
    ):
        body = "\n".join(raw[:-1] + ',"_version":1}' for _, raw in rows)
        await ch.execute(f"INSERT INTO {table} FORMAT JSONEachRow\n{body}")
    else:
        await ch.insert(table, [{**row, "_version": 1} for row, _ in rows])


def clickhouse_wire_independent(rows):
    if all(
        raw.isascii()
        and "\\" not in raw
        and all(type(value) in (str, int, bool, type(None)) for value in row.values())
        for row, raw in rows
    ):
        body = "\n".join(raw[:-1] + ',"_version":1}' for _, raw in rows)
    else:
        body = "\n".join(json.dumps({**row, "_version": 1}) for row, _ in rows)
    return re.search(r"\bmkt\b", body) is None


def clickhouse_fixture_contract(path):
    tree = ast.parse(Path(path).read_text())
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    client = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ClickHouse")
    methods = {
        node.name: node for node in client.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    content = [
        keyword.value
        for node in ast.walk(methods["_post"])
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "post"
        for keyword in node.keywords
        if keyword.arg == "content"
    ]
    require(len(content) == 1, "Unknown ClickHouse wire contract")
    constants = {
        "MARKET_TABLES",
        "TRADE_COLUMNS",
        "TYPES",
        "ZSTD",
        "TIME_CODEC",
        "VERSION_CODEC",
        "CODECS",
        "TRADES_TTL_DAYS",
        "TRADES",
    }
    nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id in constants for target in node.targets)
    ]
    nodes += [functions["ddl"], *(methods[name] for name in ("__init__", "insert", "execute")), content[0]]
    return hashlib.sha256("\n".join(ast.dump(node) for node in nodes).encode()).hexdigest()


def fixture_admission(settings, fixtures):
    from tank import clickhouse_market, market_storage, models, storage

    fixtures = Path(fixtures)
    require(digest(fixtures / "manifest.json") == MANIFEST_SHA256, "Full fixture manifest mismatch")
    manifest = read_json((fixtures / "manifest.json").read_text())
    files = {}
    for entry in [*manifest["files"], *manifest["auxiliary_files"]]:
        path = fixtures / entry["name"]
        observed = digest(path)
        require(observed == entry.get("archive_sha256", entry["sha256"]), "Fixture admission hash mismatch")
        require(
            path.stat().st_size == entry.get("compressed_bytes", entry["bytes"]),
            "Fixture admission size mismatch",
        )
        files[entry["name"]] = observed
    return {
        "manifest_sha256": MANIFEST_SHA256,
        "privacy_sha256": digest(fixtures / "privacy.json"),
        "loader_sha256": digest(__file__),
        "storage_contracts": {
            module.__name__: digest(module.__file__) for module in (market_storage, models, storage)
        },
        "clickhouse_contract_sha256": clickhouse_fixture_contract(clickhouse_market.__file__),
        "schema_sha256": digest(Path(clickhouse_market.__file__).with_name("market_schema.py")),
        "fixture_files": files,
        "source_config": {
            filename: hashlib.sha256(settings.read_config_bytes(Path(filename).name)).hexdigest()
            for filename in manifest["source_config_git_objects"]
        },
    }


def validate_clickhouse_seed(seed, comparison_id, admission, manifest):
    from tank.clickhouse_market import MARKET_TABLES

    require(
        isinstance(seed, dict)
        and set(seed) == {"comparison_id", "admission", "source_database", "inputs", "copy_tables"},
        "Invalid ClickHouse seed descriptor",
    )
    require(re.fullmatch(r"[a-f0-9]{32}", comparison_id or ""), "Invalid comparison identity")
    require(seed["comparison_id"] == comparison_id, "Foreign comparison seed")
    require(seed["admission"] == admission, "Stale fixture admission")
    require(
        isinstance(seed["source_database"], str)
        and re.fullmatch(r"mkt_tank_test_fixture_[a-f0-9]{32}", seed["source_database"]),
        "Owned ClickHouse seed required",
    )
    entries = [entry for entry in manifest["files"] if entry["table"] in MARKET_TABLES]
    require(isinstance(seed["inputs"], list) and len(seed["inputs"]) == len(entries), "Partial seed inputs")
    for observed, entry in zip(seed["inputs"], entries, strict=True):
        require(
            all(observed.get(key) == entry[key] for key in ("name", "rows"))
            and observed.get("collection") == entry["table"],
            "Seed collection mismatch",
        )
        for key in ("sha256", "archive_sha256"):
            if key in entry:
                require(observed.get(key) == entry[key], "Seed collection identity mismatch")
    require(
        isinstance(seed["copy_tables"], dict)
        and set(seed["copy_tables"]) == {entry["table"] for entry in entries}
        and all(type(value) is bool for value in seed["copy_tables"].values()),
        "Invalid wire admission",
    )


async def copy_clickhouse(ch, source_database, table, expected_rows):
    from postgres_helpers import _finish

    from tank.clickhouse_market import MARKET_TABLES
    from tank.market_schema import MARKET_COLUMNS, MARKET_IDENTITIES

    for name in (source_database, ch.database):
        require(
            isinstance(name, str) and re.fullmatch(r"mkt_tank_test_fixture_[a-f0-9]{32}", name),
            "Owned ClickHouse copy database required",
        )
    require(source_database != ch.database, "Independent copy database required")
    require(urlsplit(ch.base).hostname in {"localhost", "127.0.0.1", "::1"}, "Loopback copy required")
    require(table in MARKET_TABLES, "Unknown copy collection")
    require(type(expected_rows) is int and expected_rows >= 0, "Invalid native row count")
    fields = ["storage_key", *MARKET_COLUMNS[table], "_version"]
    columns = ",".join(f"`{field}`" for field in fields)
    identity = ",".join(f"`{field}`" for field in MARKET_IDENTITIES[table])
    suffix = table.split(".")[1]
    source, target = f"{source_database}.{suffix}", f"{ch.database}.{suffix}"
    counted = (
        f"SELECT count() AS n, uniqExact(storage_key) AS keys, "
        f"uniqExact(tuple({identity})) AS identities, countIf(_version != 1) AS versions FROM "
    )
    expected = [{"n": expected_rows, "keys": expected_rows, "identities": expected_rows, "versions": 0}]

    async def materialize():
        schemas = []
        for database in (source_database, ch.database):
            params = {"database": (database, "String"), "table": (suffix, "String")}
            require(
                await ch.rows(
                    "SELECT engine FROM system.tables WHERE database={database:String} AND name={table:String}",
                    params,
                )
                == [{"engine": "ReplacingMergeTree"}],
                "Native copy requires physical market tables",
            )
            schema = await ch.rows(
                "SELECT name,type,default_kind,default_expression FROM system.columns "
                "WHERE database={database:String} AND table={table:String} ORDER BY position",
                params,
            )
            require([row["name"] for row in schema] == fields, "Native copy column mismatch")
            schemas.append(schema)
        require(schemas[0] == schemas[1], "Native copy schema mismatch")
        require(
            await ch.rows(f"SELECT count() AS n FROM {target} FINAL") == [{"n": 0}], "Copy target not empty"
        )
        require(
            await ch.rows(counted + source + " FINAL") == expected, "Native source count/identity mismatch"
        )
        await ch.execute(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {source} FINAL")
        require(await ch.rows(counted + target + " FINAL") == expected, "Native copy count/identity mismatch")
        for left, right in ((source, target), (target, source)):
            require(
                await ch.rows(
                    f"SELECT count() AS n FROM (SELECT {columns} FROM {left} FINAL "
                    f"EXCEPT DISTINCT SELECT {columns} FROM {right} FINAL)"
                )
                == [{"n": 0}],
                "Native full-column copy mismatch",
            )

    await _finish(materialize())


async def seed_full(
    repo,
    settings,
    fixtures,
    progress=None,
    *,
    clickhouse_only=False,
    clickhouse_seed=None,
    comparison_id=None,
):
    from psycopg.conninfo import conninfo_to_dict

    from tank import clickhouse_market
    from tank.market_schema import MARKET_COLUMNS, MARKET_IDENTITIES
    from tank.models import TABLE_COLUMNS

    checkpoint = progress or (lambda stage, **details: None)
    database = conninfo_to_dict(repo.database_url).get("dbname", "")
    require(re.fullmatch(r"tank_test_fixture_[a-f0-9]{32}", database), "Owned fixture database required")
    require(repo.clickhouse.database == "mkt_" + database, "Owned ClickHouse database required")
    schema_path = Path(clickhouse_market.__file__).with_name("market_schema.py")
    require(
        digest(schema_path) == "250a2cb262289908da7887754f8fa632d8a4f82ffec20d2dc485b9587ceb2823",
        "Original/current typed storage contract mismatch",
    )
    require(not (clickhouse_only and clickhouse_seed is not None), "Conflicting fixture modes")
    if clickhouse_seed is None:
        manifest, privacy = validate_fixture(fixtures)
    else:
        manifest = read_json((Path(fixtures) / "manifest.json").read_text())
        privacy = read_json((Path(fixtures) / "privacy.json").read_text())
        validate_clickhouse_seed(
            clickhouse_seed, comparison_id, fixture_admission(settings, fixtures), manifest
        )
    for filename, entry in manifest["source_config_git_objects"].items():
        require(
            hashlib.sha256(settings.read_config_bytes(Path(filename).name)).hexdigest() == entry["sha256"],
            "Original package config mismatch",
        )
    day_count_dimensions = len(native_counts(fixtures, manifest))
    cache_path = Path(settings.data_dir) / "market" / "kline_day_counts.json"
    require(not cache_path.exists(), "Native day-count cache already exists")
    checkpoint(
        "fixture_preflight_complete",
        rows=sum(entry["rows"] for entry in manifest["files"]),
        collections=len(manifest["files"]),
        dimensions=day_count_dimensions,
        peers=len(manifest["peer_spot_instrument_ids"]),
    )
    inventory, envelopes, copy_tables = [], [], {}
    for entry in manifest["files"]:
        captured = entry["table"]
        table = PUBLIC_TABLES.get(captured, captured)
        is_ch = table in clickhouse_market.MARKET_TABLES
        if clickhouse_only and not is_ch:
            continue
        if is_ch and clickhouse_seed is not None and clickhouse_seed["copy_tables"][table]:
            await copy_clickhouse(repo.clickhouse, clickhouse_seed["source_database"], table, entry["rows"])
            inventory.append(next(row for row in clickhouse_seed["inputs"] if row["collection"] == table))
            checkpoint("collection_native_copy_complete", collection=table, rows=entry["rows"])
            continue
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
        wire_independent = True
        for index, (row, raw) in enumerate(retained_rows(fixtures, entry, privacy, schema)):
            by_instrument[row.get("instrument_id") or row.get("token_id")] += 1
            missing.update(set(entry["columns"]) - set(row))
            if is_ch:
                batch.append((row, raw))
            else:
                additions = {}
                if captured in PUBLIC_TABLES:
                    key = row.get("instrument_id" if captured == "registry-resolved" else "token_id")
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
                    if clickhouse_only:
                        wire_independent &= clickhouse_wire_independent(batch)
                    await insert_clickhouse(repo.clickhouse, table, batch)
                else:
                    await insert_postgres(repo, table, batch, columns)
                batch.clear()
        if batch:
            if is_ch:
                if clickhouse_only:
                    wire_independent &= clickhouse_wire_independent(batch)
                await insert_clickhouse(repo.clickhouse, table, batch)
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
            observed_rows = counted[0]["n"]
        else:
            observed_rows = await postgres_count(repo, table)
            require(observed_rows == entry["rows"], "PostgreSQL typed count mismatch")
        checkpoint("collection_native_import_complete", collection=table, rows=observed_rows)
        if is_ch:
            copy_tables[table] = wire_independent
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
    checkpoint("native_cache_installed", dimensions=day_count_dimensions)
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
        "copy_tables": copy_tables if clickhouse_only else None,
    }
