"""Read-only final guarded-retirement receipt; no source/event/queue mutation."""

import hashlib
import io
import json
import shutil
from pathlib import Path

import psycopg
import zstandard
from psycopg import sql
from verify_restore import verify_live

from tank.market_migration import verify_legacy_table_bundle
from tank.settings import Settings

root = Path("/data/bundles")
out = Path("/qa")
baseline = json.loads((out / "proof/tank.json").read_text())
archive = {
    name: record["rows"]
    for name, record in baseline["tables"].items()
    if name.startswith("archive.") and record["rows"]
}
manifests = []
for path in root.glob("*.json.zst"):
    with (
        path.open("rb") as source,
        zstandard.ZstdDecompressor().stream_reader(source) as decoded,
        io.TextIOWrapper(decoded, encoding="utf-8") as rows,
    ):
        line = rows.readline(65537).encode()
    if len(line) > 65536:
        continue
    if line.endswith(b',"rows":[\n'):
        continue
    record = json.loads(line)
    if record.get("format") != "LEGACY_SQL_ARCHIVE_MANIFEST_V1":
        continue
    assert hashlib.sha256(line).hexdigest() == path.name.removesuffix(".json.zst"), (
        "Manifest content checksum mismatch"
    )
    manifests.append(record)
assert len(manifests) == 4, "Expected four retirement manifests"
exports = {}
for manifest in manifests:
    for entry in manifest["tables"]:
        arguments = {key: value for key, value in entry.items() if key != "path"}
        metadata = verify_legacy_table_bundle(entry["path"], **arguments)
        name = entry["collection"]
        assert name not in exports and archive[name] == entry["row_count"], (
            "Archive original row count mismatch"
        )
        exports[name] = {
            "rows": entry["row_count"],
            "sha256": entry["sha256"],
            "path": entry["path"],
            "bytes": Path(entry["path"]).stat().st_size,
            "full_readback": True,
        }
assert set(exports) == set(archive), "Missing original nonempty archive table export"
domain = {
    name: dict(record)
    for name, record in baseline["tables"].items()
    if not name.startswith(("archive.", "mkt.")) and name != "ops.schema_migrations"
}
assert domain["out.alerts"]["rows"] == 0
domain["out.alerts"]["columns"] = [name for name in domain["out.alerts"]["columns"] if name != "linter"]
domain["out.alerts"]["hash_columns"] = domain["out.alerts"]["columns"]
assert verify_live(domain, baseline["sequences"]) == [], "Domain/job/session/sequence state changed"
with psycopg.connect(Settings().database_url, autocommit=True) as connection:
    connection.execute("SET default_transaction_read_only=on")
    versions = [
        row[0] for row in connection.execute("SELECT version FROM ops.schema_migrations ORDER BY version")
    ]
    expected = [p.name for p in sorted(Path("/app/src/tank/migrations").glob("*.sql"))]
    assert versions == expected and len(versions) == 35, "Migration ledger mismatch"
    assert connection.execute("SELECT to_regnamespace('archive')").fetchone()[0] is None, (
        "Archive schema remains"
    )
    assert (
        connection.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema='out' AND table_name='alerts' AND column_name='linter'"
        ).fetchone()[0]
        == 0
    ), "Retired projection remains"
    market = {}
    for name, record in baseline["tables"].items():
        if not name.startswith("mkt."):
            continue
        schema, table = name.split(".")
        count = connection.execute(
            sql.SQL("SELECT count(*) FROM ONLY {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
        ).fetchone()[0]
        assert count == record["rows"], "Typed market count changed"
        market[name] = count
    size = connection.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    assert connection.execute("SHOW max_locks_per_transaction").fetchone()[0] == "64"
report = {
    "passed": True,
    "ledger": versions,
    "archive_absent": True,
    "linter_column_absent": True,
    "exported_tables": exports,
    "exported_rows": sum(r["rows"] for r in exports.values()),
    "all_nonempty_archive_originals_preserved": True,
    "domain_jobs_sessions_fingerprints_unchanged": True,
    "sequences_unchanged": True,
    "typed_market_table_counts_unchanged": True,
    "market_counted_tables": len(market),
    "pre_migration_market_full_row_hash_proof": True,
    "post_migration_market_full_row_hash_repeated": False,
    "new_raw_market_frames_path_exists": Path("/data/raw/market_frames").exists(),
    "pg_database_bytes": size,
    "free_bytes": shutil.disk_usage("/data").free,
    "runtime_started": False,
    "market_warming_started": False,
}
assert not report["new_raw_market_frames_path_exists"]
p = out / "post-migration-proof.json"
p.write_text(json.dumps(report, indent=2) + "\n")
p.chmod(0o600)
print(
    json.dumps({key: value for key, value in report.items() if key not in {"ledger", "exported_tables"}}),
    flush=True,
)
