"""Forward-only evidence-preserving transition to typed market storage."""

import asyncio
import hashlib
import json
import os
from datetime import UTC
from pathlib import Path

from tank.market_schema import MARKET_COLUMNS
from tank.util import jsonable


async def prepare_market_migration(repo, export_dir):
    """Freeze legacy writers; verify exported raw evidence before permitting SQL removal."""
    from psycopg import sql

    conn = repo._conn()
    tables = await (
        await conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='mkt' AND tablename NOT LIKE '%_default' AND tablename !~ '_[0-9]{8}$' ORDER BY tablename"
        )
    ).fetchall()
    for row in tables:
        await conn.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier("mkt", row["tablename"]))
        )
    exists = await (await conn.execute("SELECT to_regclass('mkt.trades') AS name")).fetchone()
    if not exists["name"]:
        return
    count = (await (await conn.execute("SELECT count(*) AS n FROM mkt.trades")).fetchone())["n"]
    if count:
        if not export_dir:
            raise RuntimeError("027 requires market_export_dir for verified legacy raw-trade Parquet export")
        import pyarrow as pa
        import pyarrow.parquet as pq

        root = Path(export_dir).resolve()
        root.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([("legacy_row_json", pa.string()), ("row_sha256", pa.string())])
        manifests = []
        writer = None
        current = None
        batch = []
        total = 0
        group_count = 0
        chain = None

        def flush():
            nonlocal batch
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch = []

        def seal():
            nonlocal writer
            if writer is None:
                return
            flush()
            writer.close()
            writer = None
            verified = hashlib.sha256()
            checked_count = 0
            for rb in pq.ParquetFile(staging).iter_batches(batch_size=500):
                for item in rb.to_pylist():
                    digest = hashlib.sha256(item["legacy_row_json"].encode()).hexdigest()
                    if digest != item["row_sha256"]:
                        raise RuntimeError("Legacy raw row checksum mismatch")
                    verified.update(bytes.fromhex(digest))
                    checked_count += 1
            if checked_count != group_count or verified.hexdigest() != chain.hexdigest():
                raise RuntimeError("Legacy raw export count/chain mismatch")
            with staging.open("rb") as stream:
                os.fsync(stream.fileno())
                file_hash = hashlib.file_digest(stream, "sha256").hexdigest()
            destination = root / (current + "-" + file_hash + ".parquet")
            staging.replace(destination)
            manifests.append(
                {
                    "path": str(destination),
                    "sha256": file_hash,
                    "row_count": group_count,
                    "row_chain_sha256": chain.hexdigest(),
                }
            )

        async with conn.cursor(name="market_raw_export") as cursor:
            await cursor.execute(
                "SELECT * FROM mkt.trades ORDER BY instrument_id,date_trunc('hour',ts AT TIME ZONE 'UTC'),ts,storage_key"
            )
            try:
                async for row in cursor:
                    group = (
                        hashlib.sha256(str(row.get("instrument_id")).encode()).hexdigest()[:16]
                        + "-"
                        + (row["ts"].astimezone(UTC).strftime("%Y%m%dT%H") if row.get("ts") else "UNKNOWN")
                    )
                    if group != current:
                        seal()
                        current = group
                        group_count = 0
                        chain = hashlib.sha256()
                        staging = root / (group + ".staging.parquet")
                        writer = pq.ParquetWriter(staging, schema)
                    encoded = json.dumps(jsonable(row), sort_keys=True, separators=(",", ":"))
                    digest = hashlib.sha256(encoded.encode()).hexdigest()
                    chain.update(bytes.fromhex(digest))
                    group_count += 1
                    total += 1
                    batch.append({"legacy_row_json": encoded, "row_sha256": digest})
                    if len(batch) == 500:
                        flush()
                seal()
            finally:
                if writer is not None:
                    writer.close()
        if total != count:
            raise RuntimeError("Legacy raw export row count changed while locked")
        manifest = {"format": "LEGACY_SQL_RAW_EVIDENCE_V1", "row_count": total, "files": manifests}
        manifest_text = json.dumps(manifest, sort_keys=True, indent=2)
        manifest_hash = hashlib.sha256(manifest_text.encode()).hexdigest()
        manifest_path = root / ("legacy-raw-" + manifest_hash + ".json")
        with manifest_path.open("w") as stream:
            stream.write(manifest_text)
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    await conn.execute("SELECT set_config('tank.market_raw_export_verified','yes',true)")
    if count:
        return {
            "export_id": manifest_hash,
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_hash,
            "row_count": total,
        }


LATER_MARKET_COLLECTIONS = {"mkt.capture_streams", "mkt.capture_requests"}


async def finish_market_migration(repo):
    from psycopg import sql

    conn = repo._conn()
    for collection in MARKET_COLUMNS:
        if collection in LATER_MARKET_COLLECTIONS:
            continue
        table = collection.split(".")[1]
        from tank.market_storage import _put_market_batch

        actual_columns = await (
            await conn.execute(
                "SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped",
                (collection,),
            )
        ).fetchall()
        available = {row["attname"] for row in actual_columns}
        target_columns = {
            name: kind for name, kind in MARKET_COLUMNS[collection].items() if name in available
        }
        async with conn.cursor(name="typed_copy_" + table) as cursor:
            await cursor.execute(
                sql.SQL("SELECT storage_key,document FROM {} ORDER BY storage_key").format(
                    sql.Identifier("archive", table)
                )
            )
            batch = []
            keys = []
            async for row in cursor:
                batch.append(row["document"])
                keys.append(row["storage_key"])
                if len(batch) == 500:
                    await _put_market_batch(repo, collection, batch, keys=keys, _columns=target_columns)
                    batch = []
                    keys = []
            if batch:
                await _put_market_batch(repo, collection, batch, keys=keys, _columns=target_columns)

        checks = await (
            await conn.execute(
                "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=%s::regclass AND contype='c'",
                ("archive." + table,),
            )
        ).fetchall()
        import re

        for check in checks:
            definition = check["definition"]
            definition = re.sub(
                r"document #>> '\{null_reasons,([^}]+)\}'::text\[\]", r"null_reasons ->> '\1'", definition
            )
            definition = re.sub(
                r"document#>>'\{null_reasons,([^}]+)\}'", r"null_reasons ->> '\1'", definition
            )
            if "document" in definition:
                raise RuntimeError(
                    f"Untranslated legacy market constraint: {table}.{check['conname']}: {definition}"
                )
            if not definition.endswith("NOT VALID"):
                definition += " NOT VALID"
            await conn.execute(
                sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                    sql.Identifier(*collection.split(".")),
                    sql.Identifier(check["conname"]),
                    sql.SQL(definition),
                )
            )
        relatives = await (
            await conn.execute(
                "WITH RECURSIVE tree AS (SELECT %s::regclass::oid AS oid UNION ALL SELECT i.inhrelid FROM pg_inherits i JOIN tree t ON i.inhparent=t.oid) SELECT oid::regclass::text AS relation FROM tree",
                ("archive." + table,),
            )
        ).fetchall()
        for relative in relatives:
            await conn.execute(
                sql.SQL(
                    "CREATE TRIGGER frozen_legacy_market BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION ops.protect_market_archive()"
                ).format(sql.SQL(relative["relation"]))
            )
        await conn.execute(
            sql.SQL("REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON {} FROM PUBLIC").format(
                sql.Identifier("archive", table)
            )
        )

    await conn.execute(
        "SELECT setval('mkt.liq_obs_id_seq',greatest(coalesce((SELECT max(id) FROM mkt.liq_obs),0)+1,1),false)"
    )


def read_verified_legacy_raw(manifest):
    """Verify the complete migration manifest before streaming original SQL evidence rows.

    Returns each full original row (including its original document), never a fabricated
    normalized trade. Consumers must retain provenance when rebuilding Parquet aggregates.
    """
    import pyarrow.parquet as pq

    if isinstance(manifest, (str, Path)):
        manifest_path = Path(manifest)
        raw = manifest_path.read_bytes()
        expected = manifest_path.stem.removeprefix("legacy-raw-")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("Legacy raw manifest filename checksum mismatch")
        manifest = json.loads(raw)
    if manifest.get("format") != "LEGACY_SQL_RAW_EVIDENCE_V1":
        raise ValueError("Unsupported legacy raw evidence format")
    total = 0
    for entry in manifest["files"]:
        path = Path(entry["path"])
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != entry["sha256"]:
                raise ValueError("Legacy raw file checksum mismatch")
        chain = hashlib.sha256()
        count = 0
        for batch in pq.ParquetFile(path).iter_batches(batch_size=500):
            for row in batch.to_pylist():
                digest = hashlib.sha256(row["legacy_row_json"].encode()).hexdigest()
                if digest != row["row_sha256"]:
                    raise ValueError("Legacy raw row checksum mismatch")
                chain.update(bytes.fromhex(digest))
                count += 1
        if count != entry["row_count"] or chain.hexdigest() != entry["row_chain_sha256"]:
            raise ValueError("Legacy raw file row count/chain mismatch")
        total += count
    if total != manifest["row_count"]:
        raise ValueError("Legacy raw manifest total count mismatch")
    for entry in manifest["files"]:
        for batch in pq.ParquetFile(entry["path"]).iter_batches(batch_size=500):
            for row in batch.to_pylist():
                yield json.loads(row["legacy_row_json"])


def verify_legacy_table_bundle(path, *, sha256, collection, row_count, row_chain_sha256):
    """Read every compressed legacy row back before SQL removal is authorized."""
    import io

    import zstandard

    path = Path(path)
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != sha256:
            raise ValueError("Legacy table bundle file checksum mismatch")
    count, chain = 0, hashlib.sha256()
    with (
        path.open("rb") as source,
        zstandard.ZstdDecompressor().stream_reader(source) as decoded,
        io.TextIOWrapper(decoded, encoding="utf-8") as rows,
    ):
        header = rows.readline()
        marker = ',"rows":[\n'
        if not header.endswith(marker):
            raise ValueError("Legacy table bundle header is malformed")
        metadata = json.loads(header.removesuffix(marker) + "}")
        if metadata.get("format") != "LEGACY_SQL_TABLE_V1" or metadata.get("collection") != collection:
            raise ValueError("Legacy table bundle identity mismatch")
        if metadata.get("row_count") != row_count:
            raise ValueError("Legacy table bundle declared count mismatch")
        for line in rows:
            if line == "]}\n":
                if rows.read():
                    raise ValueError("Legacy table bundle has trailing content")
                break
            encoded = json.loads(line.rstrip("\n").removesuffix(","))
            if not isinstance(encoded, str) or not isinstance(json.loads(encoded), dict):
                raise TypeError("Legacy table bundle contains a malformed SQL row")
            chain.update(hashlib.sha256(encoded.encode()).digest())
            count += 1
        else:
            raise ValueError("Legacy table bundle is truncated")
    if count != row_count or chain.hexdigest() != row_chain_sha256:
        raise ValueError("Legacy table bundle count/chain mismatch")
    return metadata


ARCHIVE_CLEANUP_TABLES = {
    "032_remove_verified_legacy_archive.sql": (
        "ops_budget_state",
        "funding_obs",
        "supply_obs",
        "supply",
        "unlocks",
        "hot_tokens",
        "capture_state",
        "capture_gaps",
        "raw_archives",
        "backfill_runs",
    ),
    "033_remove_legacy_price_flow.sql": ("flow_bars_1m", "klines_1m"),
    "034_remove_legacy_derivatives.sql": ("oi_obs", "liq_obs"),
    "035_remove_legacy_books_tickers.sql": ("book_summaries", "tickers_24h"),
}


async def prepare_archive_cleanup(repo, bundle_dir, version):
    """Freeze and export retired SQL tables; never discard historical JSON documents.

    A populated installation needs an explicit permanent bundles/ directory. Fresh
    installs with empty retired tables require no export. SQL032 only accepts the
    transaction-local authorization set after every file has passed full readback.
    """
    import tempfile

    import zstandard
    from psycopg import sql

    conn = repo._conn()
    tables = await (
        await conn.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='archive' AND c.relkind IN ('r','p') "
            "AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid) AND c.relname=ANY(%s) ORDER BY c.relname",
            (list(ARCHIVE_CLEANUP_TABLES[version]),),
        )
    ).fetchall()

    for row in tables:
        await conn.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                sql.Identifier("archive", row["relname"])
            )
        )
    exported = []
    for row in tables:
        table = row["relname"]
        identity = "archive." + table
        count = (
            await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier("archive", table))
                )
            ).fetchone()
        )["n"]
        if not count:
            continue
        if bundle_dir is None:
            raise RuntimeError(
                version[:3] + " requires legacy_bundle_dir for verified permanent archive-schema export"
            )
        root = Path(bundle_dir).resolve()
        if root.name != "bundles":
            raise ValueError("Legacy SQL evidence must be exported under the planned bundles/ root")
        root.mkdir(parents=True, exist_ok=True)
        columns = await (
            await conn.execute(
                "SELECT a.attname AS name,format_type(a.atttypid,a.atttypmod) AS type "
                "FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
                (identity,),
            )
        ).fetchall()
        metadata = {
            "format": "LEGACY_SQL_TABLE_V1",
            "collection": identity,
            "row_count": count,
            "columns": columns,
        }
        chain, written = hashlib.sha256(), 0
        with tempfile.NamedTemporaryFile(
            dir=root, prefix=".legacy-sql-", suffix=".pending", delete=False
        ) as staging:
            staging_path = Path(staging.name)
            try:
                with zstandard.ZstdCompressor(level=3).stream_writer(staging, closefd=False) as compressor:
                    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).removesuffix("}")
                    compressor.write((header + ',"rows":[\n').encode())
                    async with conn.cursor(name="archive_export_" + table) as cursor:
                        await cursor.execute(
                            sql.SQL(
                                "SELECT to_jsonb(t)::text AS original FROM {} t ORDER BY storage_key"
                            ).format(sql.Identifier("archive", table))
                        )
                        async for item in cursor:
                            encoded = item["original"]
                            chain.update(hashlib.sha256(encoded.encode()).digest())
                            if written:
                                compressor.write(b",\n")
                            compressor.write(json.dumps(encoded, ensure_ascii=False).encode())
                            written += 1
                    compressor.write(b"\n]}\n")
                staging.flush()
                os.fsync(staging.fileno())
                if written != count:
                    raise RuntimeError("Legacy table export count changed while locked")

                def file_digest(path=staging_path):
                    with path.open("rb") as source:
                        return hashlib.file_digest(source, "sha256").hexdigest()

                digest = await asyncio.to_thread(file_digest)

                entry = {
                    "collection": identity,
                    "sha256": digest,
                    "row_count": written,
                    "row_chain_sha256": chain.hexdigest(),
                }
                await asyncio.to_thread(verify_legacy_table_bundle, staging_path, **entry)
                destination = root / (digest + ".json.zst")
                if destination.exists():
                    await asyncio.to_thread(verify_legacy_table_bundle, destination, **entry)
                    staging_path.unlink()
                else:
                    staging_path.replace(destination)
                descriptor = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                entry["path"] = str(destination)
                exported.append(entry)
            except BaseException:
                staging_path.unlink(missing_ok=True)
                raise
    manifest = {
        "format": "LEGACY_SQL_ARCHIVE_MANIFEST_V1",
        "migration": version,
        "tables": exported,
        "row_count": sum(r["row_count"] for r in exported),
    }
    if exported:
        body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(body).hexdigest()
        destination = root / (digest + ".json.zst")
        encoded = zstandard.ZstdCompressor(level=3).compress(body)
        with tempfile.NamedTemporaryFile(
            dir=root, prefix=".legacy-manifest-", suffix=".pending", delete=False
        ) as staging:
            stage = Path(staging.name)
            try:
                staging.write(encoded)
                staging.flush()
                os.fsync(staging.fileno())
                if zstandard.ZstdDecompressor().decompress(stage.read_bytes()) != body:
                    raise RuntimeError("Legacy archive manifest readback mismatch")
                if (
                    destination.exists()
                    and zstandard.ZstdDecompressor().decompress(destination.read_bytes()) != body
                ):
                    raise RuntimeError("Legacy archive manifest destination differs")
                stage.replace(destination)
                descriptor = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except BaseException:
                stage.unlink(missing_ok=True)
                raise
        from tank.time_precision import collection_clock

        receipt = {
            "export_id": digest,
            "manifest_path": str(destination),
            "manifest_sha256": digest,
            "row_count": manifest["row_count"],
            "format": manifest["format"],
            "tables": exported,
            **collection_clock().fields("created_at"),
        }
        await repo.put("ops.market_migration_exports", digest, receipt, immutable=True)
    await conn.execute("SELECT set_config('tank.legacy_archive_export_verified',%s,true)", (version,))
    return manifest
