import gzip
import hashlib
import json
from pathlib import Path

DATA_REVISION = "ebf1398a869359da7d628b871f618be682c861b0"
ORIGINAL = "ff6512a4ac2107319effc8e7745ee898a47c6212"
MANIFEST_SHA256 = "a4f924bafc62ca2d422da7245e2a4cade6b011fd87e864ea998edd07660c9285"
PUBLIC_TABLES = {
    "registry-resolved": "reg.instruments",
    "registry-tokens": "reg.tokens",
    "capture-tier-public": "ops.capture_tier_transitions",
    "capture-gaps-public": "mkt.capture_gaps",
}


def stage_data(directory):
    assert hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest() == MANIFEST_SHA256
    manifest = json.loads((directory / "manifest.json").read_bytes())
    for entry in manifest["files"] + manifest["auxiliary_files"]:
        path = directory / entry["name"]
        assert path.parent == directory and not path.is_symlink()
        raw = path.read_bytes()
        assert len(raw) == entry.get("compressed_bytes", entry["bytes"])
        assert hashlib.sha256(raw).hexdigest() == entry.get("archive_sha256", entry["sha256"])
        if path.suffix == ".gz":
            checksum, size, count = hashlib.sha256(), 0, 0
            with gzip.open(path, "rb") as stream:
                while block := stream.read(1 << 20):
                    checksum.update(block)
                    size += len(block)
                    count += block.count(b"\n")
            assert (size, count, checksum.hexdigest()) == (entry["bytes"], entry["rows"], entry["sha256"])
    return manifest


async def seed_data(repo, settings, directory, manifest):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    from tank import clickhouse_market as chm
    from tank.market_schema import MARKET_IDENTITIES
    from tank.models import TABLE_COLUMNS

    native = {}
    for filename, entry in manifest["source_config_git_objects"].items():
        assert hashlib.sha256((settings.config_dir / Path(filename).name).read_bytes()).hexdigest() == entry["sha256"]
    for entry in manifest["files"]:
        table = PUBLIC_TABLES.get(entry["table"], entry["table"])
        path = directory / entry["name"]
        if table in chm.MARKET_TABLES:
            ch = repo.clickhouse

            async def chunks():
                with path.open("rb") as stream:
                    while block := stream.read(1 << 20):
                        yield block

            response = await ch.http.post(
                ch.base, params={"query": f"INSERT INTO {table.replace('mkt.', ch.database + '.')} FORMAT JSONEachRow"},
                headers={"X-ClickHouse-User": ch.user, "X-ClickHouse-Key": ch.password, "Content-Encoding": "gzip"},
                content=chunks(),
            )
            assert response.status_code == 200, "Native gzip import refused"
            columns = ["storage_key", *TABLE_COLUMNS[table]]
            identity = ",".join(f"`{name}`" for name in MARKET_IDENTITIES[table])
            fields = ",".join(f"`{name}`" for name in columns)
            observed = await ch.rows(
                f"SELECT count() AS n, uniqExact(storage_key) AS keys, uniqExact(tuple({identity})) AS identities, "
                f"groupBitXor(cityHash64(tuple({fields}))) AS digest FROM {table} FINAL"
            )
            assert observed[0]["n"] == observed[0]["keys"] == observed[0]["identities"] == entry["rows"]
            native[table] = observed[0]
            with gzip.open(path, "rb") as stream:
                first = json.loads(next(stream))
            selected = await ch.rows(f"SELECT * FROM {table} FINAL WHERE storage_key={{key:String}}", {"key": (first["storage_key"], "String")})
            assert len(selected) == 1 and selected[0]["_version"] == 0
            await ch.execute(f"CREATE TABLE mkt.request_wire_probe AS {table}")
            try:
                decoded = chm._record(table, selected[0])
                await ch.insert("mkt.request_wire_probe", [chm._row(table, decoded, first["storage_key"], 0)])
                typed = await ch.rows("SELECT * FROM mkt.request_wire_probe FINAL")
                assert chm._records(table, typed) == chm._records(table, selected)
                assert chm._record(table, first) == decoded
            finally:
                await ch.execute("DROP TABLE mkt.request_wire_probe")
        else:
            with gzip.open(path, "rb") as stream:
                rows = [json.loads(line) for line in stream]
            for index, row in enumerate(rows):
                if entry["table"] in PUBLIC_TABLES:
                    key = row.get("instrument_id" if entry["table"] == "registry-resolved" else "token_id")
                    if entry["table"].startswith("capture-"):
                        key = f"public:{entry['table']}:{index}"
                        row["transition_id" if entry["table"] == "capture-tier-public" else "gap_id"] = key
                    row["storage_key"] = key
                if table not in chm.MARKET_COLUMNS:
                    row.update(document=dict(row), immutable=False)
            columns = ["storage_key", *TABLE_COLUMNS[table]]
            if table not in chm.MARKET_COLUMNS:
                columns += ["document", "immutable"]
            names = sql.SQL(",").join(map(sql.Identifier, columns))
            target = sql.Identifier(*table.split("."))
            async with repo.transaction():
                await repo._conn().execute(
                    sql.SQL("INSERT INTO {} ({}) SELECT {} FROM jsonb_populate_recordset(NULL::{}, %s)").format(target, names, names, target),
                    (Jsonb(rows),),
                )
                counted = await (await repo._conn().execute(sql.SQL("SELECT count(*) AS n FROM {}").format(target))).fetchone()
            assert counted["n"] == entry["rows"]
            native[table] = {"n": counted["n"]}
    for table in manifest["confirmed_empty_tables"]:
        assert await repo.clickhouse.rows(f"SELECT count() AS n FROM {table} FINAL") == [{"n": 0}]
    cache = settings.data_dir / "market/kline_day_counts.json"
    cache.parent.mkdir(parents=True)
    cache.write_bytes((directory / "kline_day_counts.json").read_bytes())
    return native
