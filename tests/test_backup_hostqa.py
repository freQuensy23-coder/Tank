"""Opt-in current backup/restore proof using four newly owned fixture databases."""

import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import zstandard
from psycopg import sql

from tank.scheduler import Scheduler
from tank.storage import MemoryRepository
from tank.util import canonical_json, sha256, utcnow


@pytest.mark.asyncio
async def test_current_pg_dump_bundles_restore_and_verification_cache(tmp_path, monkeypatch):
    admin = os.environ.get("TANK_TEST_DATABASE_URL")
    if admin != "postgresql://a.mametyev@127.0.0.1:55437/postgres":
        pytest.skip("Requires explicit isolated hostQA controller URL")
    binaries = Path("/tmp/tank-pg16/bin")
    if not (binaries / "pg_dump").exists():
        pytest.skip("Requires owned PostgreSQL 16 runtime")
    # This test never discovers operator configuration or reads credential files.
    monkeypatch.setenv("PATH", str(binaries) + ":/usr/bin:/bin")
    names = ["backup_fixture_" + uuid.uuid4().hex[:14] for _ in range(4)]
    urls = [admin.rsplit("/", 1)[0] + "/" + name for name in names]
    data = tmp_path / "data"
    bundles = data / "bundles"
    bundles.mkdir(parents=True)
    (data / "raw").mkdir()
    sentinel = data / "raw" / "nonsecret-sentinel.txt"
    sentinel.write_text("Synthetic raw fixture must remain outside the backup")
    payload = {"event_id": "fixture-backup", "claims": [], "source_items": [], "fixture": True}
    digest = sha256(payload)
    bundle = bundles / (digest + ".json.zst")
    bundle.write_bytes(zstandard.ZstdCompressor(level=3).compress(canonical_json(payload).encode()))
    backup = tmp_path / "backup"
    backup.mkdir()
    settings = SimpleNamespace(
        data_dir=data, backup_dir=backup, database_url=urls[0], openma_database_url=urls[1]
    )
    created = []
    try:
        with psycopg.connect(admin, autocommit=True) as conn:
            for name in names:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                created.append(name)
        for index, url in enumerate(urls[:2]):
            with psycopg.connect(url) as conn:
                conn.execute("CREATE TABLE fixture_record (id integer PRIMARY KEY, payload jsonb NOT NULL)")
                conn.execute(
                    "INSERT INTO fixture_record VALUES (%s, %s::jsonb)",
                    (index, json.dumps({"fixture": True, "database": index, "bundle_sha256": digest})),
                )
        scheduler = Scheduler(MemoryRepository(), settings, None, None)
        now = utcnow().replace(hour=8)
        result = await scheduler.maintenance(now, force=True, prune=False)
        assert result["status"] == "created"
        root = Path(result["path"])
        manifest = json.loads((root / "manifest.json").read_text())
        assert set(manifest["files"]) == {"tank.dump", "oma.dump", "bundles/" + bundle.name}
        assert {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()} == set(
            manifest["files"]
        ) | {"manifest.json", "COMPLETE", "COMPLETE.json"}
        assert not (root / "data").exists() and sentinel.is_file()
        restored_bundle = root / "bundles" / bundle.name
        assert json.loads(zstandard.ZstdDecompressor().decompress(restored_bundle.read_bytes())) == payload
        assert hashlib.sha256(canonical_json(payload).encode()).hexdigest() == digest
        for index, url in enumerate(urls[2:]):
            process = subprocess.run(
                [
                    str(binaries / "pg_restore"),
                    "--no-owner",
                    "--no-privileges",
                    "--dbname",
                    url,
                    str(root / ("tank.dump" if index == 0 else "oma.dump")),
                ],
                capture_output=True,
            )
            assert process.returncode == 0, "Fixture pg_restore failed"
            with psycopg.connect(url) as conn:
                rows = conn.execute("SELECT id,payload FROM fixture_record ORDER BY id").fetchall()
            assert rows == [(index, {"fixture": True, "database": index, "bundle_sha256": digest})]
        import tank.scheduler as scheduler_module

        original = scheduler_module.file_digest
        reads = []

        def counted(path, **kwargs):
            reads.append(Path(path).name)
            return original(path, **kwargs)

        monkeypatch.setattr(scheduler_module, "file_digest", counted)
        for _ in range(2):
            assert (await scheduler.maintenance(now, prune=False))["status"] == "verified"
        assert reads == [], "Warm same-day cache rehashed unchanged archive files"
        restarted = Scheduler(MemoryRepository(), settings, None, None)
        assert (await restarted.maintenance(now, prune=False))["status"] == "verified"
        assert sorted(reads) == sorted(["tank.dump", "oma.dump", bundle.name])
        print(
            "BACKUP_PROOF: current maintenance created pg_dump+bundles-only archive; both databases restored; bundle content hash matched; 2 warm checks=0 digest reads; restart=3 digest reads"
        )
    finally:
        with psycopg.connect(admin, autocommit=True) as conn:
            for name in created:
                conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
