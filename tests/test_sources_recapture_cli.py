"""The operator recapture command lists skipped duplicates by default and releases them only with --apply."""

import asyncio
import json
from types import SimpleNamespace

from click.testing import CliRunner

from tank import cli
from tank.storage import MemoryRepository

OCTOBER = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20261028a.htm"
SOURCES = {
    "sources": [{"source_id": "S05", "feeds": [{"url": "https://www.federalreserve.gov/feeds/a.xml"}]}]
}


def invoke(monkeypatch, repo, *arguments):
    config = SimpleNamespace(load_config=lambda name: SOURCES)
    monkeypatch.setattr(cli, "run_db", lambda fn: cli.output(asyncio.run(fn(repo, config))))
    return CliRunner().invoke(cli.main, ["sources", "recapture", *arguments])


def test_recapture_dry_run_then_apply(monkeypatch):
    repo = MemoryRepository()
    state = {"source_id": "S05", "duplicate_ids": [OCTOBER], "last_modified": "Wed, 28 Oct 2026 18:00:15 GMT"}
    asyncio.run(repo.put("ops.source_checkpoints", "S05:0", state))

    listed = invoke(monkeypatch, repo, "S05", OCTOBER)
    assert listed.exit_code == 0, listed.output
    assert json.loads(listed.output)["checkpoints"] == [
        {"checkpoint": "S05:0", "external_ids": [OCTOBER], "released": False}
    ]
    assert asyncio.run(repo.get("ops.source_checkpoints", "S05:0"))["duplicate_ids"] == [OCTOBER]

    applied = invoke(monkeypatch, repo, "S05", OCTOBER, "--apply")
    assert json.loads(applied.output)["apply"] is True
    after = asyncio.run(repo.get("ops.source_checkpoints", "S05:0"))
    assert after["duplicate_ids"] == [] and "last_modified" not in after

    assert invoke(monkeypatch, repo, "S99", OCTOBER).exit_code != 0
    assert invoke(monkeypatch, repo, "S05").exit_code != 0
