"""The operator re-analysis command lists by default and queues only with --apply."""

import asyncio
import json

from click.testing import CliRunner

from tank import cli
from tank.agents.incomplete import AGENT_INCOMPLETE
from tank.storage import MemoryRepository


def invoke(monkeypatch, repo, *arguments):
    monkeypatch.setattr(cli, "run_db", lambda fn: cli.output(asyncio.run(fn(repo, None))))
    result = CliRunner().invoke(cli.main, ["reanalyse", *arguments])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_reanalyse_is_a_dry_run_unless_applied(monkeypatch):
    repo = MemoryRepository()
    for item_id, status, reason in [
        ("ITM-a", "FAILED", AGENT_INCOMPLETE),
        ("ITM-b", "SKIPPED", "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"),
        ("ITM-c", "SKIPPED", "Мнение"),
    ]:
        asyncio.run(
            repo.put(
                "ingest.source_items",
                item_id,
                {"item_id": item_id, "extraction_status": status, "extraction_reason": reason},
            )
        )

    listed = invoke(monkeypatch, repo)
    assert (listed["apply"], listed["count"]) == (False, 2)
    assert [row["item_id"] for row in listed["items"]] == ["ITM-a", "ITM-b"]
    assert asyncio.run(repo.list("ops.jobs")) == []

    limited = invoke(monkeypatch, repo, "--limit", "1", "--apply")
    assert [(row["item_id"], row["queued"]) for row in limited["items"]] == [("ITM-a", True)]
    assert [job["idem_key"] for job in asyncio.run(repo.list("ops.jobs"))] == ["ITM-a:reanalysis:1"]

    chosen = invoke(monkeypatch, repo, "--item-id", "ITM-c", "--item-id", "ITM-b", "--apply")
    assert [row["item_id"] for row in chosen["items"]] == ["ITM-b"]
    assert asyncio.run(repo.get("ingest.source_items", "ITM-c"))["extraction_status"] == "SKIPPED"
    assert asyncio.run(repo.get("ingest.source_items", "ITM-b"))["extraction_status"] == "PENDING"
