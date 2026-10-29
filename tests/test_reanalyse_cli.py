"""The operator re-analysis command lists by default and queues only with --apply."""

import asyncio
import copy
import json

import pytest
from click.testing import CliRunner

from tank import cli
from tank.agents.incomplete import AGENT_INCOMPLETE, LEGACY_INCOMPLETE, SESSION_FAILED
from tank.storage import MemoryRepository

ITEMS = {
    "ITM-incomplete": {"extraction_status": "FAILED", "extraction_reason": AGENT_INCOMPLETE},
    "ITM-legacy": {"extraction_status": "SKIPPED", "extraction_reason": LEGACY_INCOMPLETE},
    "ITM-session-failed": {"extraction_status": "FAILED", "extraction_reason": SESSION_FAILED},
    "ITM-reminded-failed": {"extraction_status": "FAILED", "extraction_reason": "AGENT_REMINDED"},
    "ITM-rejected": {
        "extraction_status": "SKIPPED",
        "extraction_decision": "NO_EVENT",
        "extraction_reason": "Мнение без нового фактического утверждения",
    },
    "ITM-filtered": {"extraction_status": "SKIPPED"},
    "ITM-crashed": {"extraction_status": "FAILED"},
    "ITM-done": {"extraction_status": "DONE"},
    "ITM-pending": {"extraction_status": "PENDING"},
}
ELIGIBLE = ["ITM-incomplete", "ITM-legacy", "ITM-session-failed", "ITM-reminded-failed"]


@pytest.fixture
def repo():
    repo = MemoryRepository()
    for n, (item_id, state) in enumerate(ITEMS.items()):
        asyncio.run(
            repo.put(
                "ingest.source_items",
                item_id,
                {"item_id": item_id, "observed_at": f"2026-10-28T10:0{n}:00Z", **state},
            )
        )
    return repo


def invoke(monkeypatch, repo, *arguments):
    monkeypatch.setattr(cli, "run_db", lambda fn: cli.output(asyncio.run(fn(repo, None))))
    result = CliRunner().invoke(cli.main, ["reanalyse", *arguments])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def stored(repo):
    return {item_id: asyncio.run(repo.get("ingest.source_items", item_id)) for item_id in ITEMS}


def jobs(repo):
    return sorted((job["type"], job["idem_key"]) for job in asyncio.run(repo.list("ops.jobs")))


def test_dry_run_is_the_default_and_changes_nothing(monkeypatch, repo):
    before = copy.deepcopy(stored(repo))
    listed = invoke(monkeypatch, repo)
    assert (listed["apply"], listed["count"]) == (False, len(ELIGIBLE))
    assert [row["item_id"] for row in listed["items"]] == ELIGIBLE
    assert all("queued" not in row for row in listed["items"])
    assert stored(repo) == before
    assert jobs(repo) == []


def test_apply_requeues_only_incomplete_items(monkeypatch, repo):
    before = copy.deepcopy(stored(repo))
    applied = invoke(monkeypatch, repo, "--apply")
    assert [(row["item_id"], row["queued"]) for row in applied["items"]] == [(i, True) for i in ELIGIBLE]
    after = stored(repo)
    for item_id in ELIGIBLE:
        assert after[item_id]["extraction_status"] == "PENDING"
        assert after[item_id]["extraction_reason"] is None
        assert (
            after[item_id]["reanalysis_history"][0]["previous_status"] == before[item_id]["extraction_status"]
        )
    for item_id in set(ITEMS) - set(ELIGIBLE):
        assert after[item_id] == before[item_id]
    assert jobs(repo) == sorted(("ROUTE_ITEM", f"{item_id}:reanalysis:1") for item_id in ELIGIBLE)


def test_applying_twice_does_not_enqueue_again(monkeypatch, repo):
    invoke(monkeypatch, repo, "--apply")
    queued = jobs(repo)
    again = invoke(monkeypatch, repo, "--apply")
    assert again["count"] == 0 and again["items"] == []
    assert jobs(repo) == queued


def test_limit_and_item_ids_narrow_the_selection(monkeypatch, repo):
    limited = invoke(monkeypatch, repo, "--limit", "1", "--apply")
    assert [row["item_id"] for row in limited["items"]] == ["ITM-incomplete"]
    assert jobs(repo) == [("ROUTE_ITEM", "ITM-incomplete:reanalysis:1")]
    chosen = invoke(monkeypatch, repo, "--item-id", "ITM-rejected", "--item-id", "ITM-legacy", "--apply")
    assert [row["item_id"] for row in chosen["items"]] == ["ITM-legacy"]
    assert stored(repo)["ITM-rejected"]["extraction_status"] == "SKIPPED"
    assert len(jobs(repo)) == 2


def test_limit_must_be_positive(monkeypatch, repo):
    monkeypatch.setattr(cli, "run_db", lambda fn: pytest.fail("must not reach the database"))
    result = CliRunner().invoke(cli.main, ["reanalyse", "--limit", "0"])
    assert result.exit_code != 0
