import importlib
import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_sources import Repo, settings

from tank.sources import SourceService
from tank.sources.xsensor import ApifyRun, scan

FIXTURES = Path(__file__).parent / "fixtures" / "x-cursor" / "retained-accounts.json"
GAPS = [
    ("vy51uN2gcSkQeaRky", "JSgsP4TYqyEJRRHQS", "S1HzYGGLo1xh7D5eK"),
    ("G3tmcnKdFoSg0hh24", "d65MxHTtpILHfRyJm", "eUjvPXqb1N7ou8Dv8"),
    ("kDlbmzHpGBOxqQqIZ", "1FXDuSk4EdNCCzckj", "BuRFULQL6ZDmZgAKd"),
]


@pytest.fixture
def retained():
    return json.loads(FIXTURES.read_text())


@pytest.fixture
def native_reader(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "apify" / "x-reader"))
    return importlib.import_module("public_profile"), importlib.import_module("direct_fetch")


def profile_sequence(monkeypatch, reader, snapshots):
    pending = iter(deepcopy(snapshots))

    async def fetch_profile(handle, deadline, evidence_limit):
        snapshot = next(pending)
        assert snapshot["account"]["handle"] == handle
        return snapshot["posts"], snapshot["account"]["wireBytes"]

    monkeypatch.setattr(reader, "fetch_profile", fetch_profile)


def retained_run(snapshot):
    account = snapshot["account"]
    return ApifyRun.model_validate(
        {
            "health": "degraded" if account.get("historyGap") else "healthy",
            "posts": snapshot["posts"],
            "accounts": [account],
            "nextCursors": {account["handle"]: snapshot["nextCursor"]},
        }
    )


@pytest.mark.parametrize("prior_id,gap_id,repeat_id", GAPS)
async def test_native_actor_keeps_missing_boundary_pending_on_identical_samples(
    monkeypatch, native_reader, retained, prior_id, gap_id, repeat_id
):
    reader, worker = native_reader
    prior, gap, repeat = [retained[key] for key in (prior_id, gap_id, repeat_id)]
    handle = prior["account"]["handle"]
    cursor = prior["account"]["lastMaxId"]
    profile_sequence(monkeypatch, reader, [gap, repeat])
    for _ in range(2):
        output = await worker.fetch_snapshot({"handles": [handle], "cursors": {handle: cursor}})
        assert output["posts"] == gap["posts"]
        assert output["accounts"][0]["lastMaxId"] == gap["account"]["lastMaxId"]
        assert output["health"] == "degraded" and output["coverageGaps"] == [handle]
        assert output["accounts"][0]["historyGap"]
        assert output["nextCursors"] == {handle: cursor}
        cursor = output["nextCursors"][handle]


@pytest.mark.parametrize("prior_id,gap_id,repeat_id", GAPS)
def test_consumer_holds_cursor_for_retained_legacy_gap_outputs(retained, prior_id, gap_id, repeat_id):
    prior, gap = retained[prior_id], retained[gap_id]
    handle = prior["account"]["handle"]
    cursor = prior["account"]["lastMaxId"]
    result = scan(retained_run(gap), [handle], {handle: cursor}, b"retained-account-projection")
    assert [row["external_id"] for row in result.rows] == [gap["account"]["lastMaxId"]]
    assert result.gaps == [handle] and result.failures == []
    assert result.cursors == {handle: cursor}


async def test_native_stale_sample_remains_failed_until_a_boundary_covering_sample(
    monkeypatch, native_reader, retained
):
    reader, worker = native_reader
    older = retained["1FXDuSk4EdNCCzckj"]
    newer = retained["om0fAvrciMOvsZQtL"]
    cursor = newer["account"]["lastMaxId"]
    profile_sequence(monkeypatch, reader, [older, newer])
    stale = await worker.fetch_snapshot({"handles": ["pancakeswap"], "cursors": {"pancakeswap": cursor}})
    assert stale["posts"] == [] and stale["health"] == "failed"
    assert stale["accounts"][0]["error"] == "PUBLIC_PROFILE_STALE_SAMPLE"
    assert stale["nextCursors"] == {"pancakeswap": cursor}
    unchanged = await worker.fetch_snapshot(
        {"handles": ["pancakeswap"], "cursors": stale["nextCursors"]}
    )
    assert unchanged["health"] == "healthy" and unchanged["coverageGaps"] == []
    assert unchanged["nextCursors"] == {"pancakeswap": cursor}


def test_consumer_rejects_a_retained_sample_cursor_below_its_highwater(retained):
    older = retained["1FXDuSk4EdNCCzckj"]
    cursor = "2115795788685299930"
    result = scan(retained_run(older), ["pancakeswap"], {"pancakeswap": cursor}, b"retained-projection")
    assert result.rows == [] and result.cursors == {"pancakeswap": cursor}
    assert [(failure.account, failure.reason) for failure in result.failures] == [
        ("pancakeswap", "CURSOR_REGRESSION")
    ]


def test_consumer_detects_missing_boundary_in_retained_legacy_healthy_output(retained):
    repeated = retained["S1HzYGGLo1xh7D5eK"]
    cursor = "2115085176413757842"
    result = scan(retained_run(repeated), ["wublockchain"], {"wublockchain": cursor}, b"retained-projection")
    assert result.gaps == ["wublockchain"]
    assert result.cursors == {"wublockchain": cursor}
    assert [row["external_id"] for row in result.rows] == ["2115085846248558689"]


def service(tmp_path, transport, handle):
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": "https://actor.test/"}]}
    config = settings(tmp_path, [source])
    config.apify_api_token = "isolated-token"
    svc = SourceService(Repo(), config, http=httpx.AsyncClient(transport=transport))
    svc.accounts = {handle: {"handle": handle, "entity_id": handle, "role": "ORGANIZATION"}}
    return svc, source


async def test_native_gap_replay_ingests_once_without_claiming_history_recovered(
    tmp_path, monkeypatch, native_reader, retained
):
    reader, worker = native_reader
    gap = retained["JSgsP4TYqyEJRRHQS"]
    cursor = "2115085176413757842"
    profile_sequence(monkeypatch, reader, [gap, gap])
    requests = []

    async def actor(request):
        value = json.loads(request.content)
        requests.append(value)
        return httpx.Response(201, json=await worker.fetch_snapshot(value))

    svc, _ = service(tmp_path, httpx.MockTransport(actor), "wublockchain")
    await svc.repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"wublockchain": cursor}})
    try:
        for expected_items in (1, 0):
            result = await svc.poll_once("S29")
            checkpoint = await svc.repo.get("ops.source_checkpoints", "S29:0")
            assert result["items"] == expected_items and result["status"] == "DEGRADED"
            assert checkpoint["history_gap"] == ["wublockchain"]
            assert checkpoint["account_cursors"] == {"wublockchain": cursor}
        assert [value["cursors"] for value in requests] == [{"wublockchain": cursor}] * 2
        items = await svc.repo.list("ingest.source_items")
        assert [item["external_id"] for item in items] == ["2115085846248558689"]
    finally:
        await svc.http.aclose()


async def test_pending_native_samples_cannot_regress_committed_cursor(
    tmp_path, monkeypatch, native_reader, retained
):
    reader, worker = native_reader
    older = retained["1FXDuSk4EdNCCzckj"]
    newer = retained["om0fAvrciMOvsZQtL"]
    initial = "2115435919771873495"
    profile_sequence(monkeypatch, reader, [newer, older])

    async def actor(request):
        return httpx.Response(201, json=await worker.fetch_snapshot(json.loads(request.content)))

    svc, source = service(tmp_path, httpx.MockTransport(actor), "pancakeswap")
    await svc.repo.put("ops.source_checkpoints", "S29:0", {"account_cursors": {"pancakeswap": initial}})
    try:
        await svc.poller.acquire(source, 0)
        await svc.poller.acquire(source, 0)
        pending = await svc.repo.get("ops.source_checkpoints", "S29:0")
        assert pending["account_cursors"] == {"pancakeswap": initial}
        assert [batch["coverage"]["account_cursors"]["pancakeswap"] for batch in pending["pending_batches"]] == [
            "2115795788685299930",
            "2115676525345530213",
        ]
        await svc.poller.drain(source, 0, force=True)
        checkpoint = await svc.repo.get("ops.source_checkpoints", "S29:0")
        assert checkpoint["pending_batches"] == []
        assert checkpoint["account_cursors"] == {"pancakeswap": "2115795788685299930"}
        items = await svc.repo.list("ingest.source_items")
        assert sorted(item["external_id"] for item in items) == [
            "2115676525345530213",
            "2115723826915103199",
            "2115775656671793518",
            "2115795788685299930",
        ]
    finally:
        await svc.http.aclose()


async def test_native_new_posts_after_baseline_are_ingested_with_their_exact_ids(
    tmp_path, monkeypatch, native_reader, retained
):
    reader, worker = native_reader
    older = retained["1FXDuSk4EdNCCzckj"]
    newer = retained["om0fAvrciMOvsZQtL"]
    profile_sequence(monkeypatch, reader, [older, newer, newer])

    async def actor(request):
        return httpx.Response(201, json=await worker.fetch_snapshot(json.loads(request.content)))

    svc, _ = service(tmp_path, httpx.MockTransport(actor), "pancakeswap")
    try:
        for expected_items in (0, 3, 0):
            result = await svc.poll_once("S29")
            assert result["items"] == expected_items and result["status"] == "HEALTHY"
        checkpoint = await svc.repo.get("ops.source_checkpoints", "S29:0")
        assert checkpoint["account_cursors"] == {"pancakeswap": "2115795788685299930"}
        items = await svc.repo.list("ingest.source_items")
        assert sorted(item["external_id"] for item in items) == [
            "2115723826915103199",
            "2115775656671793518",
            "2115795788685299930",
        ]
    finally:
        await svc.http.aclose()
