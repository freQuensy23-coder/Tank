"""Frozen time uncertainty survives decision, review horizons and operator clocks."""

import asyncio
from datetime import timedelta

import pytest
from click.testing import CliRunner
from review_typed_fixtures import put_flow

from tank.analysis.decisions import DecisionEngine
from tank.review import ReviewService, public_bounds
from tank.storage import MemoryRepository
from tank.util import parse_time, sha256


@pytest.mark.parametrize(
    "precision,expected",
    [
        ("BUCKET_1H", "BUCKET(1h)"),
        ("BUCKET(5m)", "BUCKET(5m)"),
        ("COLLECTION_ONLY", "COLLECTION_ONLY"),
        (None, None),
    ],
)
def test_decision_inherits_selected_truth_cutoff(precision, expected):
    from test_decisions import inputs

    e, a, b, m = inputs()
    m["cutoff"] = "2026-10-01T00:03:00Z"
    if precision:
        m["cutoff_prec"] = precision
    result = DecisionEngine().evaluate(e, a, b, m)
    assert result["cutoff"] == m["cutoff"] and result["cutoff_prec"] == expected
    assert bool(result["cutoff_prec_reason"]) == (expected is None)


@pytest.mark.asyncio
async def test_review_horizon_and_public_bucket_preserve_true_interval_anchor():
    repo = MemoryRepository()
    service = ReviewService(repo)
    at = "2026-10-01T00:03:00Z"
    decision = {"cutoff": at, "cutoff_prec": "BUCKET(5m)", "outlook": {"best_horizon": "H1"}}
    pending = await service.outcome({"event_id": "e"}, decision, {}, now="2026-10-01T00:05:00Z")
    assert pending["start"] == at and pending["start_prec"] == "BUCKET(5m)"
    assert pending["end"] == "2026-10-01T01:03:00Z" and pending["end_prec"] == "BUCKET(5m)"
    assert pending["computed_at_prec"] == "COLLECTION_ONLY"
    lower, upper, precision = public_bounds({"public_at": at, "public_at_prec": "BUCKET(5m)"})
    assert lower == parse_time(at) and upper == lower + timedelta(minutes=5) and precision == "BUCKET(5m)"
    event = {"public_at": at, "public_at_prec": "BUCKET(5m)", "known_to_us_at": "2026-10-03T00:06:00Z"}
    payload = {"event": event}
    digest = sha256(payload)
    await repo.put("out.evidence_bundles", digest, {"sha256": digest, "payload": payload}, immutable=True)
    await repo.put(
        "out.alerts",
        "a:1",
        {
            "alert_id": "a",
            "version": 1,
            "event_id": "e",
            "generated_at": "2026-10-03T00:06:00Z",
            "evidence_bundle_id": digest,
        },
        immutable=True,
    )
    report = await service.report("2026-10-03T00:00:00Z", "2026-10-04T00:00:00Z")
    assert report["metrics"]["S2"]["value"] is None  # Age straddles 48h at actual 00:03..00:08 bounds.
    assert report["metrics"]["S2"]["precision_counts"]["BUCKET(5m)"] == 1
    assert report["computed_at_prec"] == "COLLECTION_ONLY"
    assert report["period"]["start_prec"] == "EXACT"
    unknown = await service.outcome(
        {"generated_at": at, "event_id": "e"}, {"cutoff": at, "outlook": {"best_horizon": "H1"}}, {}, now=at
    )
    assert unknown["start_prec"] is None and unknown["end_prec"] is None
    assert unknown["start_prec_reason"] and unknown["end_prec_reason"]


def test_review_label_and_scout_human_action_use_collection_clock(monkeypatch):
    from tank import cli

    repo = MemoryRepository()

    async def seed():
        await repo.put("evt.events", "e", {"event_id": "e"})
        await repo.put("scout.expansion_proposals", "p", {"prp_id": "p"})

    asyncio.run(seed())
    monkeypatch.setattr(cli, "run_db", lambda fn: asyncio.run(fn(repo, None)))
    runner = CliRunner()
    result = runner.invoke(cli.main, ["review", "label", "e", "CORRECT_SILENCE", "--reviewer", "operator"])
    assert result.exit_code == 0, result.output
    review = asyncio.run(repo.list("qa.reviews"))[0]
    assert review["at_prec"] == "COLLECTION_ONLY"
    result = runner.invoke(cli.main, ["scout", "decide", "p", "TEST_MORE"])
    assert result.exit_code == 0, result.output
    assert asyncio.run(repo.get("scout.expansion_proposals", "p"))["decided_at_prec"] == "COLLECTION_ONLY"


@pytest.mark.asyncio
async def test_postgres_completed_outcome_preserves_precision_and_typed_local_clock():
    import os

    from tank.storage import PostgresRepository
    from tank.util import iso, new_id

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires caller-owned isolated PostgreSQL test database")
    repo = PostgresRepository(url)
    await repo.open()
    await repo.migrate()
    prefix = new_id("time-review")
    start = parse_time("2026-10-01T00:00:00Z")
    try:
        for minute in range(60):
            await put_flow(
                repo,
                f"{prefix}-{minute}",
                {
                    "instrument_id": prefix,
                    "open_time": iso(start + timedelta(minutes=minute)),
                    "c": "11",
                    "h": "12",
                    "l": "9",
                    "complete": True,
                },
            )
        result = await ReviewService(repo).outcome(
            {"alert_id": prefix, "version": 1, "event_id": prefix},
            {
                "dec_id": prefix,
                "token_id": "T",
                "cutoff": iso(start),
                "cutoff_prec": "BUCKET(1m)",
                "outlook": {"best_horizon": "H1"},
            },
            {"relative": {"ref_venue": prefix, "entry_ref_price": "10"}},
            now="2026-10-01T01:01:00Z",
        )
        assert result["status"] == "COMPLETED"
        assert result["start_prec"] == result["end_prec"] == "BUCKET(1m)"
        assert result["computed_at_prec"] == "COLLECTION_ONLY"
        async with repo.transaction():
            row = await (
                await repo._conn().execute(
                    "SELECT computed_at_prec FROM qa.outcomes WHERE outcome_id=%s", (result["outcome_id"],)
                )
            ).fetchone()
        assert row["computed_at_prec"] == "COLLECTION_ONLY"
        await repo.close()
        await repo.open()
        restored = await repo.get("qa.outcomes", result["outcome_id"])
        assert restored["start_prec"] == restored["end_prec"] == "BUCKET(1m)"
        assert restored["computed_at_prec"] == "COLLECTION_ONLY"
    finally:
        await repo.close()
