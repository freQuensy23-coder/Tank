"""Regression coverage for uncapped timer scans and bounded history queries."""

from datetime import timedelta
from pathlib import Path

import pytest
from source_support import fixture_versions

from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, utcnow


class SmallPageRepository(MemoryRepository):
    def __init__(self) -> None:
        super().__init__()
        self.scans = []
        self.yielded = []

    async def list(self, collection, *, filters=None, limit=1000):

        assert limit == 1, f"Unbounded scheduler list: {collection}"
        return await super().list(collection, filters=filters, limit=limit)

    async def iter_all(self, collection, *, filters=None, batch_size=1000):
        self.scans.append(("all", collection, filters))
        async for row in super().iter_all(collection, filters=filters, batch_size=2):
            self.yielded.append(("all", collection, row))
            yield row

    async def iter_range(
        self, collection, time_field, *, start=None, end=None, filters=None, batch_size=10000
    ):
        self.scans.append(("range", collection, time_field, start, end, filters))
        async for row in super().iter_range(
            collection, time_field, start=start, end=end, filters=filters, batch_size=2
        ):
            self.yielded.append(("range", collection, row))
            yield row


@pytest.mark.asyncio
async def test_active_events_and_lifecycle_reach_late_history_and_checkpoint_pages(tmp_path: Path) -> None:
    repo = SmallPageRepository()
    now = utcnow()
    for i in range(9):
        await repo.put(
            "evt.events",
            f"a{i}",
            {"event_id": f"a{i}", "lifecycle": "CLOSED", "known_to_us_at": iso(now - timedelta(days=90))},
        )
    for i in range(5):
        await repo.put(
            "evt.events",
            f"z{i}",
            {"event_id": f"z{i}", "lifecycle": "TRACKED", "known_to_us_at": iso(now - timedelta(days=8))},
        )
    targets = ["token:A", "token:B", "token:C", "bitcoin:BTC", "ethereum:ETH"]
    await repo.put(
        "evt.events",
        "zz",
        {
            "event_id": "zz",
            "lifecycle": "MONITORING",
            "known_to_us_at": iso(now - timedelta(days=8)),
            "analysis_set": targets,
        },
    )
    for i, token in enumerate(targets):
        await repo.put(
            "out.reaction_checkpoints",
            str(i),
            {"event_id": "zz", "token_id": token, "checkpoint": "POST_72H"},
            immutable=True,
        )
    scheduler = Scheduler(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    assert {e["event_id"] async for e in scheduler.active_events(now)} == {"zz", *(f"z{i}" for i in range(5))}
    await scheduler.lifecycle(now)
    for i in range(5):
        assert (await repo.get("evt.events", f"z{i}"))["lifecycle"] == "CLOSED"
    assert (await repo.get("evt.events", "zz"))["lifecycle"] == "COOLDOWN"
    captured = [r for kind, table, r in repo.yielded if table == "out.reaction_checkpoints"]
    assert len(captured) == 5


@pytest.mark.asyncio
async def test_heartbeat_counts_all_pages_with_time_bounds(tmp_path: Path) -> None:
    repo = SmallPageRepository()
    now = utcnow().replace(hour=12, minute=0, second=0, microsecond=0)
    for status in ("QUEUED", "RUNNING"):
        for i in range(5):
            job = await repo.enqueue(
                "FILTER_ITEM", {}, f"{status}:{i}", run_after=now - timedelta(seconds=i + 1)
            )
            await repo.put("ops.jobs", job["job_id"], {**job, "status": status})
    for i in range(7):
        await repo.put(
            "out.deliveries",
            str(i),
            {"status": "SENT", "alert_id": str(i), "at": iso(now - timedelta(hours=1))},
        )
    await repo.put(
        "out.deliveries", "old", {"status": "SENT", "alert_id": "old", "at": iso(now - timedelta(days=2))}
    )
    for i in range(5):
        await repo.put("mkt.hot_tokens", str(i), {"token_id": str(i), "event_id": "synthetic-event"})
        await repo.put("ops.llm_usage", str(i), {"at": iso(now - timedelta(hours=1)), "cost_usd": "1.25"})
    await repo.put("ops.llm_usage", "unknown", {"at": iso(now), "cost_usd": None})
    await repo.put("ops.llm_usage", "old", {"at": iso(now - timedelta(days=2)), "cost_usd": "1000"})
    await Scheduler(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    ).heartbeat(now)
    notice = await repo.get("out.ops_notices", f"heartbeat_{now.date()}")
    assert notice["analysis_backlog"] == {"queued": 5, "running": 5}
    assert notice["queue_lag_s"] == 5
    assert "alerts=7; hot tokens=5" in notice["text"]
    assert "known subtotal $6.25; unknown cost records=1" in notice["text"]


@pytest.mark.asyncio
async def test_scout_recent_truth_pages_and_checkpoint_resume(tmp_path: Path) -> None:
    repo = SmallPageRepository()
    now = utcnow().replace(microsecond=0)
    scheduler = Scheduler(
        repo,
        Settings(data_dir=tmp_path),
        None,
        None,
        thresholds=Settings(_env_file=None).thresholds(),
        versions=fixture_versions(),
    )
    for i in range(12):
        await repo.put(
            "ana.market_truth",
            f"old{i}",
            {
                "mta_id": f"old{i}",
                "token_id": "T",
                "cutoff": iso(now - timedelta(days=3, minutes=i)),
                "move_class": "SUPPORTED",
                "data_confidence": {"class": "HIGH"},
            },
        )
    await repo.put(
        "ana.market_truth",
        "baseline",
        {
            "mta_id": "baseline",
            "token_id": "T",
            "cutoff": iso(now - timedelta(hours=25)),
            "move_class": "UNKNOWN",
            "data_confidence": {"class": "HIGH"},
        },
    )
    for i in range(7):
        await repo.put(
            "ana.market_truth",
            f"recent{i}",
            {
                "mta_id": f"recent{i}",
                "token_id": "T",
                "cutoff": iso(now - timedelta(hours=i)),
                "move_class": "UNKNOWN",
                "data_confidence": {"class": "HIGH"},
            },
        )
    await scheduler.detect_scout_hooks(now)
    proposals = [r async for r in repo.iter_all("scout.expansion_proposals")]
    assert len(proposals) == 1
    assert set(proposals[0]["evidence"]) == {"baseline", *(f"recent{i}" for i in range(7))}
    checkpoint = await repo.get("ops.scheduler_checkpoints", "scout_hooks")
    assert checkpoint["at"] == iso(now)
    truth_reads = [r for kind, table, r in repo.yielded if table == "ana.market_truth"]
    assert len(truth_reads) == 7
    assert not any(scan[:2] == ("all", "ana.market_truth") for scan in repo.scans)

    await repo.put(
        "evt.events",
        "closed",
        {
            "event_id": "closed",
            "lifecycle": "CLOSED",
            "known_to_us_at": iso(now - timedelta(days=90)),
            "public_at": iso(now - timedelta(days=91)),
            "current_ems_id": "late-assessment",
        },
    )
    await repo.put(
        "evt.ems_assessments",
        "late-assessment",
        {
            "ems_id": "late-assessment",
            "event_id": "closed",
            "class": "MAJOR",
            "cutoff": iso(now + timedelta(seconds=10)),
        },
    )
    repo.scans.clear()
    await scheduler.detect_scout_hooks(now + timedelta(minutes=1))
    resumed = await repo.get("ops.scheduler_checkpoints", "scout_hooks")
    assert resumed["at"] == iso(now + timedelta(minutes=1))
    assert not any(scan[:2] == ("all", "evt.events") and scan[2] is None for scan in repo.scans)
    assert (await repo.get("scout.expansion_proposals", proposals[0]["prp_id"]))["occurrences"] == 1
    proposals_after = [r async for r in repo.iter_all("scout.expansion_proposals")]
    assert any(r["hook"] == "SOURCE_OPPORTUNITY" and r["evidence"] == ["closed"] for r in proposals_after)
