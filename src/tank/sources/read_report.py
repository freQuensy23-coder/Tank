"""Per-source failed-read rates with validated snapshots and explicit reset detection."""

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.sources.configuration import SourceConfiguration, SourceDefinition
from tank.util import iso

LIMIT = 0.01
COUNTERS = ("reads", "detail_reads")


class SourceFeedInventory(DomainModel):
    source_id: str = Field(min_length=1)
    feeds: list[JsonValue]


class CheckpointReader(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...


class ReadCounter(DomainModel):
    ok: int = Field(ge=0)
    failed: int = Field(ge=0)


class FeedReads(DomainModel):
    reads: ReadCounter
    detail_reads: ReadCounter
    status: str | None
    error: str | None


class ReadSnapshot(DomainModel):
    taken_at: str
    feeds: dict[str, FeedReads]


class DegradedFeed(DomainModel):
    feed: str
    error: str | None


class SourceReadRate(DomainModel):
    ok: int
    failed: int
    feeds: int
    reads: int
    failed_rate: float | None
    passed: bool = Field(serialization_alias="pass")
    degraded_feeds: list[DegradedFeed] = Field(default_factory=list)


class ReadWindow(DomainModel):
    from_: str | None = Field(serialization_alias="from")
    to: str


class ReadReport(DomainModel):
    window: ReadWindow
    limit: float
    sources: dict[str, SourceReadRate]
    all_pass: bool


async def snapshot(
    repo: CheckpointReader, sources: Sequence[SourceDefinition | dict[str, JsonValue]]
) -> dict[str, JsonValue]:
    """Read persisted checkpoints; an unseen feed has explicit zero counters."""
    feeds: dict[str, FeedReads] = {}
    for source in sources:
        if isinstance(source, SourceDefinition):
            source_id, feed_count = source.source_id, len(source.feeds)
        else:
            inventory = SourceFeedInventory.model_validate(
                {name: source[name] for name in ("source_id", "feeds")}
            )
            source_id, feed_count = inventory.source_id, len(inventory.feeds)
        for index in range(feed_count):
            key = f"{source_id}:{index}"
            row = await repo.get("ops.source_checkpoints", key)
            if row is None:
                feeds[key] = FeedReads(
                    reads=ReadCounter(ok=0, failed=0),
                    detail_reads=ReadCounter(ok=0, failed=0),
                    status=None,
                    error=None,
                )
                continue
            feeds[key] = FeedReads.model_validate(
                {name: row.get(name, {"ok": 0, "failed": 0}) for name in COUNTERS}
                | {"status": row.get("status"), "error": row.get("error")}
            )
    return ReadSnapshot(taken_at=iso(), feeds=feeds).model_dump(mode="json")


def report(
    end: ReadSnapshot, start: ReadSnapshot | None = None, source_ids: Sequence[str] = ()
) -> ReadReport:
    """Compute deltas only for monotonic counters; a reset invalidates the window."""
    source_feeds: dict[str, list[tuple[str, FeedReads]]] = {sid: [] for sid in source_ids}
    for key, counts in end.feeds.items():
        sid = key.split(":")[0]
        source_feeds.setdefault(sid, []).append((key, counts))
    sources: dict[str, SourceReadRate] = {}
    for sid, feeds in sorted(source_feeds.items()):
        ok = failed = 0
        degraded: list[DegradedFeed] = []
        for key, counts in feeds:
            before = start.feeds[key] if start is not None and key in start.feeds else None
            for counter_name in COUNTERS:
                current = counts.reads if counter_name == "reads" else counts.detail_reads
                prior = (
                    (before.reads if counter_name == "reads" else before.detail_reads)
                    if before is not None
                    else ReadCounter(ok=0, failed=0)
                )
                if current.ok < prior.ok or current.failed < prior.failed:
                    raise ValueError(f"Read counter reset during reporting window: {key}:{counter_name}")
                ok += current.ok - prior.ok
                failed += current.failed - prior.failed
            if counts.status == "DEGRADED":
                degraded.append(DegradedFeed(feed=key, error=counts.error))
        total = ok + failed
        sources[sid] = SourceReadRate(
            ok=ok,
            failed=failed,
            feeds=len(feeds),
            reads=total,
            failed_rate=round(failed / total, 6) if total else None,
            passed=total > 0 and failed / total < LIMIT,
            degraded_feeds=degraded,
        )
    return ReadReport(
        window=ReadWindow(from_=start.taken_at if start is not None else None, to=end.taken_at),
        limit=LIMIT,
        sources=sources,
        all_pass=all(row.passed for row in sources.values()),
    )


def rates(
    end: dict[str, JsonValue], start: dict[str, JsonValue] | None = None, source_ids: Sequence[str] = ()
) -> dict[str, JsonValue]:
    """Legacy JSON interface validates once and serializes the typed report."""
    result = report(
        ReadSnapshot.model_validate(end),
        ReadSnapshot.model_validate(start) if start is not None else None,
        source_ids,
    )
    return result.model_dump(
        mode="json",
        by_alias=True,
        exclude={
            "sources": {
                sid: {"degraded_feeds"} for sid, row in result.sources.items() if not row.degraded_feeds
            }
        },
    )


async def main(argv: Sequence[str] | None = None) -> None:
    from tank import clickhouse_market
    from tank.settings import Settings, resolve_versions
    from tank.storage import PostgresRepository

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline", type=Path, help="snapshot taken at the start of the window")
    parser.add_argument("--save", type=Path, help="write the current snapshot here")
    args = parser.parse_args(argv)
    settings = resolve_versions(Settings())
    sources = settings.load_config_as("sources", SourceConfiguration).sources
    source_ids = [source.source_id for source in sources]
    repo = PostgresRepository(settings.database_url)
    clickhouse_market.install(repo, settings)
    await repo.open()
    try:
        end = await snapshot(repo, sources)
    finally:
        await repo.close()
    if args.save:
        args.save.write_text(json.dumps(end, indent=1) + "\n")
    start = (
        ReadSnapshot.model_validate_json(args.baseline.read_bytes()).model_dump(mode="json")
        if args.baseline
        else None
    )
    print(json.dumps(rates(end, start, source_ids), indent=1))


if __name__ == "__main__":
    asyncio.run(main())
