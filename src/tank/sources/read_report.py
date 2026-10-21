"""Per-source failed-read rate for V0-A (01 §10: < 1% failed reads per source over 72 h).

Reads the cumulative counters each feed keeps in ops.source_checkpoints. A read is
one poll of a feed (after its retries) or one article/detail fetch. Run it at the
start of the window with --save, and at the end with --baseline to get the window.

    python -m tank.sources.read_report --save start.json
    python -m tank.sources.read_report --baseline start.json --save end.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from tank.util import iso

LIMIT = 0.01
COUNTERS = ("reads", "detail_reads")


async def snapshot(repo, sources):
    feeds = {}
    for source in sources:
        for index, _ in enumerate(source.get("feeds", [])):
            key = f"{source['source_id']}:{index}"
            row = await repo.get("ops.source_checkpoints", key) or {}
            feeds[key] = {name: dict(row.get(name) or {"ok": 0, "failed": 0}) for name in COUNTERS}
            feeds[key].update(status=row.get("status"), error=row.get("error"))
    return {"taken_at": iso(), "feeds": feeds}


def rates(end, start=None, source_ids=()):
    start_feeds = (start or {}).get("feeds", {})
    per_source = {sid: {"ok": 0, "failed": 0, "feeds": 0} for sid in source_ids}
    for key, counters in end["feeds"].items():
        sid = key.split(":")[0]
        row = per_source.setdefault(sid, {"ok": 0, "failed": 0, "feeds": 0})
        row["feeds"] += 1
        for name in COUNTERS:
            before = start_feeds.get(key, {}).get(name, {"ok": 0, "failed": 0})
            for field in ("ok", "failed"):
                row[field] += counters[name][field] - before.get(field, 0)
        if counters.get("status") == "DEGRADED":
            row.setdefault("degraded_feeds", []).append({"feed": key, "error": counters.get("error")})
    for row in per_source.values():
        total = row["ok"] + row["failed"]
        row["reads"] = total
        row["failed_rate"] = round(row["failed"] / total, 6) if total else None
        row["pass"] = bool(total) and row["failed"] / total < LIMIT
    return {
        "window": {"from": (start or {}).get("taken_at"), "to": end["taken_at"]},
        "limit": LIMIT,
        "sources": dict(sorted(per_source.items())),
        "all_pass": all(row["pass"] for row in per_source.values()),
    }


async def main(argv=None):
    from tank.settings import Settings, resolve_versions
    from tank.storage import PostgresRepository

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline", type=Path, help="snapshot taken at the start of the window")
    parser.add_argument("--save", type=Path, help="write the current snapshot here")
    args = parser.parse_args(argv)
    settings = resolve_versions(Settings())
    sources = settings.load_config("sources")["sources"]
    source_ids = [s["source_id"] for s in sources]
    repo = PostgresRepository(settings.database_url)
    from tank import clickhouse_market

    clickhouse_market.install(repo, settings)
    await repo.open()
    try:
        end = await snapshot(repo, sources)
    finally:
        await repo.close()
    if args.save:
        args.save.write_text(json.dumps(end, indent=1) + "\n")
    start = json.loads(args.baseline.read_text()) if args.baseline else None
    print(json.dumps(rates(end, start, source_ids), indent=1))


if __name__ == "__main__":
    asyncio.run(main())
