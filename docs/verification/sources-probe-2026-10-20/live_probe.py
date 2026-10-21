"""Live probe on openclaw-dp of every configured feed (no ingestion, no jobs): python live_probe.py OUT_DIR"""

import asyncio
import json
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.storage import MemoryRepository


async def main(out):
    settings = resolve_versions(
        Settings(data_dir=Path(tempfile.mkdtemp()), capture_only=True, x_enabled=False)
    )
    repo = MemoryRepository()
    await repo.open()
    service = SourceService(repo, settings)
    try:
        results = await service.check(probe=True, include_browser=True)
    finally:
        await service.close()
        await repo.close()
    by_source = defaultdict(list)
    for row in results:
        by_source[row["source_id"]].append(row["status"])
    summary = {
        sid: ("PASS" if all(s.startswith("PASS") for s in statuses) else "/".join(statuses))
        for sid, statuses in sorted(by_source.items())
    }
    Path(out).mkdir(parents=True, exist_ok=True)
    (Path(out) / "live-probe.json").write_text(
        json.dumps({"summary": summary, "feeds": results}, indent=1) + "\n"
    )
    print(json.dumps(summary))
    for row in results:
        if not row["status"].startswith("PASS") and row["status"] != "DISABLED":
            print(
                row["source_id"],
                row["feed_index"],
                row["url"][:90],
                row["status"],
                str(row.get("reason"))[:200],
            )


asyncio.run(main(sys.argv[1]))
