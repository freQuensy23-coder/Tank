"""Live check on openclaw-dp: repeated polls of real sources store raw only for captured items.

In-memory repository, capture-only (no jobs, no model calls), throwaway data dir.
Usage: python live_raw_check.py OUT_DIR
"""

import asyncio
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

from tank.settings import Settings, resolve_versions
from tank.sources import SourceService
from tank.storage import MemoryRepository

SOURCES = ["S01", "S09", "S10", "S12", "S14", "S24", "S30", "S33"]
ROUNDS = 3


async def main(out):
    data = Path(tempfile.mkdtemp(prefix="tank-sources-live-"))
    settings = resolve_versions(Settings(data_dir=data, capture_only=True, x_enabled=False))
    repo = MemoryRepository()
    await repo.open()
    service = SourceService(repo, settings)
    report = {"user_agent": settings.user_agent, "rounds": []}
    try:
        for round_no in range(ROUNDS):
            results = {}
            for sid in SOURCES:
                result = await service.poll_once(sid)
                results[sid] = {
                    "items": result["items"],
                    "status": result["status"],
                    "errors": result["errors"][:3],
                }
            items = await repo.list("ingest.source_items", limit=100000)
            files = list((data / "raw" / "items").rglob("*.json.zst"))
            report["rounds"].append(
                {
                    "round": round_no + 1,
                    "results": results,
                    "items_total": len(items),
                    "raw_files_total": len(files),
                    "raw_files_by_source": dict(
                        Counter(f.relative_to(data / "raw" / "items").parts[0] for f in files)
                    ),
                    "items_by_source": dict(Counter(i["source_id"] for i in items)),
                    "every_raw_file_is_an_item": sorted(f.name for f in files)
                    == sorted(i["item_id"] + ".json.zst" for i in items),
                    "observation_counts": dict(Counter(i.get("observation_count", 1) for i in items)),
                }
            )
            print(json.dumps({k: v for k, v in report["rounds"][-1].items() if k != "results"}), flush=True)
            await asyncio.sleep(5)
    finally:
        await service.close()
        await repo.close()
    Path(out).mkdir(parents=True, exist_ok=True)
    (Path(out) / "live-raw-check.json").write_text(json.dumps(report, indent=1) + "\n")


asyncio.run(main(sys.argv[1]))
