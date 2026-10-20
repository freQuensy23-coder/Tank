"""Prove S29 polls the configured X list through xsensor (02 §3.7) on openclaw-dp.

Throwaway profile and data dir, in-memory repository; the session secret is only read.
Usage: python s29_poll.py OUT_JSON
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


async def main(out):
    work = Path(tempfile.mkdtemp(prefix="tank-s29-"))
    settings = resolve_versions(
        Settings(
            data_dir=work / "data",
            x_enabled=True,
            x_profile_dir=work / "profile",
            x_storage_state_path=Path("/run/secrets/x_session"),
            x_headless=True,
        )
    )
    repo = MemoryRepository()
    await repo.open()
    service = SourceService(repo, settings)
    handles = {a.lower() for a in service.accounts}
    list_id = service.sources["S29"]["list_id"]
    try:
        first = await service.poll_once("S29")
        second = await service.poll_once("S29")
        state = await repo.get("ops.source_checkpoints", "S29:0")
        items = await repo.list("ingest.source_items", limit=100000)
    finally:
        await service.close()
        await repo.close()
    monitored = Counter(str(i.get("monitored_accounts", ["?"])[0]).lower() for i in items)
    files = list((work / "data" / "raw" / "items").rglob("*.json.zst"))
    report = {
        "list_id": list_id,
        "polls": [{k: first[k] for k in ("items", "status", "errors")}, {k: second[k] for k in ("items", "status", "errors")}],
        "checkpoint": {k: state.get(k) for k in ("status", "scan_status", "status_id", "reads", "detail_reads", "error")},
        "items": len(items),
        "raw_files": len(files),
        "monitored_accounts_seen": len(monitored),
        "monitored_outside_list": sorted(set(monitored) - handles),
        "sample": [
            {k: i.get(k) for k in ("external_id", "author", "monitored_accounts", "published_at", "url")} for i in items[:5]
        ],
    }
    Path(out).write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "sample"}))
    import shutil

    shutil.rmtree(work)


asyncio.run(main(sys.argv[1]))
