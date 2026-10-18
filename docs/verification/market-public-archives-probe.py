import asyncio
import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import httpx

from tank.market.archive import fetch_archive
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso


async def main():
    root = Path.cwd()
    settings = Settings(
        _env_file=None,
        config_dir=root / "config",
        data_dir=Path(tempfile.mkdtemp(prefix="tank-archive-public-")),
        replay=True,
        etherscan_api_key="",
        tavily_api_key="",
        typesafe_api_key="",
        openma_api_key="",
        telegram_token="",
    )
    evidence = {
        "started_at": iso(),
        "credentials_used": False,
        "env_file_loaded": False,
        "scope": "5 tokens x7 days Binance SPOT klines;2026-01-01..07; checksum+1440minute continuity; not aggTrades, not72h capture acceptance",
        "files": [],
    }
    async with httpx.AsyncClient(timeout=60) as client:
        for symbol in ["BTCUSDT", "ETHUSDT", "UNIUSDT", "AAVEUSDT", "AVAXUSDT"]:
            for day in range(7):
                at = str(date(2026, 1, 1) + timedelta(days=day))
                repo = MemoryRepository()
                row = {"symbol": symbol, "date": at}
                try:
                    result = await fetch_archive(repo, settings, symbol, at, http=client)
                    manifest = (await repo.list("mkt.raw_archives"))[0]
                    row.update(
                        status="PASS",
                        result=result,
                        sha256=manifest["sha256"],
                        bytes=Path(result["path"]).stat().st_size,
                    )
                except Exception as exc:  # noqa: BLE001 - preserve every public probe failure in evidence
                    row.update(status="FAIL", error_class=type(exc).__name__, reason=str(exc)[:300])
                evidence["files"].append(row)
                (root / "docs/verification/market-archive-5x7-public.json").write_text(
                    json.dumps(evidence, indent=2)
                )
                print(symbol, at, row["status"], flush=True)
                await asyncio.sleep(0.5)
    evidence.update(
        completed_at=iso(),
        passed=sum(r["status"] == "PASS" for r in evidence["files"]),
        failed=sum(r["status"] == "FAIL" for r in evidence["files"]),
        total_bytes=sum(r.get("bytes", 0) for r in evidence["files"]),
    )
    (root / "docs/verification/market-archive-5x7-public.json").write_text(json.dumps(evidence, indent=2))


asyncio.run(main())
