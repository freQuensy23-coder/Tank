"""Lead (20 Oct): peak memory and output of the FSC BTC Market Truth, read-only against prod.

Runs MarketTruthService.assess exactly as the RUN_MARKET_TRUTH job does, with every
repository write suppressed and the database session read-only.
"""
import asyncio
import json
import os
import resource
import sys
import time

from tank.analysis.truth import MarketTruthService
from tank.market.service import MarketService
from tank.settings import Settings, resolve_versions
from tank.storage import PostgresRepository
from tank.time_precision import propagate_timestamp

EID = "EVT-01M5DR4S808JRN94JX6K1EGJVD"
PAYLOAD = {
    "event_id": EID,
    "token_id": "bitcoin:BTC",
    "cutoff": "2026-10-20T22:51:00Z",
    "cutoff_prec": "BUCKET(1m)",
    "cutoff_prec_reason": None,
}


async def main():
    dsn = os.environ.pop("MEASURE_DSN")
    settings = resolve_versions(
        Settings(
            _env_file=None,
            config_dir="/srv/tank/config",
            data_dir=os.environ["MEASURE_DATA"],
            database_url=dsn,
        )
    )
    repo = PostgresRepository(dsn)
    await repo.open()
    suppressed = []

    async def no_write(collection, *args, **kwargs):
        suppressed.append(collection)
        return args[1] if len(args) > 1 else None

    repo.put = no_write
    repo.put_market_batch = no_write
    market = MarketService(repo, settings)
    truth = MarketTruthService(repo, settings, market)
    started = time.monotonic()
    result = await truth.assess(EID, "bitcoin:BTC", propagate_timestamp(PAYLOAD, "cutoff"))
    seconds = time.monotonic() - started
    with open(sys.argv[1], "w") as f:
        json.dump(result, f, default=str)
    primary = result.get("primary_window")
    spot = (result.get("spot") or {}).get(primary) or {}
    print(
        json.dumps(
            {
                "seconds": round(seconds, 1),
                "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024),
                "suppressed_writes": sorted(set(suppressed)),
                "primary_window": primary,
                "move_class": result.get("move_class"),
                "data_confidence": (result.get("data_confidence") or {}).get("class"),
                "dc_failed": (result.get("data_confidence") or {}).get("failed"),
                "spot_venues": [
                    {k: v.get(k) for k in ("venue", "buy_qty", "sell_qty", "cvd", "sign", "volume_share")}
                    for v in spot.get("venues", [])
                ],
                "excluded": (spot.get("coverage") or {}).get("excluded"),
                "direction": spot.get("direction"),
                "classification": spot.get("classification"),
            },
            default=str,
        )
    )
    await repo.close()


asyncio.run(main())
