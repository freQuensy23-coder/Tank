"""Evidence only: synthetic typed 1m flow bars over the H24 horizon of the shifted E2E card.

The E2E chain freezes reference A:spot:T (entry 101.1) and BTC reference A:spot:BTC
(entry 100) at cutoff 2026-10-18T17:24Z. These bars cover [cutoff, cutoff+24h), so
`tank review show` can compute the realized move at the outlook horizon.
Expected: realized +8.80 %, MFE +10.78 %, MAE -2.08 %, BTC +1 %, BTC-adjusted +7.80 %.
"""

import asyncio
import os
import sys
from datetime import timedelta

sys.path.insert(0, "/srv/tank-work/wt/tank-ops/tests")
from review_typed_fixtures import put_flow  # noqa: E402

from tank.storage import PostgresRepository  # noqa: E402
from tank.util import iso, parse_time  # noqa: E402


async def main():
    repo = await PostgresRepository(os.environ["TANK_DATABASE_URL"]).open()
    start = parse_time("2026-10-18T17:24:00Z")
    try:
        for minute in range(1440):
            stamp = iso(start + timedelta(minutes=minute))
            close = 101.1 + 8.9 * (minute + 1) / 1440
            await put_flow(
                repo,
                f"seed-T-{minute}",
                {
                    "instrument_id": "A:spot:T",
                    "open_time": stamp,
                    "c": round(close, 4),
                    "h": 112 if minute == 1000 else round(close + 0.05, 4),
                    "l": 99 if minute == 10 else round(close - 0.05, 4),
                    "complete": True,
                },
            )
            await put_flow(
                repo,
                f"seed-BTC-{minute}",
                {"instrument_id": "A:spot:BTC", "open_time": stamp, "c": 101, "complete": True},
            )
    finally:
        await repo.close()


asyncio.run(main())
